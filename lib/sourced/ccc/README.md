# Sourced::CCC — Stream-less Event Sourcing

CCC ("Command Context Consistency") is an experimental module for aggregateless, stream-less event sourcing. Events go into a flat, globally-ordered log. Consistency context is assembled dynamically by querying relevant facts via key-value pairs extracted from event payloads, rather than being pre-assigned to fixed streams.

## Core Concepts

- **No streams or aggregates** — all messages share a single append-only log with auto-increment positions.
- **Partitioning by attributes** — reactors declare which payload attributes define their consistency boundary (e.g. `partition_by :course_id`). The store uses these to build query conditions and claim work.
- **Decide → Evolve → React** — same pattern as core Sourced, but without stream IDs or sequence numbers.
- **Optimistic concurrency** — reads return a `ConsistencyGuard` that detects conflicting writes at append time.

## Messages

CCC has its own message hierarchy, separate from `Sourced::Message`. Messages have no `stream_id` or `seq` — they get a global `position` when stored.

```ruby
# Define base classes for your domain (optional but recommended)
class MyEvent < Sourced::CCC::Event; end
class MyCommand < Sourced::CCC::Command; end

# Define typed messages with payload attributes
CreateCourse = MyCommand.define('courses.create') do
  attribute :course_id, String
  attribute :course_name, String
end

CourseCreated = MyEvent.define('courses.created') do
  attribute :course_id, String
  attribute :course_name, String
end
```

### Message features

- **Auto-generated UUIDs** for `id`, `causation_id`, and `correlation_id`
- **Causal tracing** via `#correlate(other_message)` — sets `causation_id` and `correlation_id`
- **Auto-indexed keys** — `#extracted_keys` returns `[["course_id", "c1"], ["course_name", "Algebra"]]` from payload attributes, used by the store to index messages for querying
- **Own registry** — `CCC::Message.registry` is separate from `Sourced::Message.registry`. Use `.from(type: "courses.created", payload: {...})` to instantiate from a type string.
- **Typed payloads** — uses the same Plumb/Types DSL as core Sourced for attribute coercion and validation

## Store

`CCC::Store` is an SQLite-backed store (via Sequel) providing the flat message log, key-pair indexing, and consumer group management.

```ruby
require 'sequel'

db = Sequel.sqlite('my_app.db')
store = Sourced::CCC::Store.new(db)
store.install!  # creates tables (idempotent)
```

### Appending messages

```ruby
cmd = CreateCourse.new(payload: { course_id: 'c1', course_name: 'Algebra' })
position = store.append(cmd)  # returns the assigned position
```

### Reading with query conditions

```ruby
# Build conditions for a specific course
conditions = CourseCreated.to_conditions(course_id: 'c1')
# => [QueryCondition('courses.created', attrs: { course_id: 'c1' })]

# Read matching messages
result = store.read(conditions)
result.messages  # => [PositionedMessage, ...]
result.guard     # => ConsistencyGuard (for optimistic concurrency)
```

### Optimistic concurrency

```ruby
result = store.read(conditions)

# ... later, append with conflict detection
store.append(new_events, guard: result.guard)
# raises Sourced::ConcurrentAppendError if conflicting messages
# were appended after the read
```

### Partition reads

`read_partition` uses AND semantics — a message is included only when every partition attribute it declares matches the given value.

```ruby
result = store.read_partition(
  { course_id: 'c1' },
  handled_types: ['courses.created', 'courses.enrolled']
)
```

### Browsing the global log

`read_all` paginates the entire message log, without requiring query conditions or partition attributes. It returns a `ReadAllResult` with `messages` and `last_position` (the current max position in the store), so clients know whether more pages exist.

```ruby
# First page (default limit: 50, ascending order)
result = store.read_all(limit: 20)
result.messages       # => [PositionedMessage, ...]
result.last_position  # => 100 (max position in the store)

# Next page — pass the last message's position as cursor
result = store.read_all(from_position: result.messages.last.position, limit: 20)

# Check if there are more pages
has_more = result.messages.any? && result.messages.last.position < result.last_position

# Destructuring is also supported
messages, last_position = store.read_all(limit: 20)
```

Use `order: :desc` for reverse-chronological browsing (newest first). Pagination works the same way — `from_position` fetches messages *before* the given position.

```ruby
result = store.read_all(order: :desc, limit: 20)

# Next page of older messages
result = store.read_all(from_position: result.messages.last.position, order: :desc, limit: 20)
```

#### Iterating all messages with `to_enum`

`ReadAllResult#to_enum` returns a lazy `Enumerator` that transparently fetches subsequent pages as you iterate, using the same `order` and `limit` from the original query.

```ruby
# Iterate all messages in pages of 50
store.read_all(limit: 50).to_enum.each do |msg|
  puts "#{msg.position}: #{msg.type}"
end

# Works with Enumerable methods
store.read_all(order: :desc, limit: 100).to_enum.map(&:type)

# Supports lazy enumeration — stops fetching pages once satisfied
store.read_all(limit: 20).to_enum.lazy.select { |m|
  m.type == 'courses.created'
}.first(5)
```

### Database setup

`Store#install!` creates all required tables directly (useful for scripts, tests, and quick prototyping). For production apps using Sequel migrations, the store can export a migration file instead.

#### Quick setup (e.g. scripts, tests)

```ruby
db = Sequel.sqlite('my_app.db')
store = Sourced::CCC::Store.new(db)
store.install!
```

#### Exporting a Sequel migration

Use `Store#copy_migration_to` to generate a migration file compatible with `Sequel::Migrator`:

```ruby
db = Sequel.sqlite('my_app.db')
store = Sourced::CCC::Store.new(db)

# Option 1: pass a directory (uses a default filename)
store.copy_migration_to('db/migrations')

# Option 2: pass a block for full control over the path
store.copy_migration_to do
  "db/migrations/#{Time.now.strftime('%Y%m%d%H%M%S')}_create_ccc_tables.rb"
end
```

Then run your migrations as usual:

```bash
sequel -m db/migrations sqlite://my_app.db
```

#### Custom table prefix

By default, tables are prefixed with `sourced_` (e.g. `sourced_messages`, `sourced_consumer_groups`). Pass a `prefix:` to `Store.new` to customise this — for example when running multiple CCC stores in the same database:

```ruby
store = Sourced::CCC::Store.new(db, prefix: 'billing')
store.install!
# Creates: billing_messages, billing_key_pairs, billing_consumer_groups, ...
```

The prefix is carried through to exported migrations automatically.

#### Using the Installer directly

The installer is also available as a standalone object, which is useful for Rake tasks or setup scripts:

```ruby
installer = Sourced::CCC::Installer.new(db, logger: Logger.new($stdout), prefix: 'sourced')
installer.install       # create tables
installer.installed?    # check if tables exist
installer.uninstall     # drop tables (test env only)
installer.copy_migration_to('db/migrations')
```

## Deciders

Deciders handle commands, enforce invariants, and produce events. They rebuild state from event history before each decision.

```ruby
class CourseDecider < Sourced::CCC::Decider
  # Defines the consistency boundary
  partition_by :course_name

  # Initial state factory (receives partition values hash)
  state do |_partition_values|
    { name_taken: false }
  end

  # Evolve state from events (rebuilds history)
  evolve CourseCreated do |state, _event|
    state[:name_taken] = true
  end

  # Command handler — enforce invariants, then produce events
  command CreateCourse do |state, cmd|
    raise "Course '#{cmd.payload.course_name}' already exists" if state[:name_taken]

    event CourseCreated,
      course_id: cmd.payload.course_id,
      course_name: cmd.payload.course_name
  end
end
```

### Synchronous command handling

`CCC.handle!` loads history, runs the decider, appends the command + events, and advances consumer group offsets — all in one call. Designed for web controllers.

```ruby
cmd = CreateCourse.new(payload: { course_id: 'c1', course_name: 'Algebra' })
cmd, decider, events = Sourced::CCC.handle!(CourseDecider, cmd)

if cmd.valid?
  # Success — events were appended
else
  # Validation failure — cmd.errors has details
end
```

Raises `Sourced::ConcurrentAppendError` on conflicts, or `RuntimeError` on domain invariant violations (e.g. "Course already exists").

### CommandContext

`CCC::CommandContext` is a factory for building CCC commands from raw Hash attributes (e.g. HTTP params), injecting defaults like `metadata`. It mirrors `Sourced::CommandContext` but without `stream_id`, since CCC messages are stream-less.

```ruby
# In a web controller, build a context with shared metadata
ctx = Sourced::CCC::CommandContext.new(
  metadata: { user_id: session[:user_id] }
)

# Build from a type string + payload hash (e.g. from JSON params)
cmd = ctx.build(type: 'courses.create', payload: { course_id: 'c1', course_name: 'Algebra' })
cmd.metadata[:user_id] # => session[:user_id]

# Or pass an explicit command class
cmd = ctx.build(CreateCourse, payload: { course_id: 'c1', course_name: 'Algebra' })
```

String keys are automatically symbolized, so `ctx.build('type' => '...', 'payload' => { ... })` works too.

#### Scoping to a command subset

By default, `CommandContext` looks up types in `CCC::Command.registry`. Pass a `scope:` to restrict lookups to a specific command subclass — attempts to build commands outside the scope raise `Sourced::UnknownMessageError`.

```ruby
class PublicCommand < Sourced::CCC::Command; end

CreateCourse = PublicCommand.define('courses.create') do
  attribute :course_id, String
  attribute :course_name, String
end

# Only PublicCommand subclasses are allowed
ctx = Sourced::CCC::CommandContext.new(scope: PublicCommand)
ctx.build(type: 'courses.create', payload: { ... })  # OK
ctx.build(type: 'admin.delete_all', payload: {})      # raises UnknownMessageError
```

### Loading a decider's state

```ruby
decider, read_result = Sourced::CCC.load(CourseDecider, course_name: 'Algebra')
decider.state  # => { name_taken: true }
```

## Projectors

Projectors consume events to build read models. Two flavours:

### EventSourced projector

Rebuilds state from full history on every batch (like deciders).

```ruby
class CourseCatalogProjector < Sourced::CCC::Projector::EventSourced
  partition_by :course_id

  state do |_partition_values|
    { course_id: nil, course_name: nil, students: [] }
  end

  evolve CourseCreated do |state, event|
    state[:course_id] = event.payload.course_id
    state[:course_name] = event.payload.course_name
  end

  evolve StudentEnrolled do |state, event|
    state[:students] << event.payload.student_id
  end

  # Sync block runs within the store transaction after evolving
  sync do |state:, messages:, **|
    next unless state[:course_id]
    # Write projection to disk, database, cache, etc.
    File.write("projections/#{state[:course_id]}.json", state.to_json)
  end
end
```

### StateStored projector

Loads persisted state via the `state` block, evolves only new (unprocessed) messages.

```ruby
class MyProjector < Sourced::CCC::Projector::StateStored
  partition_by :course_id

  state do |partition_values|
    # Load existing state from your storage
    existing = MyDB.find(partition_values[:course_id])
    existing || { course_id: nil, students: [] }
  end

  evolve StudentEnrolled do |state, event|
    state[:students] << event.payload.student_id
  end

  sync do |state:, messages:, **|
    MyDB.upsert(state)
  end
end
```

## Reactions

Both deciders and projectors can react to events to produce new commands or events, enabling workflow orchestration.

```ruby
class EnrolmentDecider < Sourced::CCC::Decider
  partition_by :course_id

  # ... evolve and command handlers ...

  # React to an event by producing new messages
  reaction StudentEnrolled do |state, event|
    NotifyStudent.new(payload: { student_id: event.payload.student_id })
  end
end
```

Reactions are skipped during replay (when `replaying: true`), so side effects don't re-fire.

## Configuration

```ruby
require 'sourced/ccc'

Sourced::CCC.configure do |c|
  # Pass a Sequel SQLite connection or a CCC::Store instance
  c.store = Sequel.sqlite('my_app.db')

  # Optional settings
  c.worker_count = 4           # background worker fibers (default: 2)
  c.batch_size = 50            # messages per claim (default: 50)
  c.catchup_interval = 5       # seconds between catch-up polls (default: 5)
  c.max_drain_rounds = 10      # max drain iterations per pickup (default: 10)
  c.claim_ttl_seconds = 120    # stale claim threshold (default: 120)
  c.housekeeping_interval = 30 # heartbeat/reap cycle (default: 30)
end
```

## Failure handling and retries

CCC already supports consumer-group retries on failure.

- On reactor errors, `Router#handle_next_for` calls the reactor's `on_exception` hook.
- By default, that hook uses `CCC.config.error_strategy`.
- The default `Sourced::ErrorStrategy` marks the consumer group as failed immediately.
- If you configure a retrying error strategy, CCC stores the next retry time in the consumer group's `retry_at` column and skips claiming work for that group until that time has passed.

So retries are built in already, but they are opt-in via the error strategy configuration.

### Example: exponential backoff retries

```ruby
require 'sourced/ccc'

Sourced::CCC.configure do |c|
  c.store = Sequel.sqlite('my_app.db')

  c.error_strategy = Sourced::ErrorStrategy.new do |s|
    s.retry(
      times: 5,
      after: 2,
      backoff: ->(retry_after, retry_count) { retry_after * (2**(retry_count - 1)) }
    )

    s.on_retry do |retry_count, exception, message, later|
      LOGGER.warn(
        "CCC retry ##{retry_count} for #{message.type} (#{message.id}) " \
        "at #{later}: #{exception.class}: #{exception.message}"
      )
    end

    s.on_fail do |exception, message|
      LOGGER.error(
        "CCC failing consumer group after retries for #{message.type} (#{message.id}): " \
        "#{exception.class}: #{exception.message}"
      )
    end
  end
end
```

With the configuration above, failures retry after:

- retry 1: 2 seconds
- retry 2: 4 seconds
- retry 3: 8 seconds
- retry 4: 16 seconds
- retry 5: 32 seconds

After the configured retries are exhausted, the consumer group is marked as failed.

## Registering reactors

```ruby
Sourced::CCC.register(CourseDecider)
Sourced::CCC.register(EnrolmentDecider)
Sourced::CCC.register(CourseCatalogProjector)
```

This registers the reactor's consumer group with the store and adds it to the global router.

## Background processing

### Falcon (recommended)

`CCC::Falcon` provides a ready-made Falcon service that runs both the web server and CCC background workers as sibling fibers. No separate worker process needed.

```ruby
# falcon.rb
#!/usr/bin/env falcon-host
require_relative 'domain'
require_relative 'app'
require 'sourced/ccc/falcon'

service "my-app" do
  include Sourced::CCC::Falcon::Environment
  include Falcon::Environment::Rackup

  url "http://localhost:9292"
  count 1
end
```

Start with:

```bash
bundle exec falcon host
```

The service automatically calls `CCC.setup!` in each forked process, which replays the `CCC.configure` block to create fresh database connections. This is necessary because SQLite connections are not fork-safe.

#### How it works

- `CCC::Falcon::Environment` — mixin that sets the `service_class` to `CCC::Falcon::Service`. Include it in your Falcon service definition alongside `Falcon::Environment::Rackup`.
- `CCC::Falcon::Service` — extends `Falcon::Service::Server`. On `run`, it calls `CCC.setup!`, starts the web server, and spawns a `CCC::Dispatcher` with all settings from `CCC.config`. On `stop`, it shuts down the dispatcher before the server.
- No separate HouseKeeper fibers are needed — the `StaleClaimReaper` is embedded in the CCC Dispatcher.

### Supervisor (standalone)

For running workers without a web server, the supervisor starts workers that claim partitions, process messages, and ack offsets.

```ruby
# Start blocking (handles INT/TERM signals for graceful shutdown)
Sourced::CCC::Supervisor.start

# Or create and start manually
supervisor = Sourced::CCC::Supervisor.new(
  router: Sourced::CCC.router,
  count: 4
)
supervisor.start
```

### How it works

1. **Store** appends messages and notifies listeners of new message types
2. **Dispatcher** routes notifications to a `WorkQueue`, mapping message types to interested reactors
3. **Workers** pop reactors from the queue, claim a partition via `Router#handle_next_for`, process messages, and ack
4. **CatchUpPoller** periodically pushes all reactors as a safety net (handles missed notifications)
5. **ScheduledMessagePoller** promotes due delayed messages into the main CCC log
6. **StaleClaimReaper** releases claims held by dead workers

### Router (direct usage)

The router can also be used directly for testing or scripting:

```ruby
router = Sourced::CCC.router

# Process one batch for a specific reactor
router.handle_next_for(CourseDecider)

# Drain all pending work across all reactors
router.drain
```

## Consumer groups

Each reactor class is a consumer group. The store tracks per-partition offsets so multiple reactors process the same events independently.

The lifecycle methods (`stop_consumer_group`, `start_consumer_group`, `reset_consumer_group`, `consumer_group_active?`) accept either a String group ID or any object responding to `#group_id` (e.g. a reactor class).

```ruby
store = Sourced::CCC.store

# Pass reactor classes directly
store.stop_consumer_group(CourseDecider)
store.start_consumer_group(CourseDecider)
store.reset_consumer_group(CourseDecider)  # reprocess from beginning
store.consumer_group_active?(CourseDecider)  # => true/false

# Or use plain strings
store.stop_consumer_group('CourseApp::CourseDecider')
```

When retries are configured via `CCC.config.error_strategy`, failed consumer groups remain active but paused until their `retry_at` time. Once that time passes, they become claimable again automatically.

## Monitoring

`Store#stats` returns system-wide diagnostics for monitoring and debugging CCC deployments.

```ruby
stats = store.stats
stats.max_position  # => 42 (latest position in the message log)
stats.groups        # => array of per-consumer-group hashes
```

Each group hash contains:

| Key                  | Description                                                    |
|----------------------|----------------------------------------------------------------|
| `group_id`           | Consumer group identifier (e.g. `"CourseDecider"`)             |
| `status`             | `"active"`, `"stopped"`, or `"failed"`                         |
| `retry_at`           | `Time` of next retry, or `nil`                                 |
| `error_context`      | Hash with error details (`{}` when healthy, see below)         |
| `oldest_processed`   | `MIN(last_position)` across partitions where processing started |
| `newest_processed`   | `MAX(last_position)` across partitions                          |
| `partition_count`    | Number of offset rows (partitions) for this group              |

### `error_context`

The `error_context` hash is empty (`{}`) for healthy groups. When a group is stopped or has failed, it may contain:

| Key                  | Present when | Description                          |
|----------------------|--------------|--------------------------------------|
| `:message`           | Stopped      | Operator-supplied reason for stopping |
| `:exception_class`   | Failed       | Exception class name (e.g. `"RuntimeError"`) |
| `:exception_message` | Failed       | Exception message string             |

When retries are configured, `error_context` also accumulates retry state set by `GroupUpdater#retry_later`.

```ruby
stats = store.stats
stats.groups.each do |g|
  puts "#{g[:group_id]}: #{g[:status]} (#{g[:partition_count]} partitions, up to position #{g[:newest_processed]})"
  if g[:status] == 'failed'
    puts "  error: #{g[:error_context][:exception_class]}: #{g[:error_context][:exception_message]}"
  end
end
```

## Testing

CCC ships with RSpec helpers for Given-When-Then testing of deciders and projectors. The helpers call `handle_batch` directly — no store, router, or consumer group setup needed.

```ruby
require 'sourced/ccc/testing/rspec'

RSpec.configure do |config|
  config.include Sourced::CCC::Testing::RSpec
end
```

### Testing deciders

`with_reactor` takes a decider class and partition attributes, then chains `.given` (history), `.when` (command), and `.then` (expected outcomes).

```ruby
RSpec.describe CourseDecider do
  include Sourced::CCC::Testing::RSpec

  it 'creates a course' do
    with_reactor(CourseDecider, course_name: 'Algebra')
      .when(CreateCourse, course_id: 'c1', course_name: 'Algebra')
      .then(CourseCreated, course_id: 'c1', course_name: 'Algebra')
  end

  it 'rejects duplicate course names' do
    with_reactor(CourseDecider, course_name: 'Algebra')
      .given(CourseCreated, course_id: 'c1', course_name: 'Algebra')
      .when(CreateCourse, course_id: 'c2', course_name: 'Algebra')
      .then(RuntimeError, "Course 'Algebra' already exists")
  end

  it 'produces no events for a no-op command' do
    with_reactor(CourseDecider, course_name: 'Algebra')
      .when(SomeNoopCommand, course_name: 'Algebra')
      .then([])
  end
end
```

#### Multiple expected messages

When a decider produces events and reactions, pass all expected messages as instances:

```ruby
it 'produces event and reaction' do
  with_reactor(EnrolmentDecider, course_id: 'c1')
    .given(CourseCreated, course_id: 'c1', course_name: 'Algebra')
    .when(EnrolStudent, course_id: 'c1', student_id: 's1')
    .then(
      StudentEnrolled.new(payload: { course_id: 'c1', student_id: 's1' }),
      NotifyStudent.new(payload: { student_id: 's1' })
    )
end
```

#### Block form

Pass a block to `.then` to receive the raw action pairs for custom assertions:

```ruby
it 'inspects action pairs' do
  with_reactor(CourseDecider, course_name: 'Algebra')
    .when(CreateCourse, course_id: 'c1', course_name: 'Algebra')
    .then { |pairs|
      actions, source_msg = pairs.first
      append = Array(actions).find { |a| a.is_a?(Sourced::CCC::Actions::Append) }
      expect(append.messages.first).to be_a(CourseCreated)
    }
end
```

#### `.then!` — run sync actions

Use `.then!` instead of `.then` to execute sync actions before assertions:

```ruby
it 'runs sync block' do
  with_reactor(CourseDecider, course_name: 'Algebra')
    .when(CreateCourse, course_id: 'c1', course_name: 'Algebra')
    .then! { |pairs| ... }
end
```

### Testing projectors

Projectors use `.given` (events to evolve) and `.then` with a block that receives the projected state. `.when` is not supported — projectors don't handle commands.

#### StateStored

```ruby
RSpec.describe ItemProjector do
  include Sourced::CCC::Testing::RSpec

  it 'builds state from events' do
    with_reactor(ItemProjector, list_id: 'L1')
      .given(ItemAdded, list_id: 'L1', name: 'Apple')
      .given(ItemAdded, list_id: 'L1', name: 'Banana')
      .then { |state| expect(state[:items]).to eq(['Apple', 'Banana']) }
  end

  it 'handles removal' do
    with_reactor(ItemProjector, list_id: 'L1')
      .given(ItemAdded, list_id: 'L1', name: 'Apple')
      .and(ItemArchived, list_id: 'L1', name: 'Apple')
      .then { |state| expect(state[:items]).to eq([]) }
  end

  it 'runs sync actions with then!' do
    with_reactor(ItemProjector, list_id: 'L1')
      .given(ItemAdded, list_id: 'L1', name: 'Apple')
      .then! { |state| expect(state[:synced]).to be true }
  end
end
```

#### EventSourced

Same API — the helper creates an instance, evolves from all given messages, and yields state:

```ruby
RSpec.describe CatalogProjector do
  include Sourced::CCC::Testing::RSpec

  it 'rebuilds state from full history' do
    with_reactor(CatalogProjector, course_id: 'c1')
      .given(CourseCreated, course_id: 'c1', course_name: 'Algebra')
      .given(StudentEnrolled, course_id: 'c1', student_id: 's1')
      .then { |state| expect(state[:students]).to eq(['s1']) }
  end
end
```

### Message matching

`.then` compares messages by **class** and **payload** only. Fields like `id`, `created_at`, `causation_id`, `correlation_id`, and `metadata` are ignored, so tests don't need to match auto-generated values.

## Full example

See `examples/ccc_app/` for a complete Sinatra application with:
- Two deciders (course creation with name uniqueness, student enrolment with capacity limits)
- An event-sourced projector writing JSON files
- Synchronous command handling via `CCC.handle!` in HTTP endpoints
- Background worker processing via Falcon
