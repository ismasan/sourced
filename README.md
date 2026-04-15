# Sourced — Stream-less Event Sourcing

Sourced is a Ruby library for aggregateless, stream-less event sourcing. Events go into a flat, globally-ordered log. Consistency context is assembled dynamically by querying relevant facts via key-value pairs extracted from event payloads, rather than being pre-assigned to fixed streams.

## Core Concepts

- **No streams or aggregates** — all messages share a single append-only log with auto-increment positions.
- **Partitioning by attributes** — reactors declare which payload attributes define their consistency boundary (e.g. `partition_by :course_id`). The store uses these to build query conditions and claim work.
- **Decide → Evolve → React** — reactors handle commands, evolve state from history, and react to events.
- **Optimistic concurrency** — reads return a `ConsistencyGuard` that detects conflicting writes at append time.

## Messages

Messages have no `stream_id` or `seq` — they get a global `position` when stored.

```ruby
# Define base classes for your domain (optional but recommended)
class MyEvent < Sourced::Event; end
class MyCommand < Sourced::Command; end

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
- **Registry** — `Sourced::Message.registry` indexes defined types. Use `.from(type: "courses.created", payload: {...})` to instantiate from a type string.
- **Typed payloads** — Plumb/Types DSL for attribute coercion and validation

## Store

`Sourced::Store` is an SQLite-backed store (via Sequel) providing the flat message log, key-pair indexing, and consumer group management.

```ruby
require 'sequel'

db = Sequel.sqlite('my_app.db')
store = Sourced::Store.new(db)
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
store = Sourced::Store.new(db)
store.install!
```

#### Exporting a Sequel migration

Use `Store#copy_migration_to` to generate a migration file compatible with `Sequel::Migrator`:

```ruby
db = Sequel.sqlite('my_app.db')
store = Sourced::Store.new(db)

# Option 1: pass a directory (uses a default filename)
store.copy_migration_to('db/migrations')

# Option 2: pass a block for full control over the path
store.copy_migration_to do
  "db/migrations/#{Time.now.strftime('%Y%m%d%H%M%S')}_create_sourced_tables.rb"
end
```

Then run your migrations as usual:

```bash
sequel -m db/migrations sqlite://my_app.db
```

#### Custom table prefix

By default, tables are prefixed with `sourced_` (e.g. `sourced_messages`, `sourced_consumer_groups`). Pass a `prefix:` to `Store.new` to customise this — for example when running multiple Sourced stores in the same database:

```ruby
store = Sourced::Store.new(db, prefix: 'billing')
store.install!
# Creates: billing_messages, billing_key_pairs, billing_consumer_groups, ...
```

The prefix is carried through to exported migrations automatically.

#### Using the Installer directly

The installer is also available as a standalone object, which is useful for Rake tasks or setup scripts:

```ruby
installer = Sourced::Installer.new(db, logger: Logger.new($stdout), prefix: 'sourced')
installer.install       # create tables
installer.installed?    # check if tables exist
installer.uninstall     # drop tables (test env only)
installer.copy_migration_to('db/migrations')
```

## Deciders

Deciders handle commands, enforce invariants, and produce events. They rebuild state from event history before each decision.

```ruby
class CourseDecider < Sourced::Decider
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

`Sourced.handle!` loads history, runs the decider, appends the command + events, and advances consumer group offsets — all in one call. Designed for web controllers.

```ruby
cmd = CreateCourse.new(payload: { course_id: 'c1', course_name: 'Algebra' })
cmd, decider, events = Sourced.handle!(CourseDecider, cmd)

if cmd.valid?
  # Success — events were appended
else
  # Validation failure — cmd.errors has details
end
```

Raises `Sourced::ConcurrentAppendError` on conflicts, or `RuntimeError` on domain invariant violations (e.g. "Course already exists").

### CommandContext

`Sourced::CommandContext` is a factory for building Sourced commands from raw Hash attributes (e.g. HTTP params), injecting defaults like `metadata`. It mirrors `Sourced::CommandContext` but without `stream_id`, since Sourced messages are stream-less.

```ruby
# In a web controller, build a context with shared metadata
ctx = Sourced::CommandContext.new(
  metadata: { user_id: session[:user_id] }
)

# Build from a type string + payload hash (e.g. from JSON params)
cmd = ctx.build(type: 'courses.create', payload: { course_id: 'c1', course_name: 'Algebra' })
cmd.metadata[:user_id] # => session[:user_id]

# Or pass an explicit command class
cmd = ctx.build(CreateCourse, payload: { course_id: 'c1', course_name: 'Algebra' })
```

String keys are automatically symbolized, so `ctx.build('type' => '...', 'payload' => { ... })` works too.

#### Callback hooks (`on` and `any`)

Subclass `CommandContext` and register class-level hooks to enrich or transform commands at build time — e.g. injecting session data or adding metadata from the request scope.

- **`on(MessageClass, ...)`** — runs for one or more command types. Multiple `on` calls for the same class accumulate (all blocks run in registration order).
- **`any`** — runs for all commands (multiple blocks allowed, executed in order)

Both receive the `app` scope and the command, and must return the (possibly modified) command. `on` blocks run before `any` blocks. Blocks are evaluated in the context of the `CommandContext` instance, so they can call private helper methods defined on the subclass.

```ruby
class AppCommandContext < Sourced::CommandContext
  # Enrich a specific command with data from the app scope
  on CreateCourse do |app, cmd|
    cmd.with_payload(created_by: app.current_user.id)
  end

  # Same block for multiple command types
  on EnrolStudent, DropStudent do |app, cmd|
    cmd.with_metadata(campus: app.current_campus)
  end

  # Additional block for EnrolStudent — both blocks run in order
  on EnrolStudent do |app, cmd|
    cmd.with_metadata(enrolment_source: 'web')
  end

  # Add metadata to every command
  any do |app, cmd|
    cmd.with_metadata(
      request_id: app.request_id,
      session_id: app.session_id
    )
  end
end
```

Pass the request-scoped `app` object at construction time:

```ruby
# In a web controller
ctx = AppCommandContext.new(
  metadata: { user_id: session[:user_id] },
  app: self  # e.g. Sinatra app instance, Rack env wrapper, etc.
)

cmd = ctx.build(type: 'courses.create', payload: { course_id: 'c1', course_name: 'Algebra' })
cmd.metadata[:request_id]  # => set by the `any` hook
```

`app` defaults to `nil`, so existing callers without hooks are unaffected. Hooks are inherited by subclasses.

Since blocks run in instance context, you can extract shared logic into private methods:

```ruby
class AppCommandContext < Sourced::CommandContext
  on CreateCourse do |app, cmd|
    cmd.with_metadata(user_id: build_user_id(app))
  end

  private

  def build_user_id(app)
    "user-#{app.session_id}"
  end
end
```

#### Scoping to a command subset

By default, `CommandContext` looks up types in `Sourced::Command.registry`. Pass a `scope:` to restrict lookups to a specific command subclass — attempts to build commands outside the scope raise `Sourced::UnknownMessageError`.

```ruby
class PublicCommand < Sourced::Command; end

CreateCourse = PublicCommand.define('courses.create') do
  attribute :course_id, String
  attribute :course_name, String
end

# Only PublicCommand subclasses are allowed
ctx = Sourced::CommandContext.new(scope: PublicCommand)
ctx.build(type: 'courses.create', payload: { ... })  # OK
ctx.build(type: 'admin.delete_all', payload: {})      # raises UnknownMessageError
```

### Loading a decider's state

```ruby
decider, read_result = Sourced.load(CourseDecider, course_name: 'Algebra')
decider.state  # => { name_taken: true }
```

## Projectors

Projectors consume events to build read models. Two flavours:

### EventSourced projector

Rebuilds state from full history on every batch (like deciders).

```ruby
class CourseCatalogProjector < Sourced::Projector::EventSourced
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

  # After-sync block runs after the transaction commits.
  # Use for side effects that should only happen on successful commit
  # (e.g. sending emails, HTTP calls, pushing to external queues).
  after_sync do |state:, messages:, **|
    NotificationService.notify("Course #{state[:course_name]} updated")
  end
end
```

### StateStored projector

Loads persisted state via the `state` block, evolves only new (unprocessed) messages.

```ruby
class MyProjector < Sourced::Projector::StateStored
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
class EnrolmentDecider < Sourced::Decider
  partition_by :course_id

  # ... evolve and command handlers ...

  # React to an event by producing new messages
  reaction StudentEnrolled do |state, event|
    NotifyStudent.new(payload: { student_id: event.payload.student_id })
  end
end
```

Reactions are skipped during replay (when `replaying: true`), so side effects don't re-fire.

## Sync and After-Sync Blocks

Both deciders and projectors support `sync` and `after_sync` blocks for running side effects during message processing.

- **`sync`** blocks run **inside** the store transaction, alongside event persistence and offset acknowledgement. Use them for writes that must be atomic with the event append (e.g. updating a database projection).
- **`after_sync`** blocks run **after** the transaction commits. Use them for side effects that should only happen if the commit succeeds (e.g. sending emails, HTTP calls, pushing to external queues).

Both receive the same keyword arguments as the reactor's action-building step:

| Reactor type | Keyword arguments                     |
|--------------|---------------------------------------|
| Decider      | `state:`, `messages:`, `events:`      |
| Projector    | `state:`, `messages:`, `replaying:`   |

```ruby
class OrderDecider < Sourced::Decider
  partition_by :order_id

  # ... evolve / command handlers ...

  sync do |state:, messages:, events:|
    # Runs inside the transaction
    OrderCache.update(state[:order_id], state)
  end

  after_sync do |state:, messages:, events:|
    # Runs after successful commit
    Mailer.send_confirmation(state[:order_id]) if events.any? { |e| e.is_a?(OrderPlaced) }
  end
end
```

Multiple `sync` and `after_sync` blocks can be registered; they execute in registration order. Blocks are inherited by subclasses.

## Configuration

```ruby
require 'sourced'

Sourced.configure do |c|
  # Pass a Sequel SQLite connection or a Sourced::Store instance
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

Sourced already supports consumer-group retries on failure.

- On reactor errors, `Router#handle_next_for` calls the reactor's `on_exception` hook.
- By default, that hook uses `Sourced.config.error_strategy`.
- The default `Sourced::ErrorStrategy` marks the consumer group as failed immediately.
- If you configure a retrying error strategy, Sourced stores the next retry time in the consumer group's `retry_at` column and skips claiming work for that group until that time has passed.

So retries are built in already, but they are opt-in via the error strategy configuration.

### Example: exponential backoff retries

```ruby
require 'sourced'

Sourced.configure do |c|
  c.store = Sequel.sqlite('my_app.db')

  c.error_strategy = Sourced::ErrorStrategy.new do |s|
    s.retry(
      times: 5,
      after: 2,
      backoff: ->(retry_after, retry_count) { retry_after * (2**(retry_count - 1)) }
    )

    s.on_retry do |retry_count, exception, message, later|
      LOGGER.warn(
        "Sourced retry ##{retry_count} for #{message.type} (#{message.id}) " \
        "at #{later}: #{exception.class}: #{exception.message}"
      )
    end

    s.on_fail do |exception, message|
      LOGGER.error(
        "Sourced failing consumer group after retries for #{message.type} (#{message.id}): " \
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
Sourced.register(CourseDecider)
Sourced.register(EnrolmentDecider)
Sourced.register(CourseCatalogProjector)
```

This registers the reactor's consumer group with the store and adds it to the global router.

## Background processing

### Falcon (recommended)

`Sourced::Falcon` provides a ready-made Falcon service that runs both the web server and Sourced background workers as sibling fibers. No separate worker process needed.

```ruby
# falcon.rb
#!/usr/bin/env falcon-host
require_relative 'domain'
require_relative 'app'
require 'sourced/falcon'

service "my-app" do
  include Sourced::Falcon::Environment
  include Falcon::Environment::Rackup

  url "http://localhost:9292"
  count 1
end
```

Start with:

```bash
bundle exec falcon host
```

The service automatically calls `Sourced.setup!` in each forked process, which replays the `Sourced.configure` block to create fresh database connections. This is necessary because SQLite connections are not fork-safe.

#### How it works

- `Sourced::Falcon::Environment` — mixin that sets the `service_class` to `Sourced::Falcon::Service`. Include it in your Falcon service definition alongside `Falcon::Environment::Rackup`.
- `Sourced::Falcon::Service` — extends `Falcon::Service::Server`. On `run`, it calls `Sourced.setup!`, starts the web server, and spawns a `Sourced::Dispatcher` with all settings from `Sourced.config`. On `stop`, it shuts down the dispatcher before the server.
- No separate HouseKeeper fibers are needed — the `StaleClaimReaper` is embedded in the Sourced Dispatcher.

### Supervisor (standalone)

For running workers without a web server, the supervisor starts workers that claim partitions, process messages, and ack offsets.

```ruby
# Start blocking (handles INT/TERM signals for graceful shutdown)
Sourced::Supervisor.start

# Or create and start manually
supervisor = Sourced::Supervisor.new(
  router: Sourced.router,
  count: 4
)
supervisor.start
```

### How it works

1. **Store** appends messages and notifies listeners of new message types
2. **Dispatcher** routes notifications to a `WorkQueue`, mapping message types to interested reactors
3. **Workers** pop reactors from the queue, claim a partition via `Router#handle_next_for`, process messages, and ack
4. **CatchUpPoller** periodically pushes all reactors as a safety net (handles missed notifications)
5. **ScheduledMessagePoller** promotes due delayed messages into the main Sourced log
6. **StaleClaimReaper** releases claims held by dead workers

### Router (direct usage)

The router can also be used directly for testing or scripting:

```ruby
router = Sourced.router

# Process one batch for a specific reactor
router.handle_next_for(CourseDecider)

# Drain all pending work across all reactors
router.drain
```

## Consumer groups

Each reactor class is a consumer group. The store tracks per-partition offsets so multiple reactors process the same events independently.

The lifecycle methods (`stop_consumer_group`, `start_consumer_group`, `reset_consumer_group`, `consumer_group_active?`) accept either a String group ID or any object responding to `#group_id` (e.g. a reactor class).

```ruby
store = Sourced.store

# Pass reactor classes directly
store.stop_consumer_group(CourseDecider)
store.start_consumer_group(CourseDecider)
store.reset_consumer_group(CourseDecider)  # reprocess from beginning
store.consumer_group_active?(CourseDecider)  # => true/false

# Or use plain strings
store.stop_consumer_group('CourseApp::CourseDecider')
```

When retries are configured via `Sourced.config.error_strategy`, failed consumer groups remain active but paused until their `retry_at` time. Once that time passes, they become claimable again automatically.

### Lifecycle hooks via Router

The Router provides lifecycle methods that wrap the Store operations and invoke optional callbacks on the reactor class. This lets reactors run cleanup or setup logic when their consumer group is stopped, reset, or started.

```ruby
# Accept a reactor class or a string group_id
Sourced.stop_consumer_group(CourseDecider, 'maintenance window')
Sourced.reset_consumer_group(CourseDecider)
Sourced.start_consumer_group(CourseDecider)

# String group_id works too — the router resolves it to the registered class
Sourced.stop_consumer_group('CourseApp::CourseDecider')
```

These delegate to `Router#stop_consumer_group`, `Router#reset_consumer_group`, and `Router#start_consumer_group`, which:

1. Resolve the argument to a registered reactor class (raising `ArgumentError` if the string doesn't match any registered reactor)
2. Call the corresponding `Store` method
3. Invoke the reactor's callback (`on_stop`, `on_reset`, `on_start`)

#### Defining callbacks

Override the no-op class methods on your reactor to hook into lifecycle events:

```ruby
class CourseDecider < Sourced::Decider
  partition_by :course_name

  # Called when the consumer group is stopped.
  # `message` is the optional reason string passed to stop_consumer_group.
  def self.on_stop(message = nil)
    Rails.logger.info "CourseDecider stopped: #{message}"
  end

  # Called when the consumer group is reset (offsets cleared).
  def self.on_reset
    Rails.cache.delete_matched('course_projections/*')
  end

  # Called when the consumer group is started.
  def self.on_start
    Rails.logger.info 'CourseDecider started'
  end
end
```

Reactors without custom callbacks work fine — the defaults are no-ops.

## Monitoring

`Store#stats` returns system-wide diagnostics for monitoring and debugging Sourced deployments.

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

### `Store#read_offsets` — inspecting partition offsets

`read_offsets` lists individual consumer group offsets with optional filtering and cursor-based pagination. Useful for inspecting the progress and claim status of each partition.

```ruby
result = store.read_offsets
result.offsets       # => array of offset hashes
result.total_count   # => total number of matching offsets (ignoring pagination)
```

#### Parameters

| Parameter    | Type           | Default | Description                                              |
|-------------|----------------|---------|----------------------------------------------------------|
| `group_id:` | `String`, `nil` | `nil`   | Filter by consumer group. `nil` returns all groups.      |
| `limit:`    | `Integer`       | `50`    | Max offsets per page.                                    |
| `from_id:`  | `Integer`, `nil`| `nil`   | Cursor — return offsets with `id >= from_id` (inclusive). |

#### Offset hash fields

Each offset in the result is a Hash with:

| Key              | Type          | Description                                          |
|------------------|---------------|------------------------------------------------------|
| `:id`            | `Integer`     | Offset primary key (used as pagination cursor)       |
| `:group_name`    | `String`      | Consumer group identifier                            |
| `:group_status`  | `String`      | `"active"`, `"stopped"`, or `"failed"`               |
| `:partition_key` | `String`      | Partition identifier (e.g. `"device_id:dev-1"`)      |
| `:last_position` | `Integer`     | Highest acked position for this partition             |
| `:claimed`       | `Boolean`     | Whether a worker currently holds this partition       |
| `:claimed_at`    | `String`, `nil`| ISO8601 timestamp of the claim                      |
| `:claimed_by`    | `String`, `nil`| Worker ID holding the claim                         |

#### Filtering by group

```ruby
result = store.read_offsets(group_id: 'CourseDecider')
result.offsets.each do |o|
  puts "#{o[:partition_key]}: position #{o[:last_position]}, claimed=#{o[:claimed]}"
end
```

#### Pagination

```ruby
# First page
page1 = store.read_offsets(limit: 20)

# Next page using cursor
page2 = store.read_offsets(limit: 20, from_id: page1.offsets.last[:id] + 1)
```

#### Auto-pagination with `to_enum`

`OffsetsResult#to_enum` returns a lazy `Enumerator` that fetches subsequent pages automatically.

```ruby
# Iterate all offsets in pages of 20
store.read_offsets(limit: 20).to_enum.each do |offset|
  puts "#{offset[:group_name]} / #{offset[:partition_key]}: #{offset[:last_position]}"
end

# Works with Enumerable methods
behind = store.read_offsets(limit: 50).to_enum.lazy.select { |o|
  o[:last_position] < store.latest_position - 100
}.to_a
```

#### Array destructuring

```ruby
offsets, total_count = store.read_offsets(group_id: 'CourseDecider')
```

## Testing

Sourced ships with RSpec helpers for Given-When-Then testing of deciders and projectors. The helpers call `handle_batch` directly — no store, router, or consumer group setup needed.

```ruby
require 'sourced/testing/rspec'

RSpec.configure do |config|
  config.include Sourced::Testing::RSpec
end
```

### Testing deciders

`with_reactor` takes a decider class and partition attributes, then chains `.given` (history), `.when` (command), and `.then` (expected outcomes).

```ruby
RSpec.describe CourseDecider do
  include Sourced::Testing::RSpec

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
      append = Array(actions).find { |a| a.is_a?(Sourced::Actions::Append) }
      expect(append.messages.first).to be_a(CourseCreated)
    }
end
```

#### `.then!` — run sync and after_sync actions

Use `.then!` instead of `.then` to execute both `sync` and `after_sync` actions before assertions:

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
  include Sourced::Testing::RSpec

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
  include Sourced::Testing::RSpec

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

See `examples/app/` for a complete Sinatra application with:
- Two deciders (course creation with name uniqueness, student enrolment with capacity limits)
- An event-sourced projector writing JSON files
- Synchronous command handling via `Sourced.handle!` in HTTP endpoints
- Background worker processing via Falcon
