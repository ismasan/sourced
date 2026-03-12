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
# => [QueryCondition('courses.created', 'course_id', 'c1')]

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

## Deciders

Deciders handle commands, enforce invariants, and produce events. They rebuild state from event history before each decision.

```ruby
class CourseDecider < Sourced::CCC::Decider
  # Defines the consistency boundary
  partition_by :course_name

  # Initial state factory (receives partition values array)
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
    existing = MyDB.find(partition_values.first)
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

The supervisor starts workers that claim partitions, process messages, and ack offsets.

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

## Full example

See `examples/ccc_app/` for a complete Sinatra application with:
- Two deciders (course creation with name uniqueness, student enrolment with capacity limits)
- An event-sourced projector writing JSON files
- Synchronous command handling via `CCC.handle!` in HTTP endpoints
- Background worker processing via Falcon
