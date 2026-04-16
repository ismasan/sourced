# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sourced is a Ruby library for **aggregateless, stream-less event sourcing**. Messages go into a flat, globally-ordered log (SQLite via Sequel). Consistency context is assembled dynamically by querying relevant facts via key-value pairs extracted from event payloads, rather than being pre-assigned to fixed streams. Reactors declare partition keys which the store uses to build query conditions and claim work.

## Core Architecture

### Key abstractions

- **Message** (`lib/sourced/message.rb`) — base class for all commands/events. No `stream_id` or `seq`; gets a global `position` when stored. Provides `causation_id` / `correlation_id`, `#correlate`, `#extracted_keys`, and a `Registry`. Subclasses: `Sourced::Command`, `Sourced::Event`.
- **Store** (`lib/sourced/store.rb`) — SQLite-backed append-only log with key-pair indexing, consumer groups, scheduled messages, and stale-claim reaping. Returns `ReadResult`, `ClaimResult`, `ConsistencyGuard`, `PositionedMessage`, `Stats`, `OffsetsResult`, `ReadAllResult`.
- **Reactor base classes** — all `extend Sourced::Consumer` and declare `partition_by :key` (+ other keys) to define their consistency boundary:
  - `Decider` (`lib/sourced/decider.rb`) — handles commands, produces events via `event` helper.
  - `Projector` (`lib/sourced/projector.rb`) — builds read models. Two flavors: `Projector::StateStored` and `Projector::EventSourced`.
  - `DurableWorkflow` (`lib/sourced/durable_workflow.rb`) — long-running workflows with step memoisation via `durable`/`wait`/`context`/`execute` and `catch(:halt)`.
  - Plain `Consumer` reactors (extend `Sourced::Consumer` directly) for side-effect-only handlers.
- **Mixins**: `Sourced::Evolve` (state evolution from history), `Sourced::React` (event → command/event reactions), `Sourced::Sync` (post-append side effects).
- **Router** (`lib/sourced/router.rb`) — registers reactors, dispatches claimed batches, manages consumer-group lifecycle hooks.
- **Dispatcher / Worker / WorkQueue** (`lib/sourced/{dispatcher,worker,work_queue}.rb`) — claim-and-drain processing; signal-driven via `InlineNotifier` + `CatchUpPoller`.
- **StaleClaimReaper** (`lib/sourced/stale_claim_reaper.rb`) — releases abandoned partition claims from dead workers via heartbeats.
- **ScheduledMessagePoller** (`lib/sourced/scheduled_message_poller.rb`) — promotes due scheduled messages into the main log.
- **Supervisor** (`lib/sourced/supervisor.rb`) — top-level process entry point wiring Dispatcher, executor, and reactors.
- **CommandContext** (`lib/sourced/command_context.rb`) — builds commands from raw attributes; supports per-message and `any` hooks.
- **Topology** (`lib/sourced/topology.rb`) — graph of reactors / message flows.
- **Installer + migrations** (`lib/sourced/installer.rb`, `lib/sourced/migrations/`) — Sequel migration template for installing store tables.
- **Falcon integration** (`lib/sourced/falcon/`) — `Environment` + `Service` for deferred post-fork setup.

### Message flow

`Command → Decider.decide → Events → Store.append → Router claims → Reactor.handle_claim → (Projections / Sync actions)`

Reactions are **deferred**: a Decider's `react` blocks don't run inline with the command that produced the triggering event. When the Decider appends events, its own subscription (`handled_messages_for_react`) picks them up on the next claim cycle and runs the reaction in a separate `handle_batch`. Consequence: the originating command's `after_sync` commits as soon as its events commit, not after reactions finish. Trade-off: command and reactions are no longer in the same transaction — a failing reaction does not roll back the command.

All reactors implement `.handle_claim(claim, history:)` and/or `.handle_batch(partition_values, new_messages, history:, replaying:)` with a uniform signature so GWT helpers and partial-ack logic work across types.

### Partition-based consistency

Reactors declare `partition_by :key1, :key2`. The store indexes every payload attribute into `sourced_key_pairs` at append time, and reads use AND-filtered conditions over these keys. `ConsistencyGuard` (returned by `read` / `claim_next`) detects conflicting appends via `messages_since(conditions, position)`.

## Development Commands

### Testing

```bash
# Full suite
bundle exec rake

# Specific file
bundle exec rspec spec/store_spec.rb
bundle exec rspec spec/decider_spec.rb
```

Tests use in-memory SQLite by default. `spec/store_spec.rb` is the central integration suite; `spec/testing/rspec_spec.rb` covers the GWT helpers in `lib/sourced/testing/rspec.rb`.

### Console

```bash
bin/console
```

## Configuration

```ruby
Sourced.configure do |config|
  config.store = Sequel.sqlite('my_app.db')   # auto-wraps in Sourced::Store
  # or: config.store = Sourced::Store.new(db)
  # or: any object matching Configuration::StoreInterface
  config.worker_count   = 2
  config.batch_size     = 50
  config.catchup_interval = 5
  config.claim_ttl_seconds = 120
end

Sourced.register(SomeDecider)
Sourced.register(SomeProjector)
```

- `Sourced.configure` stores the block and calls `setup!`; re-runnable post-fork to re-establish DB connections (used by Falcon integration).
- `Sourced.store`, `Sourced.router`, `Sourced.topology`, `Sourced.reset!` — module-level accessors.
- `Sourced.handle!(ReactorClass, command)` — synchronous command dispatch (for web controllers): validates, loads history via partition read, decides, appends with guard, advances registered offsets. Returns `HandleResult(command, reactor, events)`.
- `Sourced.load(ReactorClass, **partition_values)` — loads a reactor instance by evolving over AND-filtered partition history. Returns `[instance, read_result]`.

## DSL Patterns

### Decider

```ruby
class Courses < Sourced::Decider
  partition_by :course_id

  command CreateCourse do |_state, cmd|
    event CourseCreated, course_id: cmd.payload.course_id, course_name: cmd.payload.course_name
  end

  event CourseCreated do |state, evt|
    state[:course_id] = evt.payload.course_id
    state[:name] = evt.payload.course_name
  end

  reaction CourseCreated do |evt|
    dispatch SendWelcomeEmail, course_id: evt.payload.course_id
  end
end
```

### Message definition

```ruby
CreateCourse = Sourced::Command.define('courses.create') do
  attribute :course_id, Types::String.present
  attribute :course_name, Types::String.present
end

CourseCreated = Sourced::Event.define('courses.created') do
  attribute :course_id, String
  attribute :course_name, String
end
```

`Sourced::Command` and `Sourced::Event` each have their own `Registry` (both reachable from `Sourced::Message.registry` via recursive lookup).

### Projector flavors

- `Projector::StateStored` — evolves only the claimed batch on top of the stored state snapshot.
- `Projector::EventSourced` — evolves from full history every claim (via `context_for`).

### Scheduled / delayed messages

```ruby
cmd = SendReminder.new(payload: { course_id: 'c1' }).at(Time.now + 3600)
store.schedule_messages([cmd])
store.update_schedule!   # manual promotion (normally done by ScheduledMessagePoller)
```

In reactions: `dispatch(Cmd, ...).at(time)`.

## Store API highlights

- `append(messages, guard: nil)` — writes + auto-indexes payload keys; raises `ConcurrentAppendError` if guard is violated.
- `read(conditions, after_position:, limit:)` → `ReadResult(messages, guard)`.
- `read_partition(partition_attrs, handled_types:)` — AND-filtered read for loading reactor state.
- `read_all(after_position:, limit:, order: :asc, conditions: nil)` → `ReadAllResult` (lazy pagination via `to_enum`).
- `claim_next(reactor, worker_id:)` → `ClaimResult` with partition batch + guard. Supports compound partitions and replaying flag.
- `ack(claim, last_position:)` / `release(claim)` / `advance_offset(group_id, partition:, position:)`.
- `register_consumer_group`, `start_consumer_group`, `stop_consumer_group`, `reset_consumer_group`.
- `read_offsets(group_id:, limit:, from_id:)` → `OffsetsResult` (cursor-paginated, `to_enum`).
- `stats` → `Stats(max_position, groups)` including `error_context`.
- `worker_heartbeat` / `release_stale_claims` — claim liveness.

## Testing

- `lib/sourced/testing/rspec.rb` provides GWT helpers (`given`/`when_`/`then_`) usable across all reactor types since `#handle_batch` has a uniform signature.
- Shared store behaviour concentrated in `spec/store_spec.rb` (2400+ lines).
- Durable workflow specs demonstrate the step-memoisation pattern.

## Error Handling

- `error_strategy` on `Configuration` — configurable retry / backoff / fail. See `lib/sourced/error_strategy.rb`.
- Consumer groups have `running` / `stopped` / `failed` states. `on_fail` fires on terminal failures.
- `PartialBatchError` carries successfully-processed `action_pairs` plus the failing message so batches can be partially acked.

## Key Files

- Entrypoint: `lib/sourced.rb` (top-level API, `handle!`, `load`)
- Store: `lib/sourced/store.rb` + `lib/sourced/installer.rb` + `lib/sourced/migrations/`
- Reactors: `lib/sourced/{decider,projector,durable_workflow,consumer}.rb`
- Mixins: `lib/sourced/{evolve,react,sync}.rb`
- Dispatch: `lib/sourced/{dispatcher,worker,work_queue,stale_claim_reaper,scheduled_message_poller,inline_notifier}.rb`
- Router/topology: `lib/sourced/{router,topology}.rb`
- Messages: `lib/sourced/message.rb` (includes `QueryCondition`, `ConsistencyGuard`)
- Falcon: `lib/sourced/falcon/{environment,service}.rb`
- Testing: `lib/sourced/testing/rspec.rb`

## Local scratch (untracked)

`examples/app/` and `bench/*` are intentionally untracked (see commits `Untrack examples/app` / `Untrack bench files`). Files may exist locally for experimentation but must not be re-added to the repo without explicit approval.
