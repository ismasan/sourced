# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sourced is a Ruby library for **aggregateless, stream-less event sourcing**. Messages go into a flat, globally-ordered log (SQLite via Sequel). Consistency context is assembled dynamically by querying relevant facts via key-value pairs extracted from event payloads, rather than being pre-assigned to fixed streams. Reactors declare partition keys which the store uses to build query conditions and claim work.

## Core Architecture

### Key abstractions

- **Message** (`lib/sourced/message.rb`) — base class for all commands/events. No `stream_id` or `seq`; gets a global `position` when stored. Provides `causation_id` / `correlation_id`, `#correlate`, `#extracted_keys`, and a `Registry`. Subclasses: `Sourced::Command`, `Sourced::Event`.
- **Store** (`lib/sourced/store.rb`) — SQLite-backed append-only log with key-pair indexing, consumer groups, scheduled messages, and stale-claim reaping. Supports optional **delete-on-ack** (queue) semantics and per-message-type index basis (payload vs id). Returns `ReadResult`, `ClaimResult`, `ConsistencyGuard`, `PositionedMessage`, `Stats`, `OffsetsResult`, `ReadAllResult`.
- **Reactor base classes** — built-in reactors `extend Sourced::Consumer` and declare `partition_by :key` (+ other keys) to define their consistency boundary:
  - `Decider` (`lib/sourced/decider.rb`) — handles commands, produces events via `event` helper.
  - `Projector` (`lib/sourced/projector.rb`) — builds read models. Two flavors: `Projector::StateStored` and `Projector::EventSourced`.
  - `DurableWorkflow` (`lib/sourced/durable_workflow.rb`) — long-running workflows with step memoisation via `durable`/`wait`/`context`/`execute` and `catch(:halt)`.
  - Plain `Consumer` reactors (extend `Sourced::Consumer` directly) for side-effect-only handlers.
- **Store lifecycle** — `StoreInterface` requires `setup!`, not `install!`/`installed?`: creating tables is one store's answer to "prepare yourself", not the contract. `Store#setup!` = `install!` + `message_codec.compile!`, idempotent, called once by `Configuration#setup!`. `install!` / `installed?` remain public on `Store` for scripts and specs.
- **Reactor protocol** — the Router is **duck-typed**: any class responding to `handled_messages` and `handle_claim(claim, …)` can be registered without extending `Sourced::Consumer` or depending on Sourced (e.g. a third-party command handler). Missing optional methods (`group_id` → class name, `partition_keys` → `[]`, `exclusive?` → false, `on_exception`, `context_for`, lifecycle hooks) are filled in by `ReactorDefaults`.
- **Actions / signals** (`lib/sourced/actions.rb`) — a reactor's `handle_claim` returns `[[signals, source_message], …]` pairs. Each signal is **inert data**: a plain Hash (`{type: :append|:sync|:after_sync|:ack, …}`) or a `Sourced::Actions` value object that `deconstruct_keys` to the same shape. A `delete: true` flag marks the source message for deletion on ack. There is **no separate `:schedule` signal** — a future-dated message (built with `Message#at`) in an `:append` is transparently deferred by the store (see below).
- **ActionRunner** (`lib/sourced/action_runner.rb`) — the only code that touches the store on behalf of actions. Routes each signal to `append`/sync work, applying correlation. Third-party reactors emit plain-Hash signals with no Sourced dependency.
- **ReactorDefaults** (`lib/sourced/reactor_defaults.rb`) — `ReactorDefaults.apply(reactor)` defines missing optional protocol methods directly on the reactor class (only where absent, so the reactor's own definitions win). Chosen over a `SimpleDelegator` wrapper so the reactor stays a real class and `Injector` signature reflection keeps working.
- **Mixins**: `Sourced::Evolve` (state evolution from history), `Sourced::React` (event → command/event reactions), `Sourced::Sync` (post-append side effects).
- **Router** (`lib/sourced/router.rb`) — registers reactors (applies defaults, validates exclusive ownership), dispatches claimed batches via the `ActionRunner`, manages consumer-group lifecycle hooks.
- **Dispatcher / Worker / WorkQueue** (`lib/sourced/{dispatcher,worker,work_queue}.rb`) — claim-and-drain processing; signal-driven via `InlineNotifier` + `CatchUpPoller`.
- **StaleClaimReaper** (`lib/sourced/stale_claim_reaper.rb`) — releases abandoned partition claims from dead workers via heartbeats.
- **ScheduledMessagePoller** (`lib/sourced/scheduled_message_poller.rb`) — promotes due scheduled messages into the main log.
- **Supervisor** (`lib/sourced/supervisor.rb`) — top-level process entry point wiring Dispatcher, executor, and reactors.
- **CommandContext** (`lib/sourced/command_context.rb`) — builds commands from raw attributes; supports per-message and `any` hooks.
- **Topology** (`lib/sourced/topology.rb`) — graph of reactors / message flows.
- **Installer + migrations** (`lib/sourced/installer.rb`, `lib/sourced/migrations/`) — Sequel migration template for installing store tables.

### Message flow

`Command → Decider.decide → Events → Store.append → Router claims → Reactor.handle_claim → signals → ActionRunner → Store`

`handle_claim` returns action **signals** (not direct store calls); the Router's `execute_actions` runs them through the `ActionRunner` inside one transaction, then advances the offset cursor (`ack`) and deletes any messages a signal flagged with `delete: true`.

Reactions are **deferred**: a Decider's `react` blocks don't run inline with the command that produced the triggering event. When the Decider appends events, its own subscription (`handled_messages_for_react`) picks them up on the next claim cycle and runs the reaction in a separate `handle_batch`. Consequence: the originating command's `after_sync` commits as soon as its events commit, not after reactions finish. Trade-off: command and reactions are no longer in the same transaction — a failing reaction does not roll back the command.

All reactors implement `.handle_claim(claim, history:)` and/or `.handle_batch(partition_values, new_messages, history:, replaying:)` with a uniform signature so GWT helpers and partial-ack logic work across types.

### Partition-based consistency

Reactors declare `partition_by :key1, :key2`. The store indexes every payload attribute into `sourced_key_pairs` at append time, and reads use AND-filtered conditions over these keys. `ConsistencyGuard` (returned by `read` / `claim_next`) detects conflicting appends via `messages_since(conditions, position)`.

**Multi-consumer fan-out** is the default: each reactor is its own consumer group with independent per-partition offset cursors, all consuming the singly-stored, payload-indexed log. Many projectors/deciders can evolve from the same events; each advances its own cursor.

### Delete-on-ack queues (`exclusive` reactors)

A reactor may declare `exclusive` (routing marker: it solely owns its handled message types — the Router raises on overlap with any other reactor). Deletion itself is driven **only** by the per-message `delete: true` action flag, never by partitioning or exclusivity — forgetting a partition key never silently deletes.

An `exclusive` reactor with **no `partition_by`** (or `partition_by :__id`) is **partitioned by `Message#id`** — one partition per message: a concurrent, unordered, delete-on-ack queue that reuses the existing offsets/key_pairs engine. The message id is indexed under the reserved key name `__id` (chosen to not collide with a payload attribute); omitting `partition_by` reads more clearly than the explicit `:__id`. This is how a third-party command handler runs on Sourced as a durable queue. Non-exclusive reactors must declare a real `partition_by`; id-partitioning is only allowed for exclusive (sole-owner) reactors, which keeps id-indexing safe.

**Index basis.** Each message type is indexed by `payload` (default) or by `id`. The store derives this per type from the id-partitioned group that owns it (recorded in `register_consumer_group(handled_types:)`), so **every** append path — `handle!`, workflows, scheduled-message promotion, and the `ActionRunner` — indexes consistently. `Store#append(index_by:)` is an optional override.

The `StaleClaimReaper` calls `release_drained_offsets` (removes unclaimed partition offsets whose messages were all deleted) and `prune_orphan_key_pairs` (removes now-unreferenced key_pairs — relevant for high-cardinality id keys).

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

- `Sourced.configure` stores the block, applies it to the reused `Sourced.config`, and calls `config.setup!` (does not freeze). `Sourced.setup!` re-applies the block, re-establishes DB connections via `config.disconnect!`, and freezes the config — call it on boot/after-fork to make connections fork-safe.
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

Attributes use **native Ruby types** (`Date`, `Time`, `Symbol`, `BigDecimal`, `URI`, `Range`) — never coercing types. The codec (below) handles JSON translation at the store boundary. Free-form attributes take `Sourced::Types::JSONData` (any JSON value, recursively), not `Types::Any`, which no codec can encode.

### Codecs (`lib/sourced/store/message_codec.rb`)

Serialization is split between one global format and a store that owns everything else:

- **Format — global, unconfigurable.** `Plumb::Codec::JSON` decides how Ruby values travel (`Date` → `"2026-01-02"`, `Time` → ISO 8601 at microsecond precision). Apps teach it their own value types by registering encoders on the class itself — `Plumb::Codec::JSON.encoder MoneyEncoder` — before `Sourced.setup!`, which is when the store compiles them in. There is no codec setting on `Configuration` or `Store`.
- **Registry use — store-private.** `Sourced::Store::MessageCodec` is a `Sourced::Message::JSONCodec` (from the **sourced-message** gem) that registers each message class's **payload type** under its message type string. The base class owns the compiled-pair registry, the pair cache, `compile!`/`recompile!`, `decode` and the error classes; the subclass supplies only three private seams — `#compiled_type` (payload schema instead of the message class), `#encode_subject` (`message.payload`), and `#build` (reassemble the message around the decoded payload). It stays namespaced under `Store` because it exists for *this* store's layout, and nothing in `Configuration::StoreInterface` mentions serialization — a store with a different layout owns a different serializer, or none. Its `format:` kwarg is a seam for scoping a codec in specs, not configuration. Sidereal uses the same base class directly, unsubclassed, to encode whole messages onto its file store and socket pubsub.
- **Envelope — the store's own code.** `Store#append` writes `id`/`type`/`causation_id`/`correlation_id`/`created_at` straight into columns and only sends `payload` through the codec; `deserialize` rebuilds attrs from the row and calls `message_codec.decode`; `schedule_messages` assembles a whole-message document inline, because that table has a different layout. Routing the envelope through the codec instead costs ~7.7 µs/message on reads (measured) to rebuild what the store just took apart — see `plans/message-codecs.md`.

Other notes:

- **Payload scope is free for JSON-native payloads**: a codec composition returns the *original node* when nothing needs rewriting, so the decoder *is* the payload class.
- **Registration happens only in `compile!`**, which freezes the compiled pairs once every type is in. Encoding or decoding a type that wasn't compiled raises `Sourced::Message::JSONCodec::UnregisteredTypeError` — a bug, not a cue to compile mid-request. `compile!` is **idempotent**, so several collaborators sharing a codec can each call it on start without coordinating; `recompile!` is what picks up types (and encoders) registered since. Pairs are cached per message class on the codec *class*, so a recompile re-collects them rather than rebuilding. Specs that build stores directly rely on a `before(:suite)` hook compiling `MessageCodec.default`, mirroring the once-per-process compile at boot.
- **Boot check**: `Configuration#setup!` calls `store.setup!` — the store's generic "prepare yourself" hook (in `StoreInterface`), which for `Store` means creating tables *and* compiling its serializer. A message type the store can't persist raises `Plumb::TypeError` naming the message and attribute, and **the app fails to boot**. Specs needing a deliberately-unserializable message type must keep it *out* of the global registry — see `CodecSpecHelpers.unregistered_message` in `spec/spec_helper.rb`, since one registered example would fail every other spec's `setup!`.
- **Sharing**: `Store::MessageCodec.default` is the one instance every store takes, so a process compiles its pairs once, including after a fork. Assign `store.message_codec =` for a store that needs its own — e.g. one scoped to a private message registry.
- Encoding an invalid message raises `Sourced::Message::JSONCodec::EncodeError`; reading a row whose payload no longer fits its schema raises `Sourced::Message::JSONCodec::DecodeError` (both inherited, so `Store::MessageCodec::EncodeError` still resolves — but they descend from `StandardError`, not `Sourced::Error`); reading a row whose type isn't registered raises `Message::UnknownMessageError` (the base `Message` declares `payload` as `Static[nil]`, so building one would drop the payload).
- `metadata` is an untyped Hash — a JSON noop the codec passes through, so it must hold JSON-native values.

### Projector flavors

- `Projector::StateStored` — evolves only the claimed batch on top of the stored state snapshot.
- `Projector::EventSourced` — evolves from full history every claim (via `context_for`).

### Delete-on-ack queue reactor

A reactor can act as a durable queue: `exclusive` + no `partition_by` (id-partitioned), overriding `handle_claim` to return a `delete: true` ack that removes each handled message. **The Decider/Projector command DSL never emits `delete: true`** (it only builds `Append`/`OK`), so a queue drives `handle_claim` directly — either a `Sourced::Consumer` reactor or a plain duck-typed one (below).

```ruby
class Jobs
  extend Sourced::Consumer
  exclusive                 # sole owner of its types; no partition_by → id-partitioned

  def self.handled_messages = [RunJob]

  def self.handle_claim(claim)
    each_with_partial_ack(claim.messages) do |cmd|
      run(cmd.payload)      # ... do work, optionally append follow-ups ...
      # deletion is explicit — the returned ack carries delete: true:
      [{ type: :ack, delete: true }, cmd]
    end
  end
end
```

A third-party handler needs no Sourced base class — just respond to `handled_messages` + `handle_claim`, returning plain-Hash signals:

```ruby
class MyWorker
  def self.exclusive? = true
  def self.handled_messages = [DoThing]     # group_id/partition_keys/etc. default via ReactorDefaults
  def self.handle_claim(claim)
    claim.messages.map do |cmd|
      run(cmd)
      # append follow-ups + delete the handled command on ack:
      [[{ type: :append, messages: [DoNext.new(...)] }, { type: :ack, delete: true }], cmd]
    end
  end
end
```

### Scheduled / delayed messages

Scheduling is transparent: `append` a future-dated message (via `Message#at`) and the store defers it to the `scheduled_messages` table, promoting it into the log when due. There is no separate public scheduling method.

```ruby
cmd = SendReminder.new(payload: { course_id: 'c1' }).at(Time.now + 3600)
store.append(cmd)        # created_at in the future → deferred to scheduled_messages
store.update_schedule!   # manual promotion (normally done by ScheduledMessagePoller)
```

In reactions: `dispatch(Cmd, ...).at(time)` (the produced message is future-dated, so `append` schedules it).

## Store API highlights

- `append(messages, guard: nil, index_by: nil)` — the single write path. Immediate messages are written + auto-indexed (`index_by` optionally overrides the basis, otherwise resolved per message from the owning group); **future-dated messages (`created_at > now`) are transparently deferred to `scheduled_messages`** and promoted when due. Raises `ConcurrentAppendError` if guard is violated (rolls back any scheduling in the same call).
- `update_schedule!` — promote due scheduled messages into the log (the `ScheduledMessagePoller` calls this).
- `read(conditions, after_position:, limit:)` → `ReadResult(messages, guard)`.
- `read_partition(partition_attrs, handled_types:)` — AND-filtered read for loading reactor state.
- `read_all(after_position:, limit:, order: :asc, conditions: nil)` → `ReadAllResult` (lazy pagination via `to_enum`).
- `claim_next(reactor, worker_id:)` → `ClaimResult` with partition batch + guard. Supports compound partitions and replaying flag.
- `ack(claim, last_position:)` / `release(claim)` / `advance_offset(group_id, partition:, position:)`.
- `ack_and_delete(group_id, offset_id:, positions:)` / `delete_messages(positions)` — delete-on-ack helpers.
- `register_consumer_group(group_id, partition_by:, exclusive:, handled_types:)`, `start_consumer_group`, `stop_consumer_group`, `reset_consumer_group` (no-op for queue groups).
- `read_offsets(group_id:, limit:, from_id:)` → `OffsetsResult` (cursor-paginated, `to_enum`).
- `stats` → `Stats(max_position, groups)` including `error_context`.
- `worker_heartbeat` / `release_stale_claims` — claim liveness. `release_drained_offsets` / `prune_orphan_key_pairs` — queue cleanup.

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
- Reactor protocol: `lib/sourced/reactor_defaults.rb` (duck-typed defaults)
- Actions: `lib/sourced/{actions,action_runner}.rb` (signals + interpreter)
- Codecs: `lib/sourced/store/message_codec.rb` (payload seams) over `Sourced::Message::JSONCodec` in the sourced-message gem
- Mixins: `lib/sourced/{evolve,react,sync}.rb`
- Dispatch: `lib/sourced/{dispatcher,worker,work_queue,stale_claim_reaper,scheduled_message_poller,inline_notifier}.rb`
- Router/topology: `lib/sourced/{router,topology}.rb`
- Messages: `lib/sourced/message.rb` (includes `QueryCondition`, `ConsistencyGuard`)
- Testing: `lib/sourced/testing/rspec.rb`

## Local scratch (untracked)

`examples/app/` and `bench/*` are intentionally untracked (see commits `Untrack examples/app` / `Untrack bench files`). Files may exist locally for experimentation but must not be re-added to the repo without explicit approval.
