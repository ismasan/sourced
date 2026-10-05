## [Unreleased]

### Changed

- **Configuration is a component tree.** `Sourced.config` is the root of a
  [sourced-component](https://github.com/ismasan/sourced-component) tree built by
  `Sourced::Config.build`, declaring typed components with defaults and dependencies:
  `logger`, `db`, `notifier`, `executor`, `error_strategy`, `store`,
  `store.table_prefix`, `reactors.*`,
  `router`, `topology`, `workers.*`, `housekeeping.*` and `dispatcher`. Host apps mount
  it (`App.mount('sourced', Sourced)`) and override components; standalone apps
  override them on `Sourced.config` (`Sourced.configure` yields it). The tree can be
  inspected without booting (`Sourced.config.tree`, `.graph.to_mermaid`).
  Requires Ruby 3.2.
  - Removed `Sourced::Configuration` and its setters (`c.store =`, `c.worker_count =`,
    ...): implement components instead, ex. `c.config!('workers.count') { 4 }`.
    `Configuration::StoreInterface` is now `Config::StoreInterface`.
  - Removed `Sourced.setup!`. Boot with `Sourced.start!(task)` (or the host's
    `start!`); forking servers `Sourced.config.prepare!` before forking and start in
    each child. `Sourced.teardown!` stops workers and disconnects.
  - `Sourced.store`, `.router` and `.topology` raise `NotBuiltError` until Sourced is
    built, instead of setting up on first use. `Sourced.handle!` raises
    `ConsumerGroupNotRegisteredError` for a registered reactor whose consumer group
    isn't in the store yet (consumer groups are registered when Sourced starts).
    `Store#advance_offset` returns whether the group exists.
  - `Sourced.register` declares the reactor as `reactors.<group_id>`: registering two
    reactors with the same group_id raises, and so does registering after boot.
    Removed `Sourced.reset_topology`.
  - The `store` component compiles the default codec on `prepare!`, which touches no
    database, so a forking server prepares once in the parent and its children share
    the compiled codec. Register encoders and define message types before that.
  - `Dispatcher` is restartable: `#stop` waits for the run's workers, and `#start`
    runs fresh workers, pollers and work queue; notifications are dropped while
    stopped and the catch-up poll covers them. The `dispatcher` component uses a
    `stop` hook, so a host can `defer('dispatcher')` and start and stop it by key
    (`start_component!` / `stop_component!`), ex. only while it holds a leader lock.
    `#start` raises `Dispatcher::StillRunningError` while the previous run's workers
    are still running. Notifiers must support `start` after `stop`.
  - Removed `Dispatcher.start(task)`; the `dispatcher` component spawns workers into
    the context Sourced starts with. `Supervisor.new(config: Sourced.config)` boots
    the root of the tree, and replaces its old keyword arguments.
  - `Store.new` no longer reads a late-bound `Sourced.config.notifier`: it takes
    `notifier:` (default: its own `InlineNotifier`), and runs no queries until
    `install!`.
  - `Dispatcher#stop` waits for workers to finish the batches they're processing,
    for up to `workers.shutdown_timeout` seconds (default 30), and returns false if
    they don't; teardown uses `Dispatcher#stop!`, which raises
    `Dispatcher::ShutdownTimeoutError` instead, after the rest of the tree is torn
    down. A worker stopped before it runs no longer processes work.
  - `Config::StoreInterface` no longer requires `setup!`: how a store gets ready is
    the `store` component's lifecycle. The default component calls `Store#setup!` on
    start; a component implementing another store brings its own hooks.
  - `Router.new(store:, reactors:, error_strategy:)`; `Router#setup!` registers
    consumer groups and freezes the error strategy. Reactors no
    longer get a default `on_exception`: the router calls a reactor's own, or its
    error strategy.

- Requires plumb 0.4 and sourced-message 0.4.
- `Store::MessageCodec` encodes and decodes whole messages, not just payloads. The
  store writes the encoded payload and metadata as JSON and the envelope to columns as
  before, and decodes rows (and promoted scheduled messages) through the codec, so
  metadata is decoded by its schema and JSON is no longer parsed with
  `symbolize_names`. `#encode` returns the whole String-keyed document; field paths in
  compile errors are now prefixed `payload.`. Stored data is unchanged, so no migration.

- GWT helper (`Sourced::Testing::RSpec`): for a projector, the `given` events are the
  batch it consumes — they are handed to `handle_batch` as the new messages (and, as in
  production's unbounded read, as its history), so `.given(events).then!` runs the
  projector's `sync` / `after_sync` hooks with those events as `messages:`. `when` on a
  projector raises `ArgumentError`: there is no command to dispatch. `then!` now runs
  each hook exactly once — inside `compute_state` when a block is given, so effects on
  the yielded state are visible, and through the pipeline otherwise — and decider hooks
  see `events:` correlated to the command in both forms.

- `sync` and `after_sync` hooks now see appended messages as stored. `ActionRunner#run_pair`
  runs one action pair, accumulates the messages its `:append` signals stored — correlated
  by the runner, as always — and hands them to each `:sync` work as it runs and to each
  `:after_sync` work after the commit. A work that declares no parameters is called bare.
  `Sync#collect_actions` (and `sync_actions` / `after_sync_actions`) take a block that maps
  those messages to keyword arguments at call time; `Decider.handle_batch` uses it to bind
  `events:`, so a decider's hooks receive events carrying causation and correlation ids and
  `Sourced::Message#correlation_type`. Reactors themselves never correlate.
  `ActionRunner.correlate` is the single definition of that correlation, and the GWT
  helper (`with_reactor ... .then!`) uses it so hooks under test see the same messages.

- `Sourced::Store::MessageCodec` is now a subclass of `Sourced::Message::JSONCodec`, which
  lives in the **sourced-message** gem and is shared with Sidereal. It keeps its
  payload-only behaviour by overriding three private seams, and gains a per-class pair
  cache, `compiled?`, `recompile!`, `.reset!` and `.clear_pairs!`.
- `#encode_payload` is now `#encode` (the base class's name; it still encodes only the
  payload and still returns `nil` for a message declared without one).
- `#compile!` is idempotent — call `#recompile!` to pick up message types or encoders
  registered since the last compile.
- Encoding or decoding an uncompiled type raises
  `Sourced::Message::JSONCodec::UnregisteredTypeError` instead of `Plumb::Codec::NoEntryError`.
- `EncodeError` / `DecodeError` now descend from `StandardError` rather than
  `Sourced::Error`. `Store::MessageCodec::EncodeError` still resolves, by inheritance.

### Added

- **Message codecs.** Messages are declared with native Ruby types (`Date`, `Time`,
  `Symbol`, `BigDecimal`, `URI`, `Range`, …) and `Plumb::Codec::JSON` translates them to
  and from the store's JSON columns. Apps register encoders for their own value types
  directly on it (`Plumb::Codec::JSON.encoder MoneyEncoder`), before `Sourced.setup!`.
- `Sourced::Store::MessageCodec` — the SQLite store's serializer, namespaced under and
  owned by it. It registers each message class's payload type in a Plumb codec instance
  keyed by message type string, built and frozen at `setup!`; the
  envelope is the store's own business. The format is global and needs no configuring; a
  future store with a different layout owns a different serializer, or none. A message
  type the store can't persist raises `Plumb::TypeError` at `setup!` — the app fails to
  boot rather than raising on the first append.
- `Sourced::Types::JSONData` — any JSON value, recursively — for payload attributes
  whose shape isn't known up front.

### Fixed

- `Date` and `Time` payload attributes never round-tripped through the store: they
  were written via `#to_s` and read back as Strings, producing invalid messages. This
  affected `DurableWorkflow`'s `WaitStarted#at`.
- Scheduled messages recorded `scheduled_at` metadata via `Time#to_s` rather than ISO 8601.
- A message's `created_at` was stored at whole-second precision in the log while the
  scheduled-message blob kept microseconds. Both now come from the encoded message, at
  microsecond precision.

### Changed

- `Configuration::StoreInterface` requires `setup!` instead of `install!` / `installed?`.
  A store prepares itself however it needs to at boot; creating tables is one store's
  answer, not the contract. `Sourced::Store#setup!` creates its tables and compiles its
  serializer; `install!` / `installed?` remain public for scripts and tests.

- Reading a message whose stored payload no longer satisfies its schema raises
  `Sourced::Store::MessageCodec::DecodeError` instead of silently producing an invalid
  message. Appending an invalid message raises `Store::MessageCodec::EncodeError`.
- Reading a message whose type isn't registered raises
  `Sourced::Message::UnknownMessageError` instead of returning a base
  `Sourced::Message` with its payload silently dropped.
- `DurableWorkflow` workflow context and step outputs are typed `Types::JSONData`
  rather than `Types::Any`: values must be JSON-native, checked at append time
  rather than silently coerced to Strings on the way back out.

## [0.1.0] - 2024-09-27

- Initial release
