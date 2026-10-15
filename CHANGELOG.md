## [Unreleased]

### Changed

- **Evolve and reaction dispatch use handler maps.** `Projection.evolve_handlers` and
  `React.reaction_handlers` map each message class to its handler, filled when the handler
  is declared, instead of building and checking method names per message. Like
  `handled_messages`, they're copied when a class is subclassed: a handler declared on a
  parent after a subclass exists isn't seen by the subclass.
- **Reactor state lives in a `Sourced::Projection`.** A reactor's `state` /
  `evolve` declarations now define a projection subclass the reactor owns, and
  `#state` / `#evolve` delegate to it. Evolve blocks run in the projection's
  scope, so they no longer see the reactor's methods or collaborators.
  - **`state` blocks take no arguments.** Projections don't receive partition
    values: a state is a function of the events it evolves, so set identifying
    values (ex. ids) in `evolve` blocks. A `state` block that declares arguments
    (`state { |values| ... }`, `state { |_| ... }`) raises `ArgumentError`.
    `StateStored#load_state` still receives the partition values.
  - `before_evolve { |state, event| ... }` runs before the evolve handler of
    every event a projection (or reactor) handles, ex. to set ids or timestamps
    common to all events.
  - Projections can be standalone classes (`class CartProjection <
    Sourced::Projection`), shared between reactors with `evolve CartProjection`.
    Each reactor subclasses it, so its own extra `evolve` blocks don't leak.
  - `Projector::StateStored` loads stored state with a new `load_state` block,
    which runs in the reactor's scope. Move snapshot loading out of `state`
    blocks into `load_state`.
  - `Sourced::Evolve` can no longer be included into a plain class for state;
    use `Sourced::Projection`. `DurableWorkflow` no longer includes it.
  - The GWT helpers take projection classes: `with_reactor(CartProjection)`
    evolves the `given` messages and yields the state to a `then` block.
  - `Sourced.load(CartProjection, cart_id: 'c1')` loads a projection's state
    from the log. `Sourced.load` takes any class with `handled_messages_for_evolve`
    and `#evolve`. With `partition_keys` it builds `new(values)`; without it the
    given values are the partition and it builds `new`.
- **Message types are referenced by class only.** `reaction`, `#dispatch` and
  `Decider#event` no longer accept symbols (`reaction :student_enrolled`,
  `dispatch(:notify_student, ...)`), and the reactor-level `.[]` lookup is
  removed. Symbols were resolved against the global message registry, not the
  reactor's own messages, so they could match an unrelated type. Use the message
  constant instead.
- **Configuration is a component tree.** `Sourced.config` is the root of a
  [sourced-component](https://github.com/ismasan/sourced-component) tree built by
  `Sourced::Config.build`, declaring typed components with defaults and dependencies:
  `logger`, `db`, `notifier`, `executor`, `error_strategy`, `store`,
  `store.table_prefix`, `reactors.*`,
  `router`, `topology`, `workers.*`, `housekeeping.*` and `dispatcher`. Host apps mount
  it (`App.mount('sourced', Sourced)`) and override components; standalone apps
  override them on `Sourced.config`; `Sourced.configure` is removed. The tree can be
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
  - The store's codec is its own component, `store.codec`: a `Store::MessageCodec`
    compiled from the message registry on `prepare!` and `build!`, and passed to the
    store (`Store.new(db, message_codec:)`). Preparing touches no database, so a
    forking server prepares once in the parent and its children share the compiled
    pairs. Register encoders and define message types before that, or recycle
    `store.codec` to pick up message types defined since (ex. by a class reloader),
    which rebuilds the store and everything built on it.
  - `Dispatcher` is restartable: `#stop` waits for the run's workers, and `#start`
    runs fresh workers, pollers and work queue; notifications are dropped while
    stopped and the catch-up poll covers them. The `dispatcher` component uses a
    `stop` hook, so a host can `defer('dispatcher')` and start and stop it by key
    (`start_component!` / `stop_component!`), ex. only while it holds a leader lock.
    `#start` raises `Dispatcher::StillRunningError` while the previous run's workers
    are still running. Notifiers must support `start` after `stop`.
  - The catch-up poller, stale claim reaper and scheduled message poller are components
    (`dispatcher.catchup_poller`, `dispatcher.stale_claim_reaper`,
    `scheduled_messages.poller`, with `scheduled_messages.interval`) instead of being
    embedded in the dispatcher: each can be deferred, stopped and started by key, the
    all three along with the dispatcher they depend on (so a leader-only dispatcher keeps
    leader-only pollers), and each is waited for on stop. They're built on
    `Sourced::PeriodicTask` (`start(context)`, `stop`, restartable). The scheduled
    message poller depends on the dispatcher only to run where it runs; re-implement it
    with a dependency on `store` alone to promote in a process without workers.
    `Dispatcher.new` no longer takes
    `catchup_interval:`, `housekeeping_interval:` or `claim_ttl_seconds:`, and
    exposes `#push(reactor)` and `#reactors`. `Dispatcher::ShutdownTimeoutError` is
    `Sourced::ShutdownTimeoutError`, raised by the pollers' `stop!` too.
  - Components spawn through `Sourced::Spawner`: an Async task, an executor task, or
    threads for any other context, so a bare `Sourced.start!` or a host's `App.start!`
    runs workers and pollers in threads.
  - Removed `Dispatcher.start(task)`; the `dispatcher` component spawns workers into
    the context Sourced starts with. `Supervisor.new(config: Sourced.config)` boots
    the root of the tree, and replaces its old keyword arguments.
  - `Router#stop_consumer_group`, `#start_consumer_group`, `#reset_consumer_group` and
    `#handle_next_for` raise `Router::UnregisteredReactorError`, naming the registered
    reactors, for a group_id or reactor class the router doesn't know (was `ArgumentError`
    for a group_id; a class was passed through to the store).
  - `Store#stop_consumer_group`, `#start_consumer_group`, `#reset_consumer_group` and
    `#updating_consumer_group` raise `Store::UnknownConsumerGroupError`, naming the
    registered groups, for a group that isn't registered (start and reset were silent
    no-ops, and start announced a resume for a group that didn't exist; stop raised
    `ArgumentError`).
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
  - Removed `Store#setup!`: the store has no boot step of its own. The migration (and
    `Store#install!`, which applies it) creates the tables and sets `journal_mode = WAL`,
    outside a transaction; per-connection PRAGMAs are Sequel's; the stale claim reaper
    runs `ANALYZE` on its first tick, then hourly; and the `store` component's start hook
    checks the tables are installed (`Store::NotInstalledError`, pointing at the
    migration) and compiles the codec. Apps install tables with a migration;
    `store.install_tables` (default false) makes `Store#install!` create them when the
    `store` component starts, ex. for an in-memory database in tests. A store built
    directly (`Store.new(db)`) installs on `install!` unless given `install_tables: false`.
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

- `Sourced::Machine`, Sourced's statecharts. A machine's position is made of
  *checkpoints*, declared with `Checkpoint(:name)`: each generates an event class (type
  `<prefix>.runtime.<name>`, prefix from `group_id`) that is both the handle the chart
  places (`initial`, `checkpoint`, `final`, `start`) and the event appended when the
  machine moves there, recording `trigger`, `source`, `target` and the `exited` /
  `entered` paths, which replay applies. Checkpoints list the commands they accept and
  the checkpoints each may move to (`command PlaceOrder => Placed`, `=> A | B` or
  `=> A | nil` with `to(A)` in the block, or data-only without an arrow); `event` appends
  data events, evolved into the machine's domain `state`. Checkpoints can have `region`s,
  all active while the checkpoint is: commands go to the deepest active checkpoints that
  accept them, can target an ancestor region's checkpoint or one below their own, and
  `done Target` joins once every region is final. Eventless transitions: `always Target,
  if: guard` (transient checkpoints with an unguarded one) and guarded `done`, guards
  being a Symbol sent to the state or any `#call(state)`. `on Event => Target` moves the
  machine when it claims another reactor's event. `Checkpoint.history` /
  `history(deep: true)` re-enter a checkpoint's regions where they were. Entry and exit
  actions run as reactions when the transition is claimed; `after(delay) { dispatch Cmd }`
  blocks (seconds, or a duration string such as `'1h30m'`) dispatch commands delivered that
  long after the checkpoint was entered, refused once that entry has been left. Child
  machines run in their own partitions (keys including the parent's): a child declares
  `start` and `on_stop`, generating `Started`, `Finished` and `Stop`
  (`<prefix>.runtime.child.*`); a parent `spawns Child, as:, stop_on:` and `spawn`s it
  from a command, joins with `children_done`, or `invoke`s one for the duration of a
  checkpoint, mapping its finals with `on_done`. Commands the current checkpoint doesn't
  accept raise `CommandNotHandledError` in `Sourced.handle!` and are acked by workers.
  Definitions are checked when the machine is registered, after which its DSL is locked;
  stored paths that no longer resolve raise `UnresolvablePathError`. `Sourced.topology`
  describes machines, `Machine.to_mermaid` draws one as a Mermaid state diagram,
  `Machine.to_h` returns the definition as a JSON-serializable, XState-shaped Hash, and
  `Machine#to_h` the same Hash with the instance's status. Design in
  `plans/statecharts-transitions.md`.
- Composing machines: `region :payment, PaymentMachine, on_done: { PaymentMachine::Expired
  => Cancelled }` runs another machine as a region, and `checkpoint Working, ThingMachine,
  on_done: { … }` as a compound checkpoint whose sub-states start at its `start` (which can
  itself be compound). A composed machine runs in the parent's partition and instance, its
  projection built from the parent's state; it escalates through its root finals, which
  `on_done` maps onto the parent's checkpoints. The mounting machine appends its own
  generated classes for the composed machine's checkpoints
  (`OrderMachine::Mounted::PaymentMachine::Captured`, type
  `order_machine.runtime.mounted.payment_machine.captured`, see `checkpoint_class`), so a
  machine can be composed several times, and never reads another context's events.
- `Decider#handles_command?(command)`: whether the decider handles a command in its
  current state. Defaults to whether a handler is registered for the command's class;
  override it to refuse commands based on state without raising, the command-side
  counterpart of `should_react?`. `Sourced.handle!` raises
  `Sourced::CommandNotHandledError` when it's false, appending nothing, and background
  workers ack the command with no events.
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

- Reactor subclasses inherit their superclass's `partition_by` keys unless they declare
  their own. They had no partition keys, so their partition reads came back empty.
- `Sourced.handle!` skipped the reactor's reactions to its own events: it advanced the
  reactor's offset past everything it appended, so workers never claimed those events.
  It now advances offsets past the command only, appending the command and its events
  separately within one transaction.
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
