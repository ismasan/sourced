# Statecharts: implementation plan

Implements the design in `plans/statecharts.md`. One commit per phase, on the `statecharts` branch.

## Approach

`Sourced::Machine` is a reactor class of its own, not a generated Decider. Like `Decider`, it includes `Evolve`, `React` and `Sync` and extends `Consumer`, which gives it `context { |keys| … }`, `evolve`, `dispatch` / `.at`, the sync hooks and `partition_by`. It implements its own `command` (arrow forms), `decide` and `handle_batch`.

Why not subclass `Decider`:

- `Decider.command` conflicts with the arrow forms.
- `Decider.inherited` copies handlers between classes.
- Owning `handle_batch` lets a machine decide joins (`done`, `children_done`, `on_done`) in the batch that claimed the external fact, under the guard. No generated command, no extra hop.

It keeps the signatures that `Sourced.handle!`, `Sourced.load`, the Router and the GWT helpers rely on: `new(values)`, `#decide(command)`, `handled_messages`, `handled_messages_for_evolve`, `handle_claim(claim, history:)` and `handle_batch(partition_values, new_messages, history:, replaying:)`.

## Constraints found in the code

1. **`handle!` skips a reactor's own reactions.** It advances the reactor's offset to the last position it appended, so the reactor never claims its own events. Confirmed with a plain Decider (`Do → Done`, `reaction(Done) { dispatch Notify }`): through `handle!` with live workers, `Notify` never appears. Machines rely on own reactions for `entry`, `exit` and `after`.
2. **GWT history has no positions.** The helpers pass plain messages, and `Evolve` never reads positions. Region history is scoped by evolve order (the runtime notes the entering checkpoint, and ignores earlier region checkpoints), not by `message.position`.
3. **Decisions triggered by events skip on replay.** Reactions already skip via `should_react?`. A join decided on an external fact would re-append its checkpoint after a consumer-group reset, so event-triggered decisions take the same gate. Decisions on commands don't replay.
4. **Timers need no generated message type.** `after` blocks dispatch user commands with `.with_metadata(entered_by: evt.id)`. A command carrying `entered_by` that isn't the current checkpoint's entering event is stale.
5. **The only generated message type is the per-machine `Stop`** (phase 4). It's created when `on_stop` is declared, which must come after `partition_by`, so it's registered before the codec compiles. Its constant is set on the machine class, so handler method names derived from class names are stable.
6. **Definition-time checks need the whole class body**, so they run on registration (`Sourced.register` / `Router#register`), with `Machine.validate!` for specs.

## Commands a machine doesn't accept

Split by source, through one protocol method:

- **`#handles_command?(command)`**, an instance method, because the answer depends on loaded context. `Decider`'s default is `self.class.handled_commands.include?(command.class)`. `handle!` only asks reactors that define it (`ReactorDefaults` fills in class methods at Router registration, and `handle!` doesn't need a registered reactor). `Machine` scopes it to the active checkpoints, and answers `false` for a stale timer command. The class-level `handled_commands` stays for routing.
- **`Sourced.handle!`** asks it before deciding. On `false` it raises `Sourced::CommandNotHandledError` and appends nothing.
- **Workers:** the reactor's `handle_batch` asks it per claimed command, and on `false` acks with no events.

It's the command-side counterpart of `reacts_to?` / `should_react?`: a Decider can override it to refuse a command based on its context without raising.

## Phases

| # | Commit | Contents |
|---|---|---|
| 0a | `handle!` runs the decider's own reactions | Advance registered offsets to the command's position only, so workers claim the decider's own events. |
| 0b | `handles_command?` | Protocol method with the Decider default; `handle!` raises `CommandNotHandledError` for reactors that define it; `Decider#handle_batch` acks commands it doesn't handle. |
| 1 | Flat machines | `Sourced::Machine`; `context` / `evolve`; `initial` / `checkpoint` / `final`; the five command forms and the one-checkpoint check; auto-appended checkpoints; partition keys filled in on emitted and dispatched messages; read-only `machine` view; `load` snapshot (`value`, `context`); `entry` / `exit`; `handles_command?`; definition-time checks; GWT specs. |
| 2 | `after` timers | `entered_by` metadata, stale check, scheduled-message specs with Timecop. |
| 3 | Regions | `region`, configuration across regions, history scoping by entering checkpoint, ancestor targets, multi-region commands in definition order, `done` joins: in the deciding command, or in `handle_batch` for external facts (gated by `replaying`). |
| 4 | Child machines | `spawns` / `spawn`, `start`, `on_stop` with the generated `Stop`, `stop_on:`, children in runtime state (start and final checkpoints only), `children_done`, `invoke` (`keys:`, `on_done`, stopped on leaving), partial-key-only-evolve check. End-to-end specs over a store and router. |
| 5 | Topology and docs | Machine graph in `Sourced.topology` (checkpoints and commands, not just message flows), README section. |

Phases 1 to 3 run within one partition. Phase 4 is the first with several reactors, so it gets the store-backed specs.

## Progress

- **0a, 0b:** done.
- **1:** done. Differences from the plan:
  - The buildable check applies to blockless commands only. A single checkpoint auto-appended after a block isn't checked at definition time, because the block may emit it itself; it's validated when built (`InvalidEventError`).
  - Every checkpoint type is claimed, with or without entry/exit actions, so the claim path already sees checkpoints appended by other reactors (phase 3's joins).
  - `handle_claim` evolves the full, unbounded history, which already holds the claimed checkpoints: commands decide on the latest state, and entry/exit actions see the whole entering decision (a data event emitted after the checkpoint in the same decision). The runtime records which checkpoint each checkpoint event replaced, for exit actions.
  - No log line for commands workers ack because they aren't accepted (the Router has no logger); still open below.
- **2:** done. `after` blocks dispatch ordinary commands, future-dated from the entering event and tagged `metadata[:entered_by]`. Since `Message#correlate` copies metadata along correlation chains, the tag alone would leak into later commands. So a command counts as a timer only when its `entered_by` equals its `causation_id` (it was dispatched in reaction to that checkpoint), and it's stale when that event no longer entered the current checkpoint. Timers aren't started for a checkpoint the machine had already left when it claimed the event.

- **3:** done. Deepest wins: a checkpoint's handler runs only when none of its active regions accepts the command. Differences from the plan:
  - Regions nest to any depth (the runtime is a tree: a current checkpoint per region), rather than one level.
  - Region names and checkpoint classes are unique within a machine, so `machine.in?(:payment, PaymentStarted)` and `entered_by(:payment)` can name a region directly.
  - Region history scoping is "a checkpoint of an inactive region is ignored, and entering a checkpoint resets its regions", which needs no positions.
  - Joins are evaluated after every decision, and when any checkpoint event is claimed (not replaying), against the latest state; the guard makes concurrent joins safe.
  - Timers in regions are stale once their checkpoint entry is no longer active anywhere in the configuration.
- **4:** done. Differences from the plan:
  - `invoke` takes no `start:`: the child declares its own `start`. `keys:` receives the domain context only, since a checkpoint can be entered by a join or by a checkpoint appended elsewhere, not only by a command.
  - One loop decides everything that needs no command, after every decision and whenever a checkpoint or a child's start/final checkpoint is claimed (not replaying): `done` joins, `children_done` joins, invoking a child for the current entry of an active invoking checkpoint, and `on_done` mappings.
  - `children_done` can't declare `nil` (it would fire forever), and only fires once at least one child was spawned.
  - A child class can be registered once per parent, and must be named (its `Stop` type derives from its name).
  - Reactions must use types that declare every partition key, like commands and checkpoints.
- **5:** done. Topology has a machine branch: command nodes list their declared checkpoints without parsing, plus the events and children their blocks emit and spawn (Prism, through `SourceAnalyzer#calls_in_block`, which reads a block's `source_location`). Entry/exit/`after` actions, `done` and `children_done` joins, `invoke`, `on_done` and stopping children are automations. `Machine.to_mermaid` draws a `stateDiagram-v2`; transitions to an ancestor region's checkpoint start from the composite state containing the source, since Mermaid can't draw between states in different composites. The output hasn't been rendered with a Mermaid tool here. README has a Machines section.

## Open

- Logging a command a worker acks because it isn't accepted. Reactors have no logger; an `:ack` signal with a `reason:` that the Router logs is one option.
