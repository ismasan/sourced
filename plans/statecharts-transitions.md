# Statecharts: states as handles, transitions as events

A redesign of `Sourced::Machine` (`plans/statecharts.md`, `plans/statecharts-implementation.md`). Checkpoints stop being event classes. States become handles: plain values that name a position in the chart. The machine records each transition as a message of its own (`TourBooking::Transitioned`), whose payload says where the machine went. Domain facts become data events.

Still on the `statecharts` branch, so nothing released breaks.

## Why

Porting a realistic flow (two machines, four compound states, five parallel regions, from an XState chart) needed a workaround for each of these:

1. **A compound state can't start in a compound state.** A compound checkpoint and its machine's `start` are the same event class, so one event would have to be the start at two levels. The chart's `planning.itinerary.outline` had to be flattened, and a machine whose first state is compound can't be spawned.
2. **Re-entering a compound's start re-enters the outermost compound.** A command inside `Itinerary` targeting `Outline` exits and re-enters every enclosing compound that starts at `Outline`, re-running their entry/exit actions and restarting their timers. XState exits only below the transition's domain (the least common compound ancestor of source and target). With one event class per position at several levels, replay can't tell which level a decision meant.
3. **A machine can't be mounted twice.** Its checkpoints would be ambiguous. Five supplier regions of the same shape needed five generated machines and twenty generated event classes.
4. **No guarded choice or `always` transitions.** Each became a checkpoint whose entry dispatches a command that makes the choice: an extra round trip, and a state that exists only for the machinery.
5. **No history states.** Rebuilt by hand in domain context plus an entry action.
6. **A compound's own commands can't target its sub-states.** The command was repeated on every sub-state instead.
7. **Every eventless target needs an event class buildable from partition keys alone** (`done`, `on_done`), so escalations needed one event per level (`Quote::Accepted` → `Reserving::Started`).

They share a cause: an event class is both a domain fact and a position. Separating the two is the XState/SCXML model, where states and events are orthogonal and a transition targets a state node.

## The model

| | Now | Here |
|---|---|---|
| A state | an event class | a `Checkpoint` handle, declared in the machine |
| Being in a state | the region's latest checkpoint event | the configuration in the latest `Transitioned` |
| A transition | appending the target's event | appending `<Machine>::Transitioned` |
| Domain facts | checkpoint payloads, or data events | data events |
| Identity of a position | the event class (unique per tree) | its path in the placed tree |

### What's kept

- One machine, one partition, run to completion under one guard.
- Commands, `evolve`, data events, `entry` / `exit` as reactions, `after` timers with `entered_by`, regions, `done`, mounting (`region :x, M`, compound `checkpoint X, M`), children (`spawns`, `invoke`, `on_stop`, `children_done`), `handles_command?`, the static transition graph and definition-time validation.
- Command forms and the union syntax, now over handles.

## DSL

```ruby
class TourBooking < Sourced::Machine
  partition_by :tour_id, :booking_id

  # States: handles, declared before the chart so transitions can name
  # states declared further down.
  Planning  = Checkpoint(:planning)
  Booked    = Checkpoint(:booked)
  Lapsed    = Checkpoint(:lapsed)
  Withdrawn = Checkpoint(:withdrawn)
  Cancelled = Checkpoint(:cancelled)

  # Data events: domain facts, as now.
  BookingRequested = Sourced::Event.define('tour_booking.requested') { ... }

  context { Booking.new }
  evolve(BookingRequested) { |booking, evt| ... }

  initial do
    command RequestBooking => Planning do |_booking, cmd|
      event BookingRequested, cmd.payload.to_h
    end
  end

  # A compound state: PlanningMachine's chart, entered at its start.
  checkpoint Planning, PlanningMachine, on_done: {
    PlanningMachine::Finalised => Booked,
    PlanningMachine::Expired => Lapsed
  } do
    command Withdraw => Withdrawn
  end

  checkpoint Booked, TravelMachine do
    command CancelBooking => Cancelled
  end

  checkpoint Lapsed do
    command Revive => Planning
  end

  final Cancelled
end

class PlanningMachine < Sourced::Machine
  Itinerary = Checkpoint(:itinerary)
  OnHold    = Checkpoint(:on_hold)
  Expired   = Checkpoint(:expired)
  Finalised = Checkpoint(:finalised)

  start Itinerary, ItineraryMachine do   # a compound start: gap 1
    command Hold => OnHold
    command Finalise => Finalised
  end

  checkpoint OnHold do
    after(hold_limit) { dispatch Expire }
    command Resume => Itinerary.history  # shallow history: gap 5
    command Expire => Expired
  end

  final Finalised                         # mapped by the mounting machine's on_done
  final Expired
end

class ItineraryMachine < Sourced::Machine
  Outline  = Checkpoint(:outline)
  Proposed = Checkpoint(:proposed)

  start Outline do
    command Propose => Proposed | nil do |booking, _cmd|
      to Proposed if booking.proposable?
    end
  end

  checkpoint Proposed do
    command Revise => Outline   # exits below Itinerary only: gap 2
  end
end
```

### States

- **`Checkpoint(:name)` declares, `checkpoint` places.** `Machine.Checkpoint(name)` builds a `Sourced::Machine::Checkpoint` handle, records it on the machine, and returns it, in the style of `Kernel#Integer`. `checkpoint Handle do … end` places it in this region and declares its transitions; `start Handle` and `final Handle` likewise. Placing never declares and declaring never places, so no macro changes meaning with its argument's type.
- **Symbols are coerced, not a second meaning.** `checkpoint :outline do … end` and `command X => :proposed` resolve the Symbol to the machine's declared handle of that name, or declare one when placing (`checkpoint :outline`), for states nothing references before they're placed.
- **Handles are recorded on their machine**, which gives definition-time checks a handle declared but never placed, and two handles with one name. A handle placed by a machine that didn't declare it is rejected: a machine's chart only uses its own states.
- **A handle is a value**: its name and its declaring machine. It isn't placed until the tree is built, so one handle can appear at several paths when its machine is mounted more than once (gap 3).
- **Names are unique per machine, paths per tree.** A machine declares each name once, but mounting it twice places its handles at two paths: `hotel.holding` and `flights.holding` are one handle, `SupplierMachine::Holding`.
- **`Handle | Other`** builds a union, and `Handle | nil` one that may stay. As now, `nil | Handle` is boolean or: write `nil` last. Plain Symbols are accepted as shorthand (`command X => :paid`, `=> [:paid, :declined]`), resolved within the declaring machine at validation.
- **A compound state is a handle placed with a machine** (`checkpoint Planning, PlanningMachine`), entered at that machine's `start`. The handle and the machine's start are now separate states, one level apart, so the "must start at it" rule and the `<type>/start` node go away, and `start Itinerary, ItineraryMachine` nests as deep as needed.
- **Handles are Ruby `Data` values, not Plumb types.** Plumb's `|` is on types and builds parsers, so it would need a class per state and flattening of `Or` trees (today's `__flatten_targets`). A target set only needs membership:

  ```ruby
  Checkpoint = Data.define(:name, :machine) do
    def |(other) = Targets.new([self]) | other
  end

  # The checkpoints a transition may go to; nil means it may stay.
  Targets = Data.define(:members) do
    def |(other)
      case other
      in Targets then with(members: members | other.members)
      in Checkpoint | nil then with(members: members | [other])
      end
    end
  end
  ```

- **Why not `state(:name)`:** `state { … }` already builds the domain state. `Checkpoint` keeps the existing word, capitalised as a constructor.

### Transitions in handlers

- **Blockless:** `command Propose => Proposed` transitions to `Proposed`.
- **With a block:** the block calls **`to(Handle)`** to choose a target among the declared ones, and may append data events with `event`. A block that doesn't call `to` transitions to the declared target when there's exactly one, and stays when the union includes `nil`. Otherwise it's a `TransitionError` (today's `CheckpointError`).
- **One transition per handler**, as now. Several handlers for one command (sibling regions) each make their own.
- **Data events no longer double as positions**, so they never need to be buildable from partition keys.

### Eventless transitions (gaps 4, 7)

```ruby
checkpoint Reviewing do
  always Cancelled, if: ->(trip) { trip.any?('cancelled') }
  always Refused,   if: ->(trip) { trip.any?('refused') }
  always Confirmed, if: :all_confirmed?
  always AwaitingSuppliers
end
```

- **`always Target, if: guard`** is evaluated in order, as soon as the state is entered, within the same decision. The first match transitions. A guard is anything with `#call(context)` (a lambda, a module, an object), or a Symbol sent to the domain context. `to_h` renders a Symbol by name, a module or object by its (class) name, and a lambda as `guard`.
- **A state with an unguarded `always`** is transient: it can't have `after` or accept commands. Checked at definition time.
- **`done Target`** and **`on_done: { Final => Target }`** take handles. `done` accepts several guarded targets like `always`, for a guarded join (`done Confirmed, if: :all_confirmed?`, then `done Partial`).
- **Targets need no payload**, so escalation needs no event per level (gap 7).
- **`MAX_JOINS`** still caps eventless steps per decision.

### Targets and domains (gaps 2, 6)

- **Target resolution:** a handle in a transition resolves to the placement of that handle in the declaring node's lineage, innermost first. So `Revise => Outline` inside `ItineraryMachine` means *this* `Itinerary`'s `Outline`, wherever `ItineraryMachine` is mounted.
- **Reachable targets:** any state in the declaring node's region or an ancestor region, as now, plus **descendants of the declaring node** (gap 6: a compound's command targeting its own sub-state). Sibling regions stay unreachable. Checked at definition time.
- **The domain** is the least common compound ancestor of source and target. Exited: the active states below the domain, innermost first. Entered: the path from the domain down to the target, then default entry below it (each compound's start, each parallel state's regions at their initial positions), outermost first.
- **Self-transitions:** a compound's own command targeting itself re-enters it (exit, entry, regions reset), as XState's `reenter: true`. A command inside it targeting its start doesn't touch the compound.
- **Mounted machines target the mounting machine's states only through `on_done`**, so a machine stays mountable anywhere. Allowing direct references to ancestor handles is left open.

### History (gap 5)

- **`Compound.history`** (shallow) and **`Compound.history(deep: true)`** are pseudo-targets. A transition to one enters the compound at the child the region was last in, or its start if it has none.
- The runtime keeps each region's last active child, updated when the region exits. Since it's derived from `exited`, replay restores it without extra payload.

### External events

- **`on SomeEvent => Target`** in a checkpoint block: a transition triggered by an event the machine evolves (from another reactor, or a child). It replaces appending a checkpoint from outside, as in the old README's `final PaymentCaptured # appended by a payments reactor`.
- It's decided in `handle_batch` when the event is claimed, under the guard and gated by `replaying`, like `done` joins on external facts today.
- `on SomeEvent => A | B do |context, evt| … end` uses `to`, as for commands.

## The `Transitioned` message

> Replaced by an event class per checkpoint: see [Revision: an event class per checkpoint](#revision-an-event-class-per-checkpoint). The payload's transition attributes, paths and replay rules below still apply.

Each machine with `partition_by` generates `<Machine>::Transitioned` (`"#{type_prefix}.transitioned"`), set as a constant on the class when `partition_by` is declared, as `Stop` is with `on_stop`. A mounted machine's transitions use the top machine's type.

```ruby
attribute <partition keys>
attribute :trigger,       String          # command or event type, or 'done' / 'on_done' / 'always' / 'start' / 'stop'
attribute :source,        String          # path of the state that declared the transition
attribute :target,        String          # path of the declared target (a history pseudo-target resolved)
attribute :exited,        Array[String]   # paths, innermost first
attribute :entered,       Array[String]   # paths, outermost first
attribute :configuration, JSONData        # machine.value after this transition, by path segment
```

- **One `Transitioned` per transition.** A command handled by three regions appends three. A `done` join, an `on_done` escalation and each `always` step append one each. The decision is the macrostep; each event is a microstep.
- **Paths** join state names and region names with dots, from the root: `reserving.flights.holding`. A compound state's sub-states follow it directly (`planning.itinerary.outline`); a parallel state's go through the region name.
- **Replay doesn't re-run transition semantics.** Evolving a `Transitioned` sets the configuration from its payload, and applies `entered` / `exited` to per-region bookkeeping (entering event, visited set, last child for history). Entry/exit sets are computed once, at decide time. So history stays readable after the definition changes, as long as its paths still resolve (see Renames).
- **Entry/exit actions and timers** run when the machine claims a `Transitioned`, from its `exited` and `entered`, skipped when replaying, as now. `entered_by` is the `Transitioned` event's id.
- **Causation** is the command or event that triggered the decision, as for any decided event.

### Children

Today a parent evolves only a child's start and final checkpoints, so that busy children don't contend for the parent's guard. One `Transitioned` type per machine would wake the parent on every child step. So a machine that can be a child (`start` plus `on_stop`) also gets:

- **`<Machine>::Started`**: appended by the parent's `spawn` / `invoke` in the parent's decision, entering the child's start (with default entry, so a compound start works: gap 1).
- **`<Machine>::Finished`**: appended along with the `Transitioned` that enters a root final, carrying that final's path.

The parent evolves and claims these two, as it does the child's start and root finals today. `ChildRef#state` becomes the final's handle, or `nil` while running. `on_done` and `children_done` map the child's root final handles.

## Reading the machine

- **`machine.value`**: the configuration, with handles: `{ Planning => { Itinerary => Proposed } }`, or `{ Reserving => { hotel: Holding, flights: Holding, … } }`. Where a handle is placed more than once, the region name disambiguates, as now.
- **`machine.in?(Proposed)`**, **`machine.in?(:flights, Holding)`**, and **`machine.in?('reserving.flights.holding')`**.
- **`Machine.to_h`**: node ids are paths, which removes the `<type>/start` ids. Nodes gain history nodes where something targets a checkpoint's history (see Phase 8 in Progress) and `always: [{ target:, guard: }]`; mounts' `on_done` edges stay. `status.value` mirrors `machine.value` with paths.
- **Mermaid** ids are paths with `.` → `_`.
- **Topology**: commands lead to the machine's `Transitioned` with target state names, instead of to checkpoint event types. Data events appear as now.
- **`Sourced.load`** and **`Snapshot`** unchanged in shape.

## Renames

Paths in stored `Transitioned` events refer to state and region names. Renaming or moving a state would make old configurations unresolvable.

- **An unresolvable path raises** on evolve (fail fast), naming the path and the event id. No silent fallback to `initial`.
- **Renaming, moving or removing a state** that stored histories refer to isn't supported by this plan: keep the name.[^renames]

## What's lost, and the replacements

| Lost | Replacement |
|---|---|
| Custom events as transitions, with domain attributes | data events appended in the same decision |
| Other reactors appending a checkpoint to move the machine | `on Event => Target` |
| Readable logs (`tour_booking.paid`) | `tour_booking.transitioned` with `target`, plus data events; the Sourced UI renders `source → target` |
| Subscribing to one state's event type | subscribing to `<Machine>::Transitioned` (or `Finished`) and filtering on `target` |
| Old histories | none: breaking change on an unreleased branch |

## Phases

One commit per phase, specs first.

| # | Contents |
|---|---|
| 1 | **Handles and the tree.** `Checkpoint` handles and `Machine.Checkpoint(name)`, unions, Symbols coerced to handles. Placement by path; names unique per region; a machine mounted more than once. Definition-time checks rewritten over handles (reachability including descendants, finals, transient states). |
| 2 | **`Transitioned` and replay.** Generated type; the runtime's configuration and per-region bookkeeping from `entered` / `exited`; `machine.value`, `in?`, `load`. Flat machines and command forms (`to`, unions, `nil`, data-only) on the new model; the GWT helpers. |
| 3 | **Transition semantics.** Target resolution by lineage, domains, exit/entry sets, default entry through compound starts (gap 1), parallel states, self-transitions and re-entry (gap 2), descendant targets (gap 6), multi-region commands. |
| 4 | **Actions and timers** from `Transitioned`: `entry` / `exit` order, `after` with `entered_by`, stale timers. |
| 5 | **Eventless.** `always` with guards, transient states, guarded `done`, `on_done` (gaps 4, 7), `MAX_JOINS`. |
| 6 | **External events.** `on Event => Target` decided in `handle_batch`, gated by `replaying`. |
| 7 | **Children.** `Started` / `Finished`, `spawns`, `invoke`, `on_stop` / `Stop`, `children_done`, child `on_done`, compound child starts. Store-backed specs. |
| 8 | **History.** Shallow and deep pseudo-targets (gap 5). |
| 9 | **Rendering.** `to_h`, Mermaid, Topology. |
| 10 | **Unresolvable paths raise**, naming the path and the event id. Mapping old names isn't part of this plan.[^renames] |
| 11 | **Docs and examples.** README, CHANGELOG, `CLAUDE.md`'s Machine notes; port the specs' machines and the holiday booking example (sidereal `examples/machine`) as acceptance tests, extended to exercise every gap listed under Why with none of the workarounds. |

Phases 1 to 3 replace the runtime's core and can't ship separately: the suite is red between them. 4 to 10 are each independent once 3 lands.

## Open

- **Renaming the domain `state`.** It stays for now. If Sourced later renames it to `context` everywhere (XState's `value` / `context` split), the handle constructor could become `State(:name)` with `state` placing it.
- **Direct references to ancestor states from a mounted machine** (`Proposed` targeting `TourBooking::Cancelled`) instead of `on_done`. Simpler for machines that are only ever mounted in one place; couples them to it.
- **One `Transitioned` per decision** (a list of transitions) instead of one per transition. Fewer events, but subscribers then see several moves at once.
- **Storing the full configuration** in every `Transitioned`. Redundant with `entered` / `exited`, but makes any single event self-describing for UIs and children. Measure the payload on the acceptance examples.
- **Guards as data for rendering.** Named guards (`if: :all_confirmed?`) render on the edge; lambdas render as `guard`.

## Revision: an event class per checkpoint

One generic `<Machine>::Transitioned` type makes logs hard to read (`tour_booking.transitioned` everywhere). It also means nothing can subscribe to a single state, and Topology can't name targets. This revision generates an event class per checkpoint (`PaymentMachine::Captured`) and keeps what `Transitioned` gave us.

The old design's problems (gaps 1, 2, 3, 7) didn't come from one class per checkpoint. They came from the event class *being* the position. Positions stay paths in the payload (`source`, `target`, `exited`, `entered`), so a class per checkpoint only names the event, and the gaps stay closed:

- A machine mounted twice appends the same class at `hotel.holding` and at `flights.holding`.
- A compound start at two levels is one event that lists both in `entered`.
- An eventless target needs only a generated class, which never carries domain data.

### Checkpoints are event classes

```ruby
class PaymentMachine < Sourced::Machine
  partition_by :order_id, :payment_id

  Pending  = Checkpoint(:pending)    # PaymentMachine::Pending, type 'payment_machine.runtime.pending'
  Captured = Checkpoint(:captured)
  Declined = Checkpoint(:declined)

  start Pending do
    command Pay => Captured | Declined do |_payment, cmd|
      to(cmd.payload.ok ? Captured : Declined)
    end
  end

  final Captured
  final Declined
end
```

- **`Machine.Checkpoint(name)`** generates a subclass of `Sourced::Machine::Checkpoint`, an abstract `Sourced::Event`, records it on the machine and returns it. The class is the handle: the tree places it, `to` targets it, and `machine.value` / `in?` return and take it. It's also the event this machine appends when it runs as a top machine, whether standalone or as a child. The `Checkpoint` / `Targets` `Data` values go away. `Targets` stays as the union built by `|`.
- **The payload is the machine's partition keys, as typed attributes, plus the transition attributes** (`trigger`, `source`, `target`, `exited`, `entered`). A machine that is only ever mounted has no `partition_by`; its classes have no keys and are only ever used as handles (see below).
- **`partition_by` and `consumer_group` come first.** A machine must declare both (where it has them) before any `Checkpoint` or mount, so their classes get its keys and its type prefix. Anything else raises.
- **The type is `"#{type_prefix}.runtime.#{name}"`.** The `runtime` segment keeps generated types apart from the machine's own data events (`payment_machine.captured` stays free for a domain event). Under it:
  - `runtime.<name>` for the machine's own checkpoints. Names can't contain dots.
  - `runtime.child.*` for `Started` / `Finished` / `Stop`.
  - `runtime.mounted.<prefix>.<name>` for mounted machines.

  These three can't collide. Every generated class also raises if its type is already registered, because the registry silently replaces a duplicate.
- **`type_prefix` comes from `group_id`**, normalised as today (`::` → `.`, underscored). `group_id` defaults to the class name, and `consumer_group 'payments'` fixes it, as a message's type string fixes its storage name apart from its class name. `group_id` is already the machine's stable identity: a new one means a new consumer group without offsets, which reprocesses every partition (and re-runs reactions, since a new group isn't replaying). `reset_consumer_group` is how to replay, so tying types to `group_id` adds no constraint. It covers every generated type: the machine's checkpoints, the classes it generates for mounted machines, and `Started` / `Finished` / `Stop`. A machine that is only ever mounted is never registered as a consumer, but `consumer_group` still fixes its prefix.
- **A `group_id` is needed** when `Checkpoint` is called: a class name, or an explicit `consumer_group` for an anonymous machine.
- **The constant is the user's.** `Captured = Checkpoint(:captured)` names the class through the assignment; `Checkpoint` doesn't `const_set`, which would warn on reassignment. The constant can be renamed freely: storage keeps the type, which comes from the checkpoint's name. Placing by Symbol (`checkpoint :outline`) generates the class and sets the constant (`Outline`), since nothing else names it.
- **`Checkpoint.|(other)`** builds `Targets`, overriding Plumb's `|` on message classes (which builds a `Plumb::Or`). `Captured | Declined | nil` works as now; `nil` still goes last.
- **Checkpoints don't take domain attributes** in this revision. If they do later (`Checkpoint(:paid) { attribute :amount }`), `validate!` must reject them as targets of `done`, `on_done` and `always`, which have no data to build them from (gap 7).

### Mounted machines: the mounting machine's own classes

A mounted machine runs in its top machine's partition. If it appended its own classes there, they would be shared with every other context it runs in:		

- `OrderMachine` and `BillingMachine` both mounting `PaymentMachine` on the same `order_id`;
- `PaymentMachine` standalone or as a child;
- a parent that spawns `PaymentMachine` and also mounts it.

Each machine would then read, and wake for, events whose paths belong to another tree.

So a mounting machine generates its own class for every checkpoint of the machine it mounts:

```ruby
class OrderMachine < Sourced::Machine
  partition_by :order_id

  Placed = Checkpoint(:placed)       # 'order_machine.runtime.placed'

  checkpoint Placed do
    region :payment, PaymentMachine
    # generates, for each of PaymentMachine's checkpoints:
    #   OrderMachine::Mounted::PaymentMachine::Captured
    #   type 'order_machine.runtime.mounted.payment_machine.captured'
    #   payload: order_id + transition attributes
    #   .checkpoint => PaymentMachine::Captured (the handle)
  end
end
```

- **Every context has its own types.** `PaymentMachine` standalone or as a child appends `payment_machine.runtime.*`, inside `OrderMachine` it appends `order_machine.runtime.mounted.payment_machine.*`, and inside `BillingMachine` it appends `billing_machine.runtime.mounted.payment_machine.*`. A machine subscribes only to its own types, so it neither reads nor wakes for another context's events. The store, `extracted_keys` and the read conditions are unchanged.
- **Nothing is mutated.** The generated classes belong to the mounting machine. The mounted machine's classes stay as they were defined.
- **Typed keys.** A generated class has the mounting machine's partition keys, whatever the mounted machine's own `partition_by` is.
- **Handles stay the mounted machine's classes.** Handlers, `to`, `machine.value` and `in?` use `PaymentMachine::Captured`. Each generated class points back to it with `.checkpoint`, and the runtime maps events to handles that way.
  - A generated class can't subclass its handle: a mounted `ThingMachine` that has its own `partition_by` would pass on keys (`thing_id`) that `OrderMachine`'s events don't carry. Both subclass `Sourced::Machine::Checkpoint`.
  - `PaymentMachine::Captured === evt` is false for a mounted event. Code that needs the handle uses `evt.class.checkpoint`, which a machine's own classes answer with `self`.
- **Generated when the mount is declared** (`region :x, M`, or `checkpoint X, M` for a compound mount), so they're registered before the codec compiles. The mounted machine is complete by then, since it's referenced as a constant.
  - Generation covers the mounted machine's tree recursively, one class per (mounting machine, handle). `Marketplace` mounting `OrderMachine` generates classes for `OrderMachine`'s own checkpoints and for the `PaymentMachine` inside it.
  - A machine without `partition_by` generates nothing: it can't be a top machine, so whichever machine mounts it generates for its tree.
  - `OrderMachine`'s classes for `PaymentMachine` go unused when `OrderMachine` itself is mounted in `Marketplace`. That costs some registered types and nothing else.
- **The type uses the mounted machine's prefix, not the mount's name.** A machine mounted twice (`hotel`, `flights`) shares one class per checkpoint, and the paths tell the placements apart. Renaming a region changes no type.
- **Lookup:** `OrderMachine.checkpoint_class(PaymentMachine::Captured)` returns the class `OrderMachine` appends for a handle: the handle itself for its own checkpoints, the generated class for a mounted machine's.

### Transitions

- **One event per transition**, of the top machine's class for the target. Entering the compound checkpoint `Planning` appends one `TourBooking::Planning` whose `entered` is `[planning, planning.itinerary, planning.itinerary.outline]`. A transition inside a mounted `PaymentMachine` appends `OrderMachine::Mounted::PaymentMachine::Captured`. A `stop` appends the class of the `on_stop` final.
- **Handlers still choose with `to`, not `event`.** `to` records the target. After the block, the runtime applies the default (single target, or stay on `nil`), checks the target is declared, computes the payload and appends the event after the block's data events. `event(Captured)` keeps raising, pointing to `to`: `event` appends data events at the point it's called, with their own payload, so a checkpoint there would look like an append while acting like `to`.
- **Replay is unchanged.** The runtime applies `entered` / `exited` from the events of its own checkpoint classes. `__transition_classes` becomes the machine's own classes, the generated ones and its own `Started`, fixed at `validate!`.
- **Entry/exit actions and timers** run on claiming a checkpoint event, as they do for `Transitioned` now. `entered_by` is that event's id.
- **`Machine.transition_classes`** (public) lists them, so a projection can subscribe to every move with `evolve(*OrderMachine.transition_classes)`. Store subscriptions match type strings, not class hierarchies, so there's no single type to subscribe to. A subscriber that wants a mounted machine's checkpoint in every context lists each mounting machine's class; normally it only cares about one machine's partition.

### Children keep `Started` / `Finished`

- **`Started`** is appended by `spawn` / `invoke` and carries `input`. **`Finished`** is appended on entering a root final of the top machine's tree. One `Finished` covers every final, so a child with several finals is still one subscription.
- **The types move under `runtime.child`:** `payment_machine.runtime.child.started`, `…runtime.child.finished`, and the `Stop` command, `…runtime.child.stop`.
- **A parent can spawn and mount the same machine.** The mounted copy appends the parent's generated classes, never the child's.
- **Dropping them is a separate decision.** With mounting machines generating their own classes, a parent could subscribe to the child's own start and final classes without confusing them with a mounted copy's. `input` would move onto the child's start class (added by `start`, on the machine's own class), and the final's path is already in `target`. Revisit once R3 lands.
- An invoked child's `on_done` transition keeps `trigger: <Child>::Finished.type`.

### Renames

- **Class constants** can be renamed freely, since the type is stored, not the constant. That includes the machine's own class, once `consumer_group` fixes its prefix.
- **Renaming a checkpoint** changes its type, the types of the classes generated for it, and its paths, so it isn't supported: keep the name, and rename the constant instead.[^renames]
- **Renaming a machine class**, mounted or not, changes no type when it declares `consumer_group`. Without one, its `group_id` and every generated type change with the class name, as its consumer group already does: declare `consumer_group` with the old name first.

### What changes elsewhere

- **Topology:** a command leads to its target checkpoints' types. This closes phase 9's missing "target state names".
- **`to_h` / Mermaid:** unchanged. Ids are paths.
- **GWT helpers and specs:** `moved` builds `machine.checkpoint_class(target)`.
- **Codec:** checkpoint classes are generated in the class body, at `Checkpoint` and at mounts. That's earlier than `Transitioned` was (`partition_by` / `validate!`), so they're always registered before the codec compiles.

### Phases

| # | Contents |
|---|---|
| R1 | **Checkpoint classes.** `Sourced::Machine::Checkpoint`, `Machine.Checkpoint(name)` (type under `runtime` with `type_prefix` from `group_id`, clash check, no dots, `partition_by` and `consumer_group` first), `\|` to `Targets`, Symbol placement setting the constant. The tree, targets, `value` / `in?` and validation take classes; the `Data` handles go. |
| R2 | **Checkpoint events and replay.** Mounting generates classes (recursive, one per (mounting machine, handle), `.checkpoint`, `checkpoint_class`). Transitions append `checkpoint_class(target)`; replay maps events to handles; `Transitioned` removed; `transition_classes`; the `moved` helper. Store-backed specs: two machines mounting one machine on the same partition values, a machine standalone and mounted, a parent spawning and mounting one machine. Each sees only its own events. |
| R3 | **Children.** `Started` / `Finished` / `Stop` under `runtime.child`. |
| R4 | **Rendering.** Topology targets. |

R1 and R2 replace the event model together; the suite is red between them. R3 and R4 follow R2.

## Revision: declared children

Today a parent declares a child with `spawns Child, as:, stop_on:` and starts it imperatively, with `spawn :as, **payload` inside a handler block. So the definition says what children a machine can have, but not when they're started: Topology finds out by looking for `spawn` calls in the block's source (Prism, `SourceAnalyzer#calls_in_block`). Invoked children are declarative already, but under another verb and with their own `as:` / `keys:` / `on_done:` kwargs.

This revision makes starting and stopping children part of the definition, with one verb pair that matches the messages each one appends (`Started`, `Stop`). The rule: **a child lives as long as the scope that started it.** A handler is a moment, so a child it starts is independent until something stops it; a checkpoint lasts, so a child it starts is stopped when the checkpoint is left.

```ruby
class LineItemMachine < Sourced::Machine
  partition_by :order_id, :line_id

  started Reserving do                       # was: start Reserving
    command Reserve => Reserved | OutOfStock do |_line, cmd|
      to(cmd.payload.available ? Reserved : OutOfStock)
    end
  end

  checkpoint Reserved do
    command Pick => Picked
  end

  final Picked
  final OutOfStock

  stopped Cancelled do                       # was: on_stop :cancelled
    entry { |line| dispatch ReleaseStock, order_id: line.order_id, line_id: line.line_id }
  end
end

class OrderMachine < Sourced::Machine
  partition_by :order_id

  started Open do
    command AddLine, starts: LineItemMachine          # one child, keys from the payload

    command Checkout => AwaitingPayment, starts: ParcelMachine | nil do |order, cmd|
      cmd.payload.parcels.each { |p| start ParcelMachine, parcel_id: p.id }
    end

    command Abandon => Cancelled
  end

  checkpoint AwaitingPayment do
    start PaymentMachine do                           # was: invoke
      payload { |order, _checkpoint| { payment_id: order.payment_id, amount: order.total } }
      on_done PaymentMachine::Captured => Paid
      on_done PaymentMachine::Declined => PaymentFailed
    end
  end

  checkpoint Paid do
    children_done ParcelMachine => Shipped
  end

  final Cancelled do
    stops LineItemMachine
  end
end
```

### Renames

| Was | Now | Scope |
|---|---|---|
| `start X` / `start X, M, on_done:` | `started X` / `started X, M, on_done:` | class body: the machine's start checkpoint |
| `on_stop X` | `stopped X` | class body: where `Stop` moves the machine |
| `invoke Child, as:, keys:, on_done:` | `start Child do payload { … }; on_done … end` | checkpoint block |
| `spawns Child, as:, stop_on:` | `starts: Child` on handlers, `stops Child` on checkpoints | — |
| `spawn :as, **payload` | `start Child, **payload` | handler block (instance) |
| `children(:as)`, `children_done :as => A` | `children(Child)`, `children_done Child => A` | — |

`started` and `stopped` name checkpoints, as `initial` / `final` do, and match the messages that enter them: `Started` enters the `started` checkpoint, `Stop` moves the machine to the `stopped` one. The constants `Started`, `Finished` and `Stop` stay reserved, so `started :started` still leaves the handle anonymous.

### `started` and `stopped`

- **`started`** is today's class-level `start`, renamed: placement, compound starts (`started X, M, on_done:`) and the rules are unchanged.
- **`stopped`** is `on_stop`, renamed, and it now places the checkpoint and takes its block, as `started` does. `on_stop` takes only a handle and places a bare final, so an entry action meant declaring the final first. Both forms work:

  ```ruby
  stopped Cancelled do                       # places a root final with this block
    entry { … }
  end

  final Cancelled do … end
  stopped Cancelled                          # marks an already-placed root final; raises if it isn't one
  ```

- Otherwise as `on_stop`: it generates `Stop` (keyed by the machine's partition keys, accepted from any started, non-final position, moving to the checkpoint, running exit actions), `Started` and `Finished`; it must be a final of the root region; it needs a name and comes after `partition_by`. A machine is a child when it declares `started` and `stopped`.

### `starts:` on handlers

- **`starts:` declares which children a handler may start.** It sits alongside the transition, and is an option on any handler: `command Cmd, starts: X` (data-only), `command Cmd => A, starts: X`, `on Event => A, starts: X`, `children_done X => A, starts: Y`.
- **`Machine.|` builds a union of machines**, as `Checkpoint.|` builds `Targets`: `starts: A | B`, `starts: A | nil`. It's its own `Data` type (`Machine::Starts`), not `Targets`, so `Shipping | ParcelMachine` raises a `DefinitionError` naming both operands, as does any other non-machine member. As with `Targets`, `nil` goes last: `nil | A` is `NilClass#|` and returns `true`.
- **What a handler must start mirrors what a transition must do.** `=> X | Y` moves to exactly one of them; `starts: A | B` starts at least one of them; `| nil` makes either optional:

  | Declaration | With a block | Without a block |
  |---|---|---|
  | `starts: A` | at least one `A` | exactly one `A`, keys from the payload |
  | `starts: A \| B` | at least one of them, any number | `DefinitionError` (which one?) |
  | `starts: A \| nil` | zero or more `A` | `DefinitionError` (nothing to decide) |

  The runtime counts a handler's starts after its block runs, as `run_handler` checks the target, and raises when the count doesn't fit (`TransitionError`, or a new `StartError`: decide in C1).
- **Without a block,** the child's keys are the command's (or event's) attributes named by the child's partition keys, and its `Started`'s `input` is the rest of the payload, without either machine's partition keys. `validate!` checks the message declares every partition key of the child that the parent doesn't have.

### `start` in handlers

- **The instance-level `start Child, **payload` replaces `spawn`.** Class-level `started` declares where a machine starts; instance-level `start` makes another machine start there.
- `start` raises unless the running handler declares `Child` in its `starts:`. Like `to`, it's only for handlers.
- It appends `Child::Started` as `spawn` does now (`start_child`): the child's keys from the payload, the parent's filled in, the rest as `input`.

### `start` in checkpoints (was `invoke`)

```ruby
checkpoint AwaitingPayment do
  start PaymentMachine do
    payload { |order, checkpoint| { payment_id: order.payment_id, amount: order.total } }
    on_done PaymentMachine::Captured => Paid
    on_done PaymentMachine::Declined => PaymentFailed
  end
end
```

- **Same semantics as `invoke`:** one child per entry of the checkpoint (matched by the entering event's id, `entry_id`), started as an eventless step after the decision that enters it, stopped when the checkpoint is left, and a mapped final moving the parent with the child's `Finished` type as `trigger`.
- **The block is required, and so is `payload`.** There's no default: the entering checkpoint event carries only the parent's keys and the transition's paths, so nothing in it can supply the child's own keys, and deriving them (from the decision's message, or the event's id) would be magic. `validate!` raises for a `start` with no block or no `payload`.
- **`payload { |context, checkpoint| … }`** returns the child's `Started` payload: its keys (the parent's are filled in) and anything else as `input`, as for `start` in handlers. `checkpoint` is the event that entered the checkpoint, for its `id` or `created_at`. The block is called once, when the child is started, and its result is recorded in `Started`, so a non-deterministic value (`SecureRandom.uuid`) is safe.
- **`on_done Final => Target`**, one per line, maps a child's root final to a target of this checkpoint, as `invoke`'s `on_done:` hash does. A child with no `on_done` runs until it finishes or the checkpoint is left.
- **No guard.** To start a child only sometimes, route through a transient checkpoint (`always AwaitingPayment, if: :payment_due?` then `always Paid`), so the condition is a state the chart shows and `in?` can query, as XState does with `invoke`. A guard on `start` would have to be re-evaluated after every decision, as guarded `always` is, which can start a child partway through its checkpoint.

### Children keyed by machine class

- **`as:` goes**, from spawned and invoked children alike. It was never stored (a `Started` doesn't carry it): it named the runtime's tracking, `children(:as)`, `children_done :as => …`, and ids in `to_h`, Mermaid and Topology. A machine can be a child only once already (`__define_child`), because a parent tracks children by the child's `Started` / `Finished` classes and couldn't tell two groups apart. An invoked child is matched to its checkpoint by `entry_id`, not by name. So the machine class is the key:
  - `machine.children(LineItemMachine)`, `machine.children` for all;
  - `children_done LineItemMachine => LinesDone`;
  - `to_h`, Mermaid and Topology use the child's `group_id` where they used `as`.
- **The machine-level `spawns` goes.** `validate!` builds the `ChildSpec`s from the tree: handlers' `starts:` (each machine once, whatever number of handlers start it), with `stop_on` taken from the `stops` that name it, and checkpoint-level `start`s. Mounted machines' declarations contribute to the top machine's, as `spawns` / `invoke` in a mounted machine do now, and a machine that starts children can still be mounted only once.
- **One machine, one role:** a machine started by handlers can't also be started by a checkpoint, and a checkpoint-level `start` of a machine appears once in the tree. Same constraint as today.
- If one machine is ever wanted in two roles, `as:` comes back as an option, stored in `Started`'s payload so tracking can tell the groups apart.

### `stops` on checkpoints

- **`stops Child` in a checkpoint's block** stops every running child of that machine when the checkpoint is entered, dispatching `Child::Stop` per child, as `stop_on` does now.
- It belongs to the checkpoint's node, so in a machine mounted twice it applies only where it's placed.
- `validate!` raises unless some handler in the top machine's tree declares `starts: Child`. Children started by a checkpoint are stopped by leaving it, not by `stops`.

### What changes elsewhere

- **Topology:** a handler produces its `starts:` machines' `Started` types from the declaration; the Prism lookup for `spawn` goes. Stop automations come from `stops`; the invoke automations are renamed but unchanged.
- **`to_h`:** a handler's `on` entry lists the machines it starts; children are listed by `group_id`; a checkpoint lists what it `stops`. Invoked children stay in XState's `invoke` (that's its name there), with `id` the child's `group_id`.
- **Mermaid:** labels a handler's edge with what it starts, e.g. `AddLine / starts LineItemMachine`.
- **README, CLAUDE.md, specs and the holiday booking example** move to the new names.

### Phases

| # | Contents |
|---|---|
| C1 | **Renames.** `start` → `started`, `on_stop` → `stopped` (placing the checkpoint, with a block). Specs ported. |
| C2 | **Handler children.** `Machine.\|` and `Machine::Starts`; `starts:` on command, `on` and `children_done` handlers; instance `start` with the count check; auto-start for blockless handlers; `stops`; `ChildSpec`s built in `validate!`, keyed by machine class; `children(Child)`, `children_done Child => …`; `spawns`, `spawn` and `as:` removed. Specs ported. |
| C3 | **Checkpoint children.** Checkpoint-level `start Child do payload { … }; on_done … end` replaces `invoke`. Specs ported. |
| C4 | **Rendering.** Topology, `to_h` and Mermaid read `starts:` / `stops` / checkpoint `start`; `SourceAnalyzer`'s `spawn` lookup removed. |
| C5 | **Docs.** README and CLAUDE.md. |

C1 comes first so the class-level `start` is free before C3 gives `start` a meaning in checkpoint blocks.

## Progress

- **Phases 1–4, 7 and 9: done** (handles and the tree, `Transitioned` and replay, transition semantics, actions and timers, children with `Started` / `Finished`, `to_h` / Mermaid / Topology). All machine specs ported; the suite is green. Differences from the plan:
  - **Descendant targets from a parallel checkpoint** exit and re-enter only the region the target is in. SCXML would re-enter the whole parallel checkpoint (its domain is the nearest *compound* ancestor). The domain here is always a region: the innermost one holding source and target, or the source's own region that holds the target.
  - **Timers** carry the entered checkpoint's path (`metadata[:entered_at]`) as well as the transition's id: one transition enters several checkpoints, and an inner one can be left and re-entered while the outer ones stay.
  - **Another machine's checkpoint is a valid target only below the declaring checkpoint** (a compound's command targeting its mounted machine's sub-state). A mounted machine still reaches its parent only through `on_done`.
  - **A `done` can't target a checkpoint inside its own regions.**
  - **`Tree#node_at`** resolves initial positions too (`placed.payment.initial`), since they're a transition's `source`.
  - **Store-backed machines must be defined before the store's codec compiles**, like any generated message type (`Stop` already was): `Transitioned` is generated at `partition_by`.
  - **A compound checkpoint's mount is named after the checkpoint**, not its machine, so one machine can be several compound checkpoints (`entered_by(:thing)`).
  - **An invoked child's `on_done` transition** has the child's `Finished` type as its `trigger`.
  - **`Transitioned` has no `configuration`** (see Open): replay rebuilds it from `entered` / `exited`.
  - **`validate!` fixes what the runtime reads per message** (`handled_commands`, `handled_messages_for_evolve`, start paths, `Started` / `Finished`) and validates children.
- **Revision R1 and R2: done** (checkpoint classes; mounting machines' generated classes; transitions append the target's class; `Transitioned` removed). Isolation specs in `spec/machine/checkpoint_classes_spec.rb`. Differences from the revision:
  - **Generated classes are named `<Machine>::Mounted::<mounted machine's full name>::<handle's constant>`**, matching the type's `runtime.mounted` segment. `Runtime` would have shadowed `Sourced::Machine::Runtime` inside the machine's body.
  - **A type can be taken over by a class for a machine with the same `group_id`** (a reloaded machine, or a spec's throwaway machine whose name repeats). Any other clash raises.
  - **Handles print as their class name** in errors (`OrderMachine::Placed`). An anonymous machine's handles print as `Machine:name`.
  - **`Started` / `Finished` / `Stop`** keep their types until R3, with `type_prefix` already from `group_id`.
  - **Topology** already maps commands and `done` / `stop_on` automations to checkpoint types, since `Transitioned` is gone. R4 is what's left of it (invoke and stop automations).
- **Revision R3: done.** `Started` / `Finished` / `Stop` are `<prefix>.runtime.child.started` / `.finished` / `.stop`. The constants `Started`, `Finished` and `Stop` are reserved for them (`Machine::CHILD_MESSAGES`): placing `:started`, `:finished` or `:stop` by Symbol leaves the handle anonymous, and `on_stop` raises if one of those constants is already defined.
- **Revision R4: done.** Topology maps each command to its targets' checkpoint types. Automations that run on leaving a checkpoint consume the types of the transitions that can leave it, by the runtime's rule (below the innermost region holding source and target, on the source's side). Those automations are exit actions, now their own `exit(<path>)` automation apart from `actions(<path>)`'s entry and `after` actions, and stopping an invoked child. Topology now calls `validate!` on each machine, as `to_h` does, so it reads one tree.
- **Phase 5: done** (`always` with guards, transient checkpoints, guarded `done`). Specs in `spec/machine/always_spec.rb`. Differences from the plan:
  - **A guarded `always` is re-evaluated after every decision** while its checkpoint is active, including a data-only command's. That's XState's rule (eventless transitions are checked after each microstep), so a non-transient checkpoint moves on once a command's data events make its guard pass.
  - **A transient checkpoint can't accept commands, have timers, invoke children, have regions, or join with `done` / `children_done`.** The machine leaves it in the same decision that enters it, so none of these would ever run. Entry and exit actions are allowed.
  - **Step order:** `always`, then mounted machines' `on_done`, `done` joins, `children_done`, then invoking children. A transient checkpoint resolves before anything that depends on where its region ends up, so a region whose last step is transient completes its join in the same decision.
  - **A `done` with no passing guard stays.** It's tried again after later decisions while the regions stay final.
  - **Unreachable declarations raise:** any `always` or `done` after an unguarded one. `always` on an initial position or a final checkpoint raises too.
  - **`to_h`:** nodes have `always: [{ target:, guard: }]`, with `guard` only for guarded ones: a Symbol's name, or `'guard'` for a lambda. `on_done` stays a target id for a single unguarded `done`, and is a list of the same shape when there are guards (XState's `onDone` takes both).
  - **Mermaid** labels these edges `always` / `always [guard]` and `done` / `done [guard]`.
  - **Topology:** a command or automation produces the checkpoints its targets' `always` can reach, transitively (through compound starts, each once). `always` and `done` count as ways out of a checkpoint for exit automations.
  - **Mounted and invoked machines' `on_done`** stay unguarded. Guarding them is out of scope.
- **Phase 6: done** (`on Event => Target`). Specs in `spec/machine/external_events_spec.rb`. Differences from the plan, and choices it didn't make:
  - **Only claiming an event an `on` declares decides.** Other evolved events don't, so a guard that depends on another reactor's facts waits for the machine's next decision.
  - **Decided like a command, against the latest state.** The claim evolves the partition's full history, including the event and anything after it, as for commands. The deepest active checkpoints declaring it each take it, in definition order. A handler whose checkpoint an earlier one left is skipped. Eventless steps follow in the same decision, and any `reaction` to the same event still runs after it.
  - **Gated like reactions** (`should_react?`, by default `!replaying`), so replay never decides it again.
  - **An initial position can take events,** like initial commands, so an event can start a machine (`initial { on PaymentCaptured => Receiving }`). Final and transient checkpoints can't. Declaring the same event twice on one checkpoint raises.
  - **Rejected events:** commands, checkpoint classes, and children's `Started` / `Finished`, which now subclass `Machine::ChildEvent`. Children go through `on_done` / `children_done`. The event must declare the top machine's partition keys, which a mounted machine's `on` is checked against too.
  - **Claimed and evolved:** `on` events join `handled_messages` and `handled_messages_for_evolve` (evolved once, even when the projection also evolves them).
  - **Rendering:**
    - `to_h`'s `on` lists commands and events by type, each with a new `kind: 'command' | 'event'`.
    - Mermaid labels event edges `on <Event>`.
    - Topology adds an `on(<path>, <Event>)` automation that consumes the event and produces its targets (through `always`). It's added for initial positions too, and `on` transitions count as ways out of a checkpoint for exit automations.
- **Phase 8: done** (history). Specs in `spec/machine/history_spec.rb`. Differences from the plan, and choices it didn't make:
  - **A `done` can't target the history of its own checkpoint or an ancestor's.** It would restore the final regions that completed the join and join again, looping until `MAX_JOINS`.
  - **`Checkpoint.history` / `.history(deep: true)` return a `Machine::History` value** (`checkpoint`, `deep`). It goes wherever a checkpoint target does: blockless arrows, unions (`Editing.history | Archived`), `to`, `always`, `done`, `on`, and mounted and invoked machines' `on_done`. `Tree#resolve` resolves it to its checkpoint, and raises at validation for a checkpoint without regions.
  - **What's remembered is per region, not per checkpoint:** when a checkpoint is left, each of its regions records where it was (`Runtime#apply`, from the event's `exited` and the positions before the event is applied). A region left at its initial position forgets. History lasts until overwritten: entering the checkpoint by default entry doesn't clear it.
  - **The transition's own exits count:** a history transition that leaves the checkpoint (a self-transition, or one from inside it) restores the configuration it's leaving, as SCXML records history on exit before entering.
  - **The event** is the checkpoint's class, `target` its path, `entered` the checkpoint then the restored paths, outermost first. Replay reads `entered` and never recomputes history.
  - **Shallow** restores each region's last checkpoint, then that checkpoint's default entry (compound starts; inline regions start at their initial position). **Deep** restores every region below, recursively. A region without history gets its default entry.
  - **Rendering:** `to_h` adds XState history nodes (`type: 'history'`, `history: 'shallow' | 'deep'`, keys `$history` / `$deep_history`, ids `<path>.$history`) to a checkpoint's `states`, only when something targets them; targets point at them. Mermaid draws the edge to the checkpoint, labelled with `(H)` / `(H*)`. Topology produces the checkpoint's class.
- **Phase 10: done.** A stored path that names no checkpoint raises `Machine::UnresolvablePathError`, naming the path and the stored message's type and id: on replay (`entered`, and `exited` for history), on running a claimed event's entry and exit actions, and for a timer's `entered_at`. A timer whose path no longer resolves used to be treated as stale and refused; it now raises too. Mapping old names stays out (see the footnote).
- **Revision C1: done.** Class-level `start` is `started`, `on_stop` is `stopped`, which places a root final with its block or marks one already placed (a block on an already-placed handle raises: placed twice). The old names are removed.
- **Revision C2: done.** `Machine.|` / `Machine::Starts`, `starts:` on `command`, `on` and `children_done`, instance `start` with the count check, blockless auto-start, `stops`, children keyed by machine class; `spawns`, `spawn` and `as:` removed (from `invoke` too). Choices the revision left open:
  - **`StartError`** is a sibling of `TransitionError` (both `Machine::Error`), raised for a count that doesn't fit and for starting a machine `starts:` doesn't declare. `start` outside a handler raises `Machine::Error`, as `to` does.
  - **Specs:** each machine records a `ChildSpec` per child as its handlers declare `starts:` (or `invoke`), so one role per machine is checked at declaration; the `Tree` merges mounted machines' (several machines in a tree may start the same child; an invoked one is invoked in one place only) and fills a started child's `stop_on` with the placed checkpoints whose `stops` name it. Entering a placed checkpoint stops what its own `stops` name, so it's per placement.
  - **A blockless `children_done … starts: A`** has no message: `A` may have no keys beyond the parent's (checked by `validate!`, with the other auto-start checks).
  - **Rendering** uses `group_id` where `as` was (`to_h` children and `children_done` entries are keyed `id:`; a `children_done` transition's `trigger` is `children_done <group_id>`). A started child's `kind` stays `'spawn'` in `to_h` until C4.
- **Open:**
  - A handler in a machine mounted twice can't tell which placement it runs in, so both copies decide alike on the same command and state. Expose the placement (its path) to handlers?
  - Phase 11 (docs, examples).

[^renames]: **Not built yet: mapping old names.** A later plan could add `Checkpoint(:settled, was: :paid)` and `region :x, was: :y`, which would keep the old checkpoint's type for the new class (and the classes generated for it) and map stored paths when evolving. It could also add a machine-level `migrate_path('a.b' => 'a.c')` for removed states. Until then, checkpoint and region names that stored histories refer to are fixed.
