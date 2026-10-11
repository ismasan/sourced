```ruby
# Domain state: any object. Evolve handlers mutate it in place.
class OrderState
  attr_reader :order_id, :card, :packed
  attr_accessor :total

  def initialize(order_id)
    @order_id = order_id
    @total = 0
    @card = nil
    @packed = []
  end

  def add_card(card) = @card = card
  def pack(sku) = @packed << sku
  def all_packed?(count) = @packed.size == count
end

# ONE reactor (one consumer group) partitioned by :order_id. Regions are orthogonal (active together),
# but every message for an order is decided one at a time, run-to-completion, under one guard.
class OrderMachine < Sourced::Machine
  partition_by :order_id

  # One state object per machine instance, rebuilt from the log for every decision.
  # The block gets the partition values, like Decider.state.
  state { |keys| OrderState.new(keys[:order_id]) }

  # State changes only through events. Evolve is machine-level: there's one state, not one per region.
  evolve(OrderPlaced)      { |order, evt| order.total = evt.payload.total }   # a checkpoint can carry data too
  evolve(PaymentInfoAdded) { |order, evt| order.add_card(evt.payload.card) }  # data event
  evolve(ItemPacked)       { |order, evt| order.pack(evt.payload.sku) }       # data event

  initial do
    command PlaceOrder => OrderPlaced           # no block: appends OrderPlaced from the command's payload
  end

  checkpoint OrderPlaced do                     # having regions makes it parallel
    command CancelOrder => OrderCancelled do |order, cmd|
      # `machine` is the read-only runtime state: configuration, children, entering events.
      raise AlreadyShipped if machine.in?(:shipping, ShipmentDispatched)
    end
    # OrderCancelled leaves OrderPlaced, so both regions stop being active in the same evolve.

    region :payment do
      initial do
        # Union: the handler must emit exactly one of the declared checkpoints.
        command StartPayment => PaymentStarted | PaymentRejected do |order, cmd|
          if order.total.positive?
            event PaymentStarted
            event PaymentInfoAdded, card: cmd.payload.card   # data events can follow the checkpoint
          else
            event PaymentRejected, reason: 'zero total'
          end
        end
      end

      checkpoint PaymentStarted do
        # entry / after are reactions: they run after the decision commits, and only dispatch.
        entry { |order| dispatch ChargeCard, card: order.card }   # order_id filled in
        after(3600) { |order| dispatch ExpirePayment }            # no-op if PaymentStarted was left

        command AddPaymentInfo do |order, cmd|  # no arrow = `=> nil`: data only, state unchanged
          event PaymentInfoAdded, card: cmd.payload.card
        end
        command RetryPayment => PaymentStarted  # re-enters PaymentStarted: entry and after run again
        command ExpirePayment => OrderCancelled # ancestor's checkpoint: leaves OrderPlaced, exiting both regions
      end

      final PaymentCaptured                     # external fact, appended by a payments reactor
      checkpoint PaymentRejected do
        entry { |order| dispatch CancelOrder }
      end
    end

    region :shipping do
      initial do
        # `| nil`: a checkpoint is optional. Without one, the region stays where it is.
        command PackItem => ItemsPacked | nil do |order, cmd|
          event ItemPacked, sku: cmd.payload.sku   # evolved immediately, so order.packed includes it
          event ItemsPacked if order.all_packed?(cmd.payload.item_count)
        end
      end

      checkpoint ItemsPacked do
        command DispatchShipment => ShipmentDispatched
      end

      final ShipmentDispatched
    end

    # Join. When a decision makes the last region final, OrderCompleted is appended in that
    # same decision. When the last region goes final through an external fact (PaymentCaptured),
    # the machine reacts to it and dispatches a generated command that appends OrderCompleted.
    done OrderCompleted
  end

  final OrderCompleted
  final OrderCancelled
end

snapshot = OrderMachine.load(order_id: 'o1')
snapshot.value     # => { OrderPlaced => { payment: PaymentStarted, shipping: :initial } }
snapshot.state     # => #<OrderState total=100 card="tok_…" packed=[]>
```

## Command forms

| Form | Block | Checkpoint |
|---|---|---|
| `command Cmd => Evt` | none | `Evt` is appended, built from whichever of the command's attributes it declares |
| `command Cmd => Evt do … end` | data events, guards (raise to reject) | `Evt` is appended after the block's events |
| `command Cmd => A \| B do … end` | required | the block emits exactly one of `A`, `B` |
| `command Cmd => A \| nil do … end` | required | the block emits `A`, or nothing (stay) |
| `command Cmd do … end` | required | sugar for `=> nil`: data only, no checkpoint |

- **One checkpoint per command.** After the handler runs, the runtime checks the decision emitted exactly one checkpoint from the declared set, or none if the set includes `nil`. Anything else is an error. It compares each emitted event's class against the union's flattened `children`. It doesn't `parse`: `(A | B).parse(b_event)` returns an `A`, because the first branch coerces the other event's payload.
- **Write `A | nil`, never `nil | A`.** `NilClass#|` is boolean or, so `nil | A` is `true`.
- **Auto-appended checkpoints take what they can from the command.** A checkpoint's job is to advance the state, so it needn't mirror the command: it's built with `Evt.new(payload: cmd.payload.to_h)`, which keeps the attributes `Evt` declares and drops the rest. Beyond the partition keys it may carry any of the command's attributes, for data collection. The same applies after a block runs, when the block didn't emit the checkpoint itself.
- **Checkpoints must declare the partition keys**, checked at definition time (as for every event the machine evolves, or it wouldn't be in the partition read).
- **Blockless checkpoints must be buildable from the command, checked at definition time.** Every checkpoint attribute that isn't a partition key and that the command doesn't declare must be allowed to be missing: `key.optional? || type.resolve(Plumb::Undefined).valid?` over `Evt::Payload._schema._schema` (so declared with `attribute?`, or with a default; `nullable` alone isn't enough). A checkpoint auto-appended after a block isn't checked at definition time, since the block may emit it itself with attributes the command lacks; the runtime check below covers it.
- **What stays a runtime check:** values that don't fit (the command's `Integer` into the checkpoint's `String`), and attributes the command declares with `attribute?` but leaves out of a particular instance. The runtime validates the checkpoint before appending it.
- **Targets are checkpoints of the command's own region or an ancestor region**, never a sibling region. An ancestor target exits every region below it, as `ExpirePayment => OrderCancelled` does. Checked at definition time.
- **A command accepted by several active regions** runs each region's handler in definition order, and each sees the events of the ones before it. The one-checkpoint rule applies per handler. A handler whose region has already been exited by an earlier one in the same decision doesn't run.
- **Partition keys are filled in** on every event the machine emits and every message it dispatches (when the message declares them), so `event PaymentStarted` and `dispatch CancelOrder` need no `order_id`.
- **The transition graph is static.** Every edge (state, command, possible checkpoints) is known when the class loads. That allows checking at definition time that each target is a declared checkpoint, finding unreachable states and states that can't be left, and drawing the machine without running it.
- **Order within a decision doesn't matter.** The state is read after the whole decision is applied, and entry actions run afterwards. So an auto-appended checkpoint landing after the data events behaves the same as an explicit one emitted first.

## Domain state and runtime state

The generated Decider holds two things: the machine's runtime state, and the user's domain state.

In code, `state` means the domain object, as in Deciders and Projectors, and `checkpoint` declares a statechart state. In prose, statechart terms ("initial state", "the invoking state") refer to checkpoints.

- **Domain state: any object.** `state { |keys| … }` builds it from the partition values, and `evolve` handlers mutate it in place, as in a Decider; their return value is ignored. Every handler receives it as its first argument: commands, guards, `entry` / `exit`, `after`, `children_done`.
- **Runtime state: owned by the machine.** It holds:
  - the active checkpoint in each region, with the event that entered it (for timers and history scoping);
  - each region's history start position;
  - the children registry.

  User code reads it through `machine`, available in every handler, and can't write to it:
  - `machine.value`: the configuration;
  - `machine.in?(PaymentStarted)` / `machine.in?(:payment, PaymentStarted)`: statechart-style matching;
  - `machine.children(:lines)`: `ChildRef`s with each child's keys, latest checkpoint and `final?`;
  - `machine.entered_by`: the event that entered the current state.
- **Checkpoints can have `evolve` handlers.** The runtime applies the checkpoint to its own state first, then runs the user's `evolve`, so the handler already sees the new configuration through `machine`.
- **`load` returns both.** `snapshot.value` and `snapshot.children` come from the runtime state, `snapshot.state` is the domain object.

## Semantics

- **One machine, one partition, one Decider.** The configuration (`{ OrderPlaced => { payment: PaymentStarted, shipping: :initial } }`) and the domain state are evolved from all of the machine's events. Workers claim a partition one at a time and decide its messages one by one, so regions never run concurrently and can share the domain state.
- **`handle!` is optimistic, not queued.** Two concurrent `handle!` calls for the same order (say one for payment, one for shipping) decide in parallel. One gets `ConcurrentAppendError` and retries, because the guard covers the whole machine.
- **Checkpoints vs data.** Events declared as `checkpoint` / `final` are checkpoints: a region is at its latest checkpoint. All other evolved events are data and only change the domain state.
- **Eventless transitions belong in the decision.** Joins (`done`), and any `always`-style transition, are compiled into the decision that triggers them, not into reactions. Otherwise the machine would briefly sit in a state XState never shows.
- **A region's history starts at its parent's entering checkpoint.** A region with no checkpoint since then is in its initial state, which has no event of its own ("no payment checkpoint since `OrderPlaced`"). Scoping by the entering checkpoint's position means re-entering `OrderPlaced` doesn't pick up the previous activation's payment checkpoints.
- **Concurrency means child machines.** For real parallel work, invoke or spawn a child machine: a separate reactor with its own partition and state, talking to the parent only through commands and events.

## Child machines

Regions are for modelling and share one partition. A child machine is for parallel work: a separate machine, with its own Decider, consumer group, partition and state.

### Spawning

```ruby
LineState = Struct.new(:sku, :qty, :currency)

# Child: its partition keys include the parent's.
class LineItemMachine < Sourced::Machine
  partition_by :order_id, :line_id

  state { |_keys| LineState.new }
  evolve(LineAdded)   { |line, evt| line.sku = evt.payload.sku; line.qty = evt.payload.qty }
  # Evolved from the parent: OrderPlaced carries only order_id, so it can never wake a line,
  # but it's in every line's partition read, from before the line existed too.
  evolve(OrderPlaced) { |line, evt| line.currency = evt.payload.currency }

  # Spawn event: appended by the parent, and this machine's first checkpoint.
  # No start command: appending it is what spawns the child.
  start LineAdded do
    entry { |line| dispatch ReserveStock, sku: line.sku, qty: line.qty }  # partition keys filled in
    command StockReserved => LineReserved                                # e.g. from an inventory reactor
    command StockUnavailable => LineFailed
  end

  checkpoint LineReserved do
    exit { |line| dispatch ReleaseStock, sku: line.sku, qty: line.qty }  # also runs when stopped
    command PickLine => LinePicked
  end

  final LinePicked
  final LineFailed

  # What this machine becomes when its parent stops it: the outcome of the generated
  # LineItemMachine::Stop command. The parent decides when; the child doesn't know why.
  on_stop LineCancelled
end

class OrderMachine < Sourced::Machine
  partition_by :order_id

  # Declares the child. Its start and final checkpoints are tracked in the runtime state,
  # and live lines are sent Stop when this machine reaches OrderCancelled.
  spawns LineItemMachine, as: :lines, stop_on: OrderCancelled

  checkpoint OrderPlaced do
    command AddLine do |order, cmd|             # spawning is data, not a checkpoint
      spawn :lines, line_id: cmd.payload.line_id, sku: cmd.payload.sku, qty: cmd.payload.qty
    end

    command PlaceAllLines do |order, cmd|       # one command can spawn many children
      cmd.payload.lines.each { |line| spawn :lines, **line }
    end

    # Join over children. Their final events come from another reactor, so this is the
    # external-fact case: a generated reaction plus command appends the checkpoint.
    children_done :lines => LinesDone | LinesPartiallyFailed do |order|
      machine.children(:lines).all? { |ref| ref.state == LinePicked } ? event(LinesDone) : event(LinesPartiallyFailed)
    end

    command CancelOrder => OrderCancelled       # live lines are sent Stop
  end

  checkpoint LinesDone do ... end
  final OrderCancelled
end

OrderMachine.load(order_id: 'o1').children[:lines]
# => [#<ChildRef keys={line_id: 'l1'} state=LinePicked>, #<ChildRef keys={line_id: 'l2'} state=LineAdded>]
```

### Invoking

`invoke` is a child tied to one state: spawned when the state is entered, stopped when it's left, its final checkpoint mapped onto the parent's. A variant of `OrderMachine` that invokes a payment machine instead of modelling payment as a region:

```ruby
PaymentState = Struct.new(:amount, :currency, :attempts)

class PaymentMachine < Sourced::Machine
  partition_by :order_id, :payment_id

  state { |_keys| PaymentState.new(0, nil, 0) }
  # Evolved from the parent: partial-key, so it never wakes a payment, but it's in its reads.
  evolve(OrderPlaced)     { |pay, evt| pay.amount = evt.payload.total; pay.currency = evt.payload.currency }
  evolve(ChargeAttempted) { |pay, _evt| pay.attempts += 1 }

  start PaymentRequested do                     # appended by the parent when it invokes us
    entry { |pay| dispatch ChargeCard, amount: pay.amount, currency: pay.currency }
    command GatewayCaptured => PaymentCaptured
    command GatewayDeclined => PaymentRetrying | PaymentDeclined do |pay, cmd|
      event ChargeAttempted
      pay.attempts < 3 ? event(PaymentRetrying) : event(PaymentDeclined, reason: cmd.payload.reason)
    end
  end

  checkpoint PaymentRetrying do
    after(60) { |pay| dispatch RetryCharge }
    command RetryCharge => PaymentRequested     # re-enters the start state: charges again
  end

  final PaymentCaptured
  final PaymentDeclined

  on_stop PaymentVoided
end

class OrderMachine < Sourced::Machine
  partition_by :order_id

  initial do
    command PlaceOrder => OrderPlaced
  end

  checkpoint OrderPlaced do
    command RequestPayment => AwaitingPayment
  end

  checkpoint AwaitingPayment do
    # Spawned in the same decision that enters AwaitingPayment, so entering the state
    # and starting the payment commit together. Sent Stop when the state is left.
    invoke PaymentMachine, as: :payment,
           start: PaymentRequested,
           keys: ->(_order, _cmd) { { payment_id: SecureRandom.uuid } },  # decided once, then a fact
           on_done: {
             PaymentCaptured => OrderPaid,      # child final checkpoint => our checkpoint
             PaymentDeclined => PaymentFailed
           }

    after(3600) { |_order| dispatch TimeOutPayment }
    command TimeOutPayment => PaymentTimedOut   # leaves the state: the payment is sent Stop
    command CancelOrder => OrderCancelled       # likewise
  end

  checkpoint PaymentFailed do
    command RequestPayment => AwaitingPayment   # re-entering invokes a fresh payment (new payment_id)
    command CancelOrder => OrderCancelled
  end

  checkpoint PaymentTimedOut do
    command RequestPayment => AwaitingPayment
  end

  checkpoint OrderPaid do ... end
  final OrderCancelled
end

OrderMachine.load(order_id: 'o1').children[:payment]
# => [#<ChildRef keys={payment_id: 'p-7f3…'} state=PaymentRetrying>]
```

The happy path:

| Step | Decided by | Appended | Then |
|---|---|---|---|
| `RequestPayment` in `OrderPlaced` | order | `AwaitingPayment(o1)`, `PaymentRequested(o1, p1)` | `PaymentRequested` wakes `(o1, p1)` |
| payment's entry action | payment (reaction) | `ChargeCard` dispatched | it evolved `OrderPlaced(o1)` for the amount |
| `GatewayDeclined` (attempts < 3) | payment | `ChargeAttempted`, `PaymentRetrying(o1, p1)` | 60s timer, then `RetryCharge` |
| `GatewayCaptured` | payment | `PaymentCaptured(o1, p1)` | wakes the order (it carries `order_id`) |
| `on_done` | order (generated reaction + command) | `OrderPaid(o1)` | the invoke has ended |

If the hour runs out first:

| Step | Decided by | Appended | Then |
|---|---|---|---|
| `TimeOutPayment` | order | `PaymentTimedOut(o1)` | leaving `AwaitingPayment` → `Stop(o1, p1)` dispatched |
| `Stop` | payment | `PaymentVoided(o1, p1)` | its exit actions run; a later `GatewayCaptured` for `p1` is rejected |

### Woken vs evolved

A machine's partition keys are its identity, in the actor-model sense. The store already draws the line this needs:

- **Woken: only by messages addressed to it.** A partition has pending work only when a handled message carries *all* of its keys. `LineAdded(o1, l1)` and `Stop(o1, l1)` wake line `l1`; `OrderCancelled(o1)` wakes no line.
- **Evolved: also messages that carry only some of its keys.** Once woken, a partition's reads include messages that carry only some of its keys, including ones from before it was spawned. That's how a line evolves `OrderPlaced(o1)` to learn the currency, without the parent copying it into `LineAdded`.

The machine enforces this at definition time: **a message type that lacks any of the machine's partition keys may only be evolved.** `command`, `start`, `checkpoint` / `final`, `on_stop` outcomes and reactions must use types that declare every key. So a machine never handles a partial-key type, such a message never lands in a machine's claimed batch, and it reaches the machine only through the history read, to be evolved. A machine's checkpoints are its own events alone.

### Rules

- **Each child instance is its own partition.** `(o1, l1)` and `(o1, l2)` are claimed by different workers, so lines run in parallel with each other and with the parent. Within one line, decisions are sequential with a shared state, as for any machine.
- **Spawning is an append.** `spawn :lines, …` emits the child's start event with the parent's partition keys filled in. It carries `order_id` and `line_id`, so it creates the `(o1, l1)` partition and wakes it, and it commits atomically with the parent's decision. The child is in its start state at once (evolve); its entry actions run when its consumer group claims the event (reaction).
- **`start` is a checkpoint, not a reaction.** The child can't refuse to start, which matches spawning in XState. A child that must validate its own start uses existing pieces instead: `reaction(LineAdded) { dispatch StartLine, … }` plus `initial { command StartLine => LineStarted | LineRejected }`. That costs two hops, and the start is no longer atomic with the parent.
- **Child → parent: the parent evolves the child's start and final checkpoints**, into its runtime state (`machine.children`). They carry `order_id`, so they wake the parent's partition and are in its reads (verified), which is what drives `children_done` and `on_done`. Only start and final events, never every child step: whatever the parent evolves joins its guard, and evolving every step would make concurrent parent decisions conflict with busy children.
- **Parent → child: commands addressed by the child's full keys.** `dispatch Cmd, line_id: …` is XState's `sendTo`. Parent facts are evolved by the child, never a signal: to make a child act, the parent must send it something.
- **Stopping: the child declares the outcome, the parent decides when.** `on_stop LineCancelled` generates a `Stop` command on the child, accepted in every non-final state, that decides into `LineCancelled`. The parent sends it, through a generated reaction, to each live child (in `machine.children`, without a final checkpoint):
  - for `spawns … stop_on: OrderCancelled`, when the parent reaches `OrderCancelled`;
  - for `invoke`, when a checkpoint leaves the invoking state, whichever command caused it.

  So each machine only knows its own events. The child claims its `Stop`, so its exit actions run, like `ReleaseStock` above. Finished children aren't sent anything. A child deciding something else at the same moment simply decides `Stop` next, as both are addressed to it. The outcome is the child's own final checkpoint, so it counts towards `children_done` like any other. A child that's spawned or invoked must declare `on_stop`, checked at definition time.
- **One invoked child per activation.** It's spawned in the decision that enters the state, so the start event commits with the entering checkpoint. Re-entering the state (`PaymentFailed → AwaitingPayment`) invokes a new child, with new `keys`, so the old child's events can't be mistaken for the new one's.
- **`on_done` maps the child's final checkpoints onto the parent's**, through the same generated reaction plus command as `children_done`. The mapped checkpoints are checked at definition time like command targets: in the invoking state's region or an ancestor's.
- **Results only count while the state is active.** If the parent has already left the invoking state when the child's final checkpoint arrives (say it was cancelled a moment earlier), the generated command does nothing, like any stale timer or join. Whether, say, captured money gets refunded is domain logic: the parent reacting to `PaymentCaptured` while cancelled.
- **An invoked child's input is usually just its keys.** The child evolves the parent's facts for the rest (the amount from `OrderPlaced`), so `PaymentRequested` only needs `order_id` and `payment_id`. It can carry more, for things the parent decided but didn't record elsewhere.
- **The child's partition keys must include all of the parent's** (`[:order_id, :line_id]` ⊇ `[:order_id]`), checked at definition time. That's what makes the child's events reach the parent, and the parent's facts available for the child to evolve. A machine with unrelated keys (a `PaymentMachine` keyed by `payment_id` alone) still works as a pure actor: started by a command, talking only through commands and reactions. But it can't be spawned or invoked.
- **Spawn events are data for the parent.** They don't count toward the one-checkpoint rule.

## Open

- **A command accepted by both a state and an active descendant.** In XState the deepest state that handles an event wins, and ancestors are only a fallback. Do the same here (the parent's handler runs only if no active descendant accepts the command), or run both in definition order like sibling regions?
- **How much of a child the parent sees.** Start and final checkpoints only, or opt-in to more (`spawns LineItemMachine, as: :lines, track: [LineReserved]`) at the cost of guard conflicts?
