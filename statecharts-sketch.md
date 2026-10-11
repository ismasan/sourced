```ruby
# Compiles to ONE Decider partitioned by :order_id. Regions are orthogonal (active together),
# but every message for an order is decided one at a time, run-to-completion, under one guard.
class OrderChart < Sourced::Statechart
  partition_by :order_id

  # Shared context: one per chart instance, rebuilt from the log for every decision.
  context { { order_id: nil, total: 0, card: nil } }

  # Context changes only through events. Evolve is chart-level: there's one context, not one per region.
  evolve(OrderPlaced)      { |ctx, evt| ctx[:order_id] = evt.payload.order_id; ctx[:total] = evt.payload.total }
  evolve(PaymentInfoAdded) { |ctx, evt| ctx[:card] = evt.payload.card }  # data event

  initial do
    command(PlaceOrder) { |ctx, cmd| event OrderPlaced, order_id: cmd.payload.order_id, total: cmd.payload.total }
  end

  state OrderPlaced do                          # checkpoint; having regions makes it parallel
    command(CancelOrder) { |ctx, cmd| event OrderCancelled, order_id: cmd.payload.order_id }
    # OrderCancelled leaves OrderPlaced, so both regions stop being active in the same evolve.

    region :payment do
      initial do
        command StartPayment do |ctx, cmd|
          event PaymentStarted, order_id: cmd.payload.order_id         # checkpoint: the state becomes this
          event PaymentInfoAdded, order_id: cmd.payload.order_id, card: cmd.payload.card  # data, though it's last
        end
      end

      state PaymentStarted do
        # entry / after are reactions: they run after the decision commits, and only dispatch.
        entry { |ctx| dispatch ChargeCard, order_id: ctx[:order_id], card: ctx[:card] }
        after(3600) { |ctx| dispatch ExpirePayment, order_id: ctx[:order_id] }  # no-op if PaymentStarted was left
        command(AddPaymentInfo) { |ctx, cmd| event PaymentInfoAdded, ... }  # state stays PaymentStarted
        command(ExpirePayment)  { |ctx, cmd| event PaymentExpired, ... }
      end

      final PaymentCaptured                     # external fact, appended by a payments reactor
      state PaymentExpired do
        entry { |ctx| dispatch CancelOrder, order_id: ctx[:order_id] }
      end
    end

    region :shipping do
      final ShipmentDispatched
    end

    # Join. When a decision makes the last region final, OrderCompleted is appended in that
    # same decision. When the last region goes final through an external fact (PaymentCaptured),
    # the chart reacts to it and dispatches a generated command that appends OrderCompleted.
    done OrderCompleted
  end

  final OrderCompleted
  final OrderCancelled
end
```

## Semantics

- **One chart, one partition, one Decider.** The configuration (`{ OrderPlaced => { payment: PaymentStarted, shipping: :initial } }`) and the context are evolved from all of the chart's events. Workers claim a partition one at a time and decide its messages one by one, so regions never run concurrently and can share the context.
- **`handle!` is optimistic, not queued.** Two concurrent `handle!` calls for the same order (say one for payment, one for shipping) decide in parallel. One gets `ConcurrentAppendError` and retries, because the guard covers the whole chart.
- **Checkpoints vs data.** Events declared as `state` / `final` are checkpoints: a region's state is its latest checkpoint. All other evolved events are data and only change the context.
- **Eventless transitions belong in the decision.** Joins (`done`), and any `always`-style transition, are compiled into the decision that triggers them, not into reactions. Otherwise the chart would briefly sit in a state XState never shows.
- **Concurrency means child charts.** For real parallel work, invoke or spawn a child chart: a separate reactor with its own partition and context, talking to the parent only through commands and events.

## Open

- A command accepted by more than one active region: run the handlers in definition order (the second sees the first's events), as SCXML does, or reject it at compile time?
- Region initial states have no event ("no payment checkpoint since `OrderPlaced`"). Region history must be scoped by the position of the entering checkpoint, so that re-entering `OrderPlaced` doesn't pick up old payment checkpoints.
- One decision emitting several checkpoints in a region: the last one is the state, but do the earlier ones' `entry` / `after` still run?
