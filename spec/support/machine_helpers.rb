# frozen_string_literal: true

# Expected transitions for machine specs:
#
#   moved(OrderMachine, 'order.place', from: 'initial', to: 'placed', entered: %w[placed], order_id: 'o1')
module MachineHelpers
  # The event +machine+ appends for a transition to the checkpoint at +to+, as
  # a decision would build it. +exited+ defaults to none, +entered+ to the
  # target alone.
  def moved(machine, trigger, from:, to:, exited: [], entered: [to], **keys)
    klass = machine.checkpoint_class(machine.tree.node_at(to).key)
    klass.new(payload: { **keys, trigger:, source: from, target: to, exited:, entered: })
  end
end
