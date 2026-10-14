# frozen_string_literal: true

# Expected transitions for machine specs:
#
#   moved(OrderMachine, 'order.place', from: 'initial', to: 'placed', entered: %w[placed], order_id: 'o1')
module MachineHelpers
  # A +Transitioned+ of +machine+, as a decision would build it. +exited+
  # defaults to none, +entered+ to the target alone.
  def moved(machine, trigger, from:, to:, exited: [], entered: [to], **keys)
    machine::Transitioned.new(payload: { **keys, trigger:, source: from, target: to, exited:, entered: })
  end
end
