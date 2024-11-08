# A batch operation defined
# as chains of Command => Event => Command
decide Commands::DoStep1 do |_state, command|
  # deciders return events
  command.follow(Events::Step1Done)
end

react Events::Step1Done, async: false do |event|
  # reactors react to events and return new commands
  event.follow(Commands::DoStep2)
end

decide Commands::DoStep2 do |state, command|
  # Deciders can emit different events
  # depending on state
  if state.foo?
    command.follow(Events::Step2Done)
  else
    command.follow(Events::Step2Failed)
  end
end

# Reactors expect specific events
# This means the runnimg operation
# will branch out depending on the event
react Events::Step2Done do |event|
  event.follow(Commands::DoStep3)
end

# Encapsulate Inventory service
# Handles commands, events and reactions of Inventory stuff
class Inventory < Machine
  module Commands
    ReserveItems = Message.define('inventory.reserve_items') do
      attribute :item_ids, [Integer]
    end
  end

  module Events
    # etc...
  end

  decide ReserveItems do |state, command|
    reserved_item_ids = database.reserve_items(command.payload.item_ids)
    if reserved_item_ids == command.payload.item_ids
      command.follow(Events::ItemsReserved, item_ids: command.payload.item_ids)
    else
      command.follow(Events::IncompleteReservation, item_ids: command.payload.item_ids, reserved_item_ids:)
    end
  end

  # Event choreography:
  # An Inventory service reacts to
  # events emitted by an Orders service
  react Orders::Events::OrderPlaced do |event|
    event.follow(Commands::ReserveItems)
  end
end

class Orchestrator < Machine
  # Event orchestration:
  # A standalone reactor reacts to events
  # emitted by multiple services and schedules commands to other services
  react Orders::Events::OrderPlaced do |event|
    event.follow(Inventory::Commands::ReserveItems)
  end
end


