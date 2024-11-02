# frozen_string_literal: true

require 'spec_helper'

module EvolveTest
  class Reactor
    include Sors::Evolve

    Event1 = Sors::Message.define('evolvetest.reactor.event1')
    Event2 = Sors::Message.define('evolvetest.reactor.event2')
    Event3 = Sors::Message.define('evolvetest.reactor.event3')

    evolve Event1 do |state, event|
      state << event
    end

    evolve Event2 do |state, event|
      state << event
    end

    # TODO:This is never good
    # We never want to evolve from any event belonging to any possible
    # domain in the system
    # we need to scope by domain or category.
    # ex. evolve_from 'carts'
    # evolve :any do |state, event|
    #   state << event.seq
    # end
  end

  class Noop
    include Sors::Evolve

    evolve Reactor::Event1
  end

  class EvolveAll
    include Sors::Evolve

    evolve_all Reactor do |state, event|
      state << event
    end
  end

  class WithBefore < EvolveAll
    before_evolve do |state, event|
      state << event.seq
    end
  end
end

RSpec.describe Sors::Evolve do
  specify '.evolve' do
    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = EvolveTest::Reactor::Event2.new(stream_id: '1', seq: 2)
    state = []
    new_state = EvolveTest::Reactor.new.evolve(state.dup, [evt1, evt2])
    expect(new_state).to eq([evt1, evt2])
  end

  specify '.handled_events_for_evolve' do
    expect(EvolveTest::Reactor.handled_events_for_evolve).to eq([
      EvolveTest::Reactor::Event1, 
      EvolveTest::Reactor::Event2
    ])
  end

  specify '.evolve handlers without a block' do
    expect(EvolveTest::Noop.handled_events_for_evolve).to eq([EvolveTest::Reactor::Event1])

    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    state = []
    new_state = EvolveTest::Noop.new.evolve(state, [evt1])
    expect(new_state).to eq(state)
  end

  specify '.evolve_all' do
    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = EvolveTest::Reactor::Event2.new(stream_id: '1', seq: 2)
    state = []
    new_state = EvolveTest::EvolveAll.new.evolve(state.dup, [evt1, evt2])
    expect(new_state).to eq([evt1, evt2])
  end

  specify '.before_evolve' do
    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = EvolveTest::Reactor::Event2.new(stream_id: '1', seq: 2)
    # evt3 is not handled by the reactor
    evt3 = EvolveTest::Reactor::Event3.new(stream_id: '1', seq: 3)
    state = []
    new_state = EvolveTest::WithBefore.new.evolve(state.dup, [evt1, evt2, evt3])
    expect(new_state).to eq([1, evt1, 2, evt2])
  end
end
