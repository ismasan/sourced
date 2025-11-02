# frozen_string_literal: true

require 'spec_helper'

module EvolveTest
  class Reactor
    include Sourced::Evolve

    Event1 = Sourced::Message.define('evolvetest.reactor.event1')
    Event2 = Sourced::Message.define('evolvetest.reactor.event2')
    Event3 = Sourced::Message.define('evolvetest.reactor.event3')

    state do |_id|
      []
    end

    event Event1 do |state, event|
      state << event
    end

    event Event2 do |state, event|
      state << event
    end
  end

  class ChildReactor < Reactor
    event Event3
  end

  class Noop
    include Sourced::Evolve

    state do |_id|
      []
    end

    event Reactor::Event1
  end

  class EvolveAll
    include Sourced::Evolve

    state do |_id|
      []
    end

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

RSpec.describe Sourced::Evolve do
  specify '.evolve' do
    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = EvolveTest::Reactor::Event2.new(stream_id: '1', seq: 2)
    state = EvolveTest::Reactor.new.evolve([evt1, evt2])
    expect(state).to eq([evt1, evt2])
  end

  specify '.handled_messages_for_evolve' do
    expect(EvolveTest::Reactor.handled_messages_for_evolve).to eq([
      EvolveTest::Reactor::Event1,
      EvolveTest::Reactor::Event2
    ])

    expect(EvolveTest::ChildReactor.handled_messages_for_evolve).to eq([
      EvolveTest::Reactor::Event1,
      EvolveTest::Reactor::Event2,
      EvolveTest::Reactor::Event3,
    ])
  end

  specify '.evolve handlers without a block' do
    expect(EvolveTest::Noop.handled_messages_for_evolve).to eq([EvolveTest::Reactor::Event1])

    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    new_state = EvolveTest::Noop.new.evolve([evt1])
    expect(new_state).to eq([])
  end

  specify '.evolve_all' do
    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = EvolveTest::Reactor::Event2.new(stream_id: '1', seq: 2)
    evolver = EvolveTest::EvolveAll.new
    expect(evolver.state).to eq([])
    new_state = evolver.evolve([evt1, evt2])
    expect(new_state).to eq([evt1, evt2])
    expect(evolver.state).to eq([evt1, evt2])
  end

  specify '.before_evolve' do
    evt1 = EvolveTest::Reactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = EvolveTest::Reactor::Event2.new(stream_id: '1', seq: 2)
    # evt3 is not handled by the reactor
    evt3 = EvolveTest::Reactor::Event3.new(stream_id: '1', seq: 3)
    new_state = EvolveTest::WithBefore.new.evolve([evt1, evt2, evt3])
    expect(new_state).to eq([1, evt1, 2, evt2])
  end
end
