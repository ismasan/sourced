# frozen_string_literal: true

require 'spec_helper'

class EvolveTestReactor
  include Sors::Evolve

  Event1 = Sors::Message.define('evolvetest.event1')
  Event2 = Sors::Message.define('evolvetest.event2')

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

RSpec.describe Sors::Evolve do
  specify '.evolve' do
    evt1 = EvolveTestReactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = EvolveTestReactor::Event2.new(stream_id: '1', seq: 2)
    state = []
    new_state = EvolveTestReactor.new.evolve(state.dup, [evt1, evt2])
    expect(new_state).to eq([evt1, evt2])
  end

  specify '.handled_events_for_evolve' do
    expect(EvolveTestReactor.handled_events_for_evolve).to eq([
      EvolveTestReactor::Event1, 
      EvolveTestReactor::Event2
    ])
  end
end
