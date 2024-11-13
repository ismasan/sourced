# frozen_string_literal: true

require 'spec_helper'

class ReactTestReactor
  include Sors::React

  Event1 = Sors::Message.define('reacttest.event1')
  Event2 = Sors::Message.define('reacttest.event2')
  Event3 = Sors::Message.define('reacttest.event3')
  Cmd1 = Sors::Message.define('reacttest.cmd1')
  Cmd2 = Sors::Message.define('reacttest.cmd2')

  react Event1 do |event|
    event.follow(Cmd1)
  end

  react Event2 do |event|
    event.follow(Cmd2)
  end

  react Event3 do |event|
    nil
  end
end

RSpec.describe Sors::React do
  specify '.react returns commands' do
    evt1 = ReactTestReactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = ReactTestReactor::Event2.new(stream_id: '1', seq: 2)
    evt3 = ReactTestReactor::Event3.new(stream_id: '1', seq: 3)
    commands = ReactTestReactor.new.react([evt1, evt2])
    expect(commands.map(&:class)).to eq([ReactTestReactor::Cmd1, ReactTestReactor::Cmd2])
  end

  specify '.handled_events_for_react' do
    expect(ReactTestReactor.handled_events_for_react).to eq([
      ReactTestReactor::Event1, 
      ReactTestReactor::Event2,
      ReactTestReactor::Event3,
    ])
  end
end
