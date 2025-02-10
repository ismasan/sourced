# frozen_string_literal: true

require 'spec_helper'

class ReactTestReactor
  include Sourced::React

  Event1 = Sourced::Message.define('reacttest.event1')
  Event2 = Sourced::Message.define('reacttest.event2')
  Event3 = Sourced::Message.define('reacttest.event3')
  Cmd1 = Sourced::Message.define('reacttest.cmd1')
  Cmd2 = Sourced::Message.define('reacttest.cmd2')
  Cmd3 = Sourced::Message.define('reacttest.cmd3')

  react Event1 do |_event|
    command Cmd1
  end

  react Event2 do |_event|
    command Cmd2
    command Cmd3 do |cmd|
      cmd.with_metadata(greeting: 'Hi!')
    end
  end

  react Event3 do |_event|
    nil
  end
end

RSpec.describe Sourced::React do
  specify '.react returns commands' do
    evt1 = ReactTestReactor::Event1.new(stream_id: '1', seq: 1)
    evt2 = ReactTestReactor::Event2.new(stream_id: '1', seq: 2)
    evt3 = ReactTestReactor::Event3.new(stream_id: '1', seq: 3)
    commands = ReactTestReactor.new.react([evt1, evt2])
    expect(commands.map(&:class)).to eq([
                                          ReactTestReactor::Cmd1,
                                          ReactTestReactor::Cmd2,
                                          ReactTestReactor::Cmd3
                                        ])
    expect(commands.map { |e| e.metadata[:producer] }).to eq(%w[ReactTestReactor ReactTestReactor ReactTestReactor])
    expect(commands.first.causation_id).to eq(evt1.id)
    expect(commands.last.causation_id).to eq(evt2.id)
    expect(commands.last.metadata[:greeting]).to eq('Hi!')
  end

  specify '.handled_events_for_react' do
    expect(ReactTestReactor.handled_events_for_react).to eq([
                                                              ReactTestReactor::Event1,
                                                              ReactTestReactor::Event2,
                                                              ReactTestReactor::Event3
                                                            ])
  end
end
