# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::React do
  before do
    stub_const('TestActor', Class.new(Sourced::Actor) do
      include Sourced::React
    end)

    stub_const('TestEvent', Sourced::Message.define('test.event'))
  end

  it 'raises error when resolving message symbol is not implemented' do
    expect { TestActor.reaction(nil) }.to raise_error(ArgumentError, /Invalid arguments/)
  end

  it 'handles no arguments (wildcard reaction)' do
    expect { TestActor.reaction {} }.not_to raise_error
  end

  it 'handles a single event class' do
    expect { TestActor.reaction(TestEvent) }
      .not_to raise_error
  end

  it 'handles an array of events' do
    expect { TestActor.reaction(TestEvent, TestEvent) }
      .not_to raise_error
  end

  it 'raises an error for an invalid event' do
    expect { TestActor.reaction(:non_existent_event) }
      .to raise_error(ArgumentError, /Cannot resolve message symbol/)
  end
end
