# frozen_string_literal: true

require 'spec_helper'

module ReactDSLTests
  Event = Sourced::Message.define('reactdsl.test')

  class Actor < Sourced::Actor
    include Sourced::React
  end
end

RSpec.describe Sourced::React do
  it 'raises error when resolving message symbol is not implemented' do
    expect { ReactDSLTests::Actor.reaction(nil) }.to raise_error(ArgumentError, /Invalid arguments/)
  end

  it 'handles no arguments (wildcard reaction)' do
    expect { ReactDSLTests::Actor.reaction {} }.not_to raise_error
  end

  it 'handles a single event class' do
    expect { ReactDSLTests::Actor.reaction(ReactDSLTests::Event) }
      .not_to raise_error
  end

  it 'handles an array of events' do
    expect { ReactDSLTests::Actor.reaction(ReactDSLTests::Event, ReactDSLTests::Event) }
      .not_to raise_error
  end

  it 'raises an error for an invalid event' do
    expect { ReactDSLTests::Actor.reaction(:non_existent_event) }
      .to raise_error(ArgumentError, /Cannot resolve message symbol/)
  end
end
