# frozen_string_literal: true

require 'spec_helper'

module HandlerTests
  class MyHandler
    include Sourced::Handler

    Event = Sourced::Message.define('handlertest.event') do
      attribute :value
    end

    on :start, name: String do |event|
      [event.follow(Event, value: event.payload.name)]
    end

    on :stop do |event, history:|
      [event.follow(Event, value: history.size)]
    end

    on :foo, :bar do |event, history:|
      [event.follow(Event, value: event.class.name)]
    end
  end
end

RSpec.describe Sourced::Handler do
  it 'implements the Reactor interface' do
    expect(Sourced::ReactorInterface === HandlerTests::MyHandler).to be(true)
  end

  specify '.handle' do
    msg = HandlerTests::MyHandler::Start.build('aa', name: 'Joe')
    result = HandlerTests::MyHandler.handle(msg)
    expect(result.first).to be_a(Sourced::Actions::AppendNext)
    expect(result.first.messages.first.payload.value).to eq('Joe')

    msg2 = HandlerTests::MyHandler::Stop.build('aa')
    result = HandlerTests::MyHandler.handle(msg2, history: [msg2])
    expect(result.first).to be_a(Sourced::Actions::AppendNext)
    expect(result.first.messages.first.payload.value).to eq(1)
  end

  specify '.on with multiple messages' do
    msg = HandlerTests::MyHandler::Foo.build('aa')
    result = HandlerTests::MyHandler.handle(msg)
    expect(result.first).to be_a(Sourced::Actions::AppendNext)
    expect(result.first.messages.first.payload.value).to eq('HandlerTests::MyHandler::Foo')

    msg = HandlerTests::MyHandler::Bar.build('aa')
    result = HandlerTests::MyHandler.handle(msg)
    expect(result.first).to be_a(Sourced::Actions::AppendNext)
    expect(result.first.messages.first.payload.value).to eq('HandlerTests::MyHandler::Bar')
  end
end
