# frozen_string_literal: true

require 'spec_helper'

module RouterTest
  AddItem = Sourced::Message.define('routertest.todos.add')
  ItemAdded = Sourced::Message.define('routertest.todos.added')

  class DeciderOnly
    extend Sourced::Consumer

    # The Decider interface
    def self.handled_commands
      [AddItem]
    end

    def self.handle_command(_cmd); end
  end

  class DeciderReactor
    extend Sourced::Consumer

    consumer do |c|
      c.async!
    end

    # The Decider interface
    def self.handled_commands
      [AddItem]
    end

    def self.handle_command(_cmd); end

    # The Reactor interface
    def self.handled_events
      [ItemAdded]
    end

    def self.handle_events(_evts)
      []
    end
  end

  class SyncReactor1
    extend Sourced::Consumer

    consumer do |c|
      c.sync!
    end

    # The Reactor interface
    def self.handled_events
      [ItemAdded]
    end

    def self.handle_events(_evts)
      []
    end
  end

  class SyncReactor2
    extend Sourced::Consumer

    consumer do |c|
      c.async = false
    end

    # The Reactor interface
    def self.handled_events
      []
    end

    def self.handle_events(_evts)
      []
    end
  end
end

RSpec.describe Sourced::Router do
  subject(:router) { described_class.new }

  describe '#register' do
    it 'registers Decider interfaces' do
      router.register(RouterTest::DeciderReactor)
      cmd = RouterTest::AddItem.new
      expect(RouterTest::DeciderReactor).to receive(:handle_command).with(cmd)
      router.handle_command(cmd)
    end

    it 'registers Reactor interfaces' do
      router.register(RouterTest::DeciderReactor)
      router.register(RouterTest::DeciderOnly)
      expect(router.async_reactors.first).to eq(RouterTest::DeciderReactor)
    end

    it 'registers async reactors' do
      router.register(RouterTest::SyncReactor1)
      router.register(RouterTest::SyncReactor2)

      evt = RouterTest::ItemAdded.new
      expect(Sourced::Router).to receive(:handle_and_ack_events_for_reactor).with(RouterTest::SyncReactor1, [evt])
      expect(Sourced::Router).not_to receive(:handle_and_ack_events_for_reactor).with(RouterTest::SyncReactor2, [evt])
      router.handle_events([evt])
    end
  end
end
