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
  subject(:router) { described_class.new(backend:) }

  let(:backend) { Sourced::Backends::TestBackend.new }

  # describe '#dispatch_next_command' do
  #   before do
  #     router.register(RouterTest::DeciderReactor)
  #   end
  #
  #   context 'when reactor raises exception' do
  #     it 'invokes .on_exception on reactor' do
  #       cmd = RouterTest::AddItem.new
  #       router.backend.schedule_commands([cmd])
  #       expect(RouterTest::DeciderReactor).to receive(:handle_command).and_raise('boom')
  #
  #       router.dispatch_next_command
  #     end
  #   end
  # end

  describe '#register' do
    it 'registers group id with configured backend' do
      expect(backend).to receive(:register_consumer_group).with(RouterTest::DeciderReactor.consumer_info.group_id)
      router.register(RouterTest::DeciderReactor)
    end
  end

  describe '#schedule_commands' do
    it 'schedules commands for the right target deciders' do
      router.register(RouterTest::DeciderReactor)
      cmd = RouterTest::AddItem.new
      expect(backend).to receive(:schedule_commands).with([cmd], group_id: RouterTest::DeciderReactor.consumer_info.group_id)
      router.schedule_commands([cmd])
    end
  end

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
      expect(router).to receive(:handle_and_ack_events_for_reactor).with(RouterTest::SyncReactor1, [evt])
      expect(router).not_to receive(:handle_and_ack_events_for_reactor).with(RouterTest::SyncReactor2, [evt])
      router.handle_events([evt])
    end

    it 'raises if registering a non-compliant interface' do
      expect do
        router.register('nope')
      end.to raise_error(Sourced::InvalidReactorError)
    end
  end
end
