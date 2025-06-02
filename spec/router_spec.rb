# frozen_string_literal: true

require 'spec_helper'

module RouterTest
  AddItem = Sourced::Message.define('routertest.todos.add')
  NextCommand = Sourced::Message.define('routertest.todos.next')
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
      [AddItem, NextCommand]
    end

    def self.handle_command(_cmd); end

    # The Reactor interface
    def self.handled_events
      [ItemAdded]
    end

    def self.handle_events(evts, replaying:)
      cmd = NextCommand.parse(stream_id: evts.first.stream_id)

      [cmd]
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

    def self.handle_events(_evts, replaying:)
      []
    end
  end
end

RSpec.describe Sourced::Router do
  subject(:router) { described_class.new(backend:) }

  let(:backend) { Sourced::Backends::TestBackend.new }

  describe '#dispatch_next_command' do
    let(:cmd) { RouterTest::AddItem.new(stream_id: '123') }

    before do
      router.register(RouterTest::DeciderReactor)
    end

    context 'with successful command handling' do
      it 'deletes command from bus' do
        router.dispatch_next_command
        expect(backend.next_command).to be_nil
      end
    end

    context 'when reactor raises exception' do
      before do
        router.schedule_commands([cmd])
        allow(RouterTest::DeciderReactor).to receive(:on_exception)
        expect(RouterTest::DeciderReactor).to receive(:handle_command).and_raise('boom')
      end

      it 'invokes .on_exception on reactor' do
        router.dispatch_next_command

        expect(RouterTest::DeciderReactor).to have_received(:on_exception) do |exception, message, group|
          expect(exception.message).to eq('boom')
          expect(message).to eq(cmd)
          expect(group).to respond_to(:stop)
        end
      end

      it 'does not delete command from bus, so that it can be retried' do
        router.dispatch_next_command
        expect(backend.next_command).to eq(cmd)
      end
    end
  end

  describe '#handle_next_event_for_reactor' do
    let(:event) { RouterTest::ItemAdded.new(stream_id: '123') }

    before do
      router.register(RouterTest::DeciderReactor)

      allow(RouterTest::DeciderReactor).to receive(:on_exception)
      backend.append_to_stream('123', [event])
    end

    context 'when reactor returns commands' do
      it 'schedules commands' do
        allow(backend).to receive(:schedule_commands)

        router.handle_next_event_for_reactor(RouterTest::DeciderReactor)
        expect(RouterTest::DeciderReactor).not_to have_received(:on_exception)
        expect(backend).to have_received(:schedule_commands) do |commands, group_id:|
          expect(commands.map(&:class)).to eq([RouterTest::NextCommand])
          expect(group_id).to eq(RouterTest::DeciderReactor.consumer_info.group_id)
        end
      end
    end

    context 'when reactor raises exception' do
      before do
        expect(RouterTest::DeciderReactor).to receive(:handle_events).and_raise('boom')
      end

      it 'invokes .on_exception on reactor' do
        router.handle_next_event_for_reactor(RouterTest::DeciderReactor)

        expect(RouterTest::DeciderReactor).to have_received(:on_exception) do |exception, message, group|
          expect(exception.message).to eq('boom')
          expect(message).to eq(event)
          expect(group).to respond_to(:stop)
        end
      end

      it 'does not acknowledge event for reactor, so that it can be retried' do
        router.handle_next_event_for_reactor(RouterTest::DeciderReactor)
        groups = backend.stats.groups
        expect(groups.first[:stream_count]).to eq(0)
      end
    end
  end

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
