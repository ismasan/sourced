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

    def self.handled_messages
      [ItemAdded, AddItem, NextCommand]
    end

    def self.handle(evt, replaying:, history:)
      cmd = NextCommand.parse(stream_id: evt.stream_id)

      Sourced::Actions::AppendNext.new([cmd])
    end
  end

  class SyncReactor1
    extend Sourced::Consumer

    consumer do |c|
      c.sync!
    end

    # The Reactor interface
    def self.handled_messages
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
    def self.handled_messages
      []
    end

    def self.handle_events(_evts, replaying:)
      []
    end
  end

  # Test reactors for argument injection
  class ReactorWithNoArgs
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event)
      Sourced::Actions::AppendNext.new([])
    end
  end

  class ReactorWithReplayingOnly
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, replaying:)
      Sourced::Actions::AppendNext.new([])
    end
  end

  class ReactorWithHistoryOnly
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, history:)
      Sourced::Actions::AppendNext.new([])
    end
  end

  class ReactorWithBothArgs
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, replaying:, history:)
      Sourced::Actions::AppendNext.new([])
    end
  end

  class ReactorWithLogger
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, logger:)
      Sourced::Actions::AppendNext.new([])
    end
  end
end

RSpec.describe Sourced::Router do
  subject(:router) { described_class.new(backend:) }

  let(:backend) { Sourced::Backends::TestBackend.new }

  describe '#dispatch_next_command', skip: true do
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
      backend.append_to_stream('123', event)
    end

    context 'when reactor returns Sourced::Actions::AppendNext' do
      it 'appends messages' do
        allow(backend).to receive(:append_next_to_stream)

        router.handle_next_event_for_reactor(RouterTest::DeciderReactor)
        expect(RouterTest::DeciderReactor).not_to have_received(:on_exception)
        expect(backend).to have_received(:append_next_to_stream) do |stream_id, events|
          expect(stream_id).to eq('123')
          expect(events.size).to eq(1)
          event = events.first
          expect(event.stream_id).to eq('123')
          expect(event).to be_a(RouterTest::NextCommand)
        end
      end
    end

    context 'when reactor raises exception' do
      before do
        expect(RouterTest::DeciderReactor).to receive(:handle).and_raise('boom')
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

    context 'argument injection' do
      let(:event) { RouterTest::ItemAdded.new(stream_id: '123') }
      let(:event2) { RouterTest::ItemAdded.new(stream_id: '123', seq: 2) }

      before do
        backend.clear!
        backend.append_to_stream('123', [event, event2])
      end

      context 'with reactor expecting no keyword arguments' do
        before { router.register(RouterTest::ReactorWithNoArgs) }

        it 'calls handle with event only' do
          allow(RouterTest::ReactorWithNoArgs).to receive(:handle).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithNoArgs)
          expect(RouterTest::ReactorWithNoArgs).to have_received(:handle).with(event)
        end
      end

      context 'with reactor expecting only replaying argument' do
        before { router.register(RouterTest::ReactorWithReplayingOnly) }

        it 'calls handle with event and replaying status' do
          allow(RouterTest::ReactorWithReplayingOnly).to receive(:handle).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithReplayingOnly)
          expect(RouterTest::ReactorWithReplayingOnly).to have_received(:handle).with(event, replaying: false)
        end
      end

      context 'with reactor expecting only history argument' do
        before { router.register(RouterTest::ReactorWithHistoryOnly) }

        it 'calls handle with event and history' do
          expected_history = [event, event2]
          allow(backend).to receive(:read_event_stream).with('123').and_return(expected_history)
          
          allow(RouterTest::ReactorWithHistoryOnly).to receive(:handle).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithHistoryOnly)
          expect(RouterTest::ReactorWithHistoryOnly).to have_received(:handle).with(event, history: expected_history)
        end
      end

      context 'with reactor expecting both replaying and history arguments' do
        before { router.register(RouterTest::ReactorWithBothArgs) }

        it 'calls handle with event, replaying status, and history' do
          expected_history = [event, event2]
          allow(backend).to receive(:read_event_stream).with('123').and_return(expected_history)
          allow(RouterTest::ReactorWithBothArgs).to receive(:handle).and_call_original

          router.handle_next_event_for_reactor(RouterTest::ReactorWithBothArgs)

          expect(RouterTest::ReactorWithBothArgs).to have_received(:handle).with(
            event, 
            replaying: false, 
            history: expected_history
          )
        end
      end

      context 'when replaying is true' do
        before { router.register(RouterTest::ReactorWithReplayingOnly) }

        it 'passes replaying: true when backend indicates replaying' do
          allow(backend).to receive(:reserve_next_for_reactor).and_yield(event, true)
          
          expect(RouterTest::ReactorWithReplayingOnly).to receive(:handle).with(event, replaying: true)
          router.handle_next_event_for_reactor(RouterTest::ReactorWithReplayingOnly)
        end
      end

      context 'with different stream having different history' do
        let(:other_event) { RouterTest::ItemAdded.new(stream_id: 'other-stream') }

        before do 
          router.register(RouterTest::ReactorWithHistoryOnly)
          backend.append_to_stream('other-stream', other_event)
        end

        it 'fetches history for the correct stream' do
          other_history = [other_event]
          allow(backend).to receive(:read_event_stream).with('other-stream').and_return(other_history)
          
          # Set up the backend to return the other event
          allow(backend).to receive(:reserve_next_for_reactor).and_yield(other_event, false)
          
          expect(RouterTest::ReactorWithHistoryOnly).to receive(:handle).with(other_event, history: other_history)
          router.handle_next_event_for_reactor(RouterTest::ReactorWithHistoryOnly)
        end
      end

      context 'with reactor expecting logger argument' do
        before { router.register(RouterTest::ReactorWithLogger) }

        it 'calls handle with event and router logger' do
          allow(RouterTest::ReactorWithLogger).to receive(:handle).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithLogger)
          expect(RouterTest::ReactorWithLogger).to have_received(:handle).with(event, logger: router.logger)
        end
      end
    end
  end

  describe '#register' do
    before do
      allow(backend).to receive(:register_consumer_group)
    end

    it 'registers group id with configured backend' do
      router.register(RouterTest::DeciderReactor)
      expect(backend).to have_received(:register_consumer_group).with(RouterTest::DeciderReactor.consumer_info.group_id)
    end

    it 'computes kargs for .handle in #kargs_for_handle' do
      router.register(RouterTest::DeciderReactor)
      expect(router.kargs_for_handle[RouterTest::DeciderReactor]).to eq(%i[replaying history])
    end
  end

  describe '#schedule_commands', skip: true do
    it 'schedules commands for the right target deciders' do
      router.register(RouterTest::DeciderReactor)
      cmd = RouterTest::AddItem.new
      expect(backend).to receive(:schedule_commands).with([cmd], group_id: RouterTest::DeciderReactor.consumer_info.group_id)
      router.schedule_commands([cmd])
    end
  end

  describe '#register' do
    it 'registers Reactor interfaces and registers group' do
      expect(backend).to receive(:register_consumer_group).with(RouterTest::DeciderReactor.consumer_info.group_id)
      router.register(RouterTest::DeciderReactor)
      expect(router.async_reactors).to include(RouterTest::DeciderReactor)
      expect(router.registered?(RouterTest::DeciderReactor)).to be true
    end

    it 'registers async reactors' do
      pending 'Re-think sync reactors'
      expect(true).to be false
      # router.register(RouterTest::SyncReactor1)
      # router.register(RouterTest::SyncReactor2)
      #
      # evt = RouterTest::ItemAdded.new
      # expect(router).to receive(:handle_and_ack_events_for_reactor).with(RouterTest::SyncReactor1, [evt])
      # expect(router).not_to receive(:handle_and_ack_events_for_reactor).with(RouterTest::SyncReactor2, [evt])
      # router.handle_events([evt])
    end

    it 'raises if registering a non-compliant interface' do
      expect do
        router.register('nope')
      end.to raise_error(Sourced::InvalidReactorError)
    end
  end
end
