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

    def self.handled_messages
      [ItemAdded, AddItem, NextCommand]
    end

    def self.handle(evt, replaying:, history:)
      cmd = NextCommand.parse(stream_id: evt.stream_id)

      Sourced::Actions::AppendNext.new([cmd])
    end

    # Override default handle_batch to forward all kargs
    def self.handle_batch(batch, history: [])
      batch.map do |message, replaying|
        actions = handle(message, replaying:, history:)
        [actions, message]
      end
    end
  end

  # Test reactors for argument injection
  class ReactorWithNoArgs
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event)
      Sourced::Actions::OK
    end
  end

  class ReactorWithReplayingOnly
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, replaying:)
      Sourced::Actions::OK
    end
  end

  # A reactor that needs history — must override handle_batch
  class ReactorWithHistoryOnly
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, history:)
      Sourced::Actions::OK
    end

    def self.handle_batch(batch, history: [])
      batch.map do |message, replaying|
        actions = handle(message, history:)
        [actions, message]
      end
    end
  end

  class ReactorWithBothArgs
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, replaying:, history:)
      Sourced::Actions::OK
    end

    def self.handle_batch(batch, history: [])
      batch.map do |message, replaying|
        actions = handle(message, replaying:, history:)
        [actions, message]
      end
    end
  end

  class ReactorWithLogger
    extend Sourced::Consumer

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event, logger:)
      Sourced::Actions::OK
    end
  end

  class ReactorWithBatchSize
    extend Sourced::Consumer

    consumer do |c|
      c.batch_size = 10
    end

    def self.handled_messages
      [ItemAdded]
    end

    def self.handle(event)
      Sourced::Actions::OK
    end
  end
end

RSpec.describe Sourced::Router do
  subject(:router) { described_class.new(backend:) }

  let(:backend) { Sourced::Backends::TestBackend.new }

  describe '#drain' do
    it 'handles and acknoledges messages for reactors, until there is none left' do
      logs = []
      reactor = Class.new do
        extend Sourced::Consumer
        def self.handled_messages = [RouterTest::ItemAdded]
      end
      reactor.define_singleton_method(:handle) do |message|
        logs << message.type
        []
      end

      e1 = RouterTest::ItemAdded.build('123')
      e2 = RouterTest::ItemAdded.build('123')
      e3 = RouterTest::ItemAdded.build('123')
      backend.append_next_to_stream('123', [e1, e2, e3])
      router.register(reactor)
      router.drain
      expect(logs.size).to eq(3)
      expect(logs.uniq).to eq(%w[routertest.todos.added])
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

        bool = router.handle_next_event_for_reactor(RouterTest::DeciderReactor)
        expect(bool).to be(true)
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

    context 'when there are no new messages for reactor' do
      it 'return false' do
        backend.ack_on(RouterTest::DeciderReactor.consumer_info.group_id, event.id)
        bool = router.handle_next_event_for_reactor(RouterTest::DeciderReactor)
        expect(bool).to be(false)
      end
    end

    context 'when reactor raises exception' do
      before do
        expect(RouterTest::DeciderReactor).to receive(:handle_batch).and_raise('boom')
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

      it 'raises immediatly is passed raise_on_error = true' do
        expect {
          router.handle_next_event_for_reactor(RouterTest::DeciderReactor, nil, true)
        }.to raise_error(RuntimeError, 'boom')
      end
    end

    context 'handle_batch interface' do
      let(:event) { RouterTest::ItemAdded.new(stream_id: '123') }
      let(:event2) { RouterTest::ItemAdded.new(stream_id: '123', seq: 2) }

      before do
        backend.clear!
        backend.append_to_stream('123', [event, event2])
      end

      context 'with reactor using default handle_batch (no history)' do
        before { router.register(RouterTest::ReactorWithNoArgs) }

        it 'calls handle_batch with batch (no history kwarg)' do
          received_batch = nil
          allow(RouterTest::ReactorWithNoArgs).to receive(:handle_batch).and_wrap_original do |original, batch, **kargs|
            received_batch = [batch, kargs]
            original.call(batch, **kargs)
          end
          router.handle_next_event_for_reactor(RouterTest::ReactorWithNoArgs)
          expect(received_batch).not_to be_nil
          batch, kargs = received_batch
          expect(batch.first.first).to eq(event)
          expect(kargs).not_to have_key(:history)
        end
      end

      context 'with reactor needing history (handle_batch accepts history:)' do
        before { router.register(RouterTest::ReactorWithHistoryOnly) }

        it 'calls handle_batch with batch and history' do
          allow(RouterTest::ReactorWithHistoryOnly).to receive(:handle_batch).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithHistoryOnly)
          expect(RouterTest::ReactorWithHistoryOnly).to have_received(:handle_batch) do |batch, **kargs|
            expect(batch.first.first).to eq(event)
            expect(kargs[:history]).to be_an(Array)
            expect(kargs[:history].map(&:id)).to include(event.id, event2.id)
          end
        end
      end

      context 'with reactor needing both replaying and history' do
        before { router.register(RouterTest::ReactorWithBothArgs) }

        it 'calls handle_batch with batch and history' do
          allow(RouterTest::ReactorWithBothArgs).to receive(:handle_batch).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithBothArgs)
          expect(RouterTest::ReactorWithBothArgs).to have_received(:handle_batch) do |batch, **kargs|
            expect(batch.first.first).to eq(event)
            expect(kargs[:history]).to be_an(Array)
          end
        end
      end

      context 'Consumer default handle_batch forwards replaying to handle' do
        before { router.register(RouterTest::ReactorWithReplayingOnly) }

        it 'passes replaying through to handle via default handle_batch' do
          # Force handle_kargs caching before spy wraps .handle (spy changes method signature)
          RouterTest::ReactorWithReplayingOnly.send(:handle_kargs)
          allow(RouterTest::ReactorWithReplayingOnly).to receive(:handle).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithReplayingOnly)
          expect(RouterTest::ReactorWithReplayingOnly).to have_received(:handle).with(event, replaying: false)
        end
      end

      context 'Consumer default handle_batch forwards logger to handle' do
        before { router.register(RouterTest::ReactorWithLogger) }

        it 'passes logger through to handle via default handle_batch' do
          # Force handle_kargs caching before spy wraps .handle (spy changes method signature)
          RouterTest::ReactorWithLogger.send(:handle_kargs)
          allow(RouterTest::ReactorWithLogger).to receive(:handle).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithLogger)
          expect(RouterTest::ReactorWithLogger).to have_received(:handle).with(event, logger: router.logger)
        end
      end

      context 'per-reactor batch_size override' do
        before do
          router.register(RouterTest::ReactorWithBatchSize)
        end

        it 'uses the reactor consumer_info.batch_size over the worker-level default' do
          allow(backend).to receive(:reserve_next_for_reactor).and_call_original
          router.handle_next_event_for_reactor(RouterTest::ReactorWithBatchSize, nil, false, batch_size: 1)
          expect(backend).to have_received(:reserve_next_for_reactor).with(
            RouterTest::ReactorWithBatchSize,
            batch_size: 10,
            with_history: false,
            worker_id: nil
          )
        end
      end
    end
  end

  specify 'class-level API' do
    expect(router.async_reactors).to eq(Sourced::Router.async_reactors)
    expect(Sourced::Router).to respond_to(:register)
    expect(Sourced::Router).to respond_to(:registered?)
    expect(Sourced::Router).to respond_to(:handle_next_event_for_reactor)
    expect(Sourced::Router).to respond_to(:backend)
  end

  describe '#register' do
    before do
      allow(backend).to receive(:register_consumer_group)
    end

    it 'registers group id with configured backend' do
      router.register(RouterTest::DeciderReactor)
      expect(backend).to have_received(:register_consumer_group).with(RouterTest::DeciderReactor.consumer_info.group_id)
    end

    it 'determines if reactor needs history from handle_batch signature' do
      router.register(RouterTest::DeciderReactor)
      expect(router.needs_history[RouterTest::DeciderReactor]).to be(true)

      router.register(RouterTest::ReactorWithNoArgs)
      expect(router.needs_history[RouterTest::ReactorWithNoArgs]).to be(false)
    end
  end

  describe '#register' do
    it 'registers Reactor interfaces and registers group' do
      expect(backend).to receive(:register_consumer_group).with(RouterTest::DeciderReactor.consumer_info.group_id)
      router.register(RouterTest::DeciderReactor)
      expect(router.async_reactors).to include(RouterTest::DeciderReactor)
      expect(router.registered?(RouterTest::DeciderReactor)).to be true
    end

    it 'raises if registering a non-compliant interface' do
      expect do
        router.register('nope')
      end.to raise_error(Sourced::InvalidReactorError)
    end
  end
end
