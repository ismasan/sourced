# frozen_string_literal: true

module BackendExamples
  module Tests
    DoSomething = Sourced::Message.define('tests.do_something') do
      attribute :account_id, Integer
    end
    SomethingHappened1 = Sourced::Message.define('tests.something_happened1') do
      attribute :account_id, Integer
    end
    SomethingHappened2 = Sourced::Message.define('tests.something_happened2') do
      attribute :account_id, Integer
    end
  end

  RSpec.shared_examples 'an ActiveRecord backend' do |_database_config|
    before :all do
      described_class.table_prefix = 'sors_ar'

      ActiveRecord::Base.establish_connection(
        adapter: 'postgresql',
        database: 'sors_test'
      )

      Migrator.new(table_prefix: described_class.table_prefix).up
    end

    after :all do
      Migrator.new(table_prefix: described_class.table_prefix).down
    end

    after do
      backend.clear!
    end

    it_behaves_like 'a backend' do
      specify 'auto-incrementing global_seq' do
        cmd1 = BackendExamples::Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = cmd1.follow_with_seq(BackendExamples::Tests::SomethingHappened1, 2, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow_with_seq(BackendExamples::Tests::SomethingHappened1, 3, account_id: cmd1.payload.account_id)
        evt3 = BackendExamples::Tests::SomethingHappened1.parse(stream_id: 's1', seq: 4, payload: { account_id: 1 })
        backend.append_to_stream(cmd1.stream_id, [evt1, evt2, evt3])
        expect(Sourced::Backends::ActiveRecordBackend::EventRecord.order(global_seq: :asc).pluck(:global_seq))
          .to eq([1, 2, 3])
      end
    end
  end

  RSpec.shared_examples 'a backend' do
    it 'is installed' do
      expect(backend.installed?).to be(true)
    end

    it 'supports the Backend interface' do
      expect do
        Sourced::Configuration::BackendInterface.parse(backend)
      end.not_to raise_error
    end

    describe '#transaction' do
      it 'resets append on error' do
        evt = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        expect do
          backend.transaction do
            backend.append_to_stream('s1', [evt])
            raise 'boom'
          end
        end.to raise_error('boom')
        expect(backend.read_stream('s1').any?).to be(false)
      end
    end

    describe '#schedule_messages and #update_schedule!' do
      it 'schedules messages to be read in the future' do
        now = Time.now

        msg0 = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 0 })
        msg1 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        msg2 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 2 })
        msg3 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 3 })

        backend.append_to_stream('s1', [msg0])
        backend.schedule_messages([msg1, msg2], at: now + 2)
        backend.schedule_messages([msg3], at: now + 10)

        backend.update_schedule!
        expect(backend.read_stream('s1')).to eq([msg0])

        Timecop.freeze(now + 3) do
          backend.update_schedule!
          expect(backend.read_stream('s1').map(&:id)).to eq([msg0, msg1, msg2].map(&:id))
        end

        Timecop.freeze(now + 11) do
          backend.update_schedule!
          messages = backend.read_stream('s1')
          expect(messages.map(&:id)).to eq([msg0, msg1, msg2, msg3].map(&:id))
          expect(messages.map(&:seq)).to eq([1, 2, 3, 4])
        end
      end

    end

    describe '#append_next_to_stream' do
      it 'appends single event to stream incrementing :seq automatically' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', evt1)
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 2 })
        expect(backend.append_next_to_stream('s1', evt2)).to be(true)
        events = backend.read_stream('s1')
        expect(events.map(&:stream_id)).to eq(['s1', 's1'])
        expect(events.map(&:seq)).to eq([1, 2])
      end

      it 'appends multiple events to stream incrementing :seq automatically' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', evt1)
        
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 2 })
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 3 })
        evt4 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 4 })
        
        expect(backend.append_next_to_stream('s1', [evt2, evt3, evt4])).to be(true)
        events = backend.read_stream('s1')
        expect(events.map(&:stream_id)).to eq(['s1', 's1', 's1', 's1'])
        expect(events.map(&:seq)).to eq([1, 2, 3, 4])
      end

      it 'handles empty array' do
        expect(backend.append_next_to_stream('s1', [])).to be(true)
        events = backend.read_stream('s1')
        expect(events).to eq([])
      end

      it 'creates new stream when appending to non-existent stream with array' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 2 })
        
        expect(backend.append_next_to_stream('s1', [evt1, evt2])).to be(true)
        events = backend.read_stream('s1')
        expect(events.map(&:stream_id)).to eq(['s1', 's1'])
        expect(events.map(&:seq)).to eq([1, 2])
      end
    end

    describe '#append_to_stream and #reserve_next_for_reactor' do
      it 'supports a time window' do
        now = Time.now
        cmd_a = nil
        evt_a1 = nil
        evt_a2 = nil

        Timecop.freeze(now - 60) do
          cmd_a = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
          evt_a1 = cmd_a.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd_a.payload.account_id)
        end
        Timecop.freeze(now - 3) do
          evt_a2 = cmd_a.follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd_a.payload.account_id)
        end
        backend.append_to_stream('s1', [cmd_a, evt_a1, evt_a2])

        reactor1 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(
              group_id: 'group1',
              start_from: -> { Time.now - 5 }
            )
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end

        backend.register_consumer_group('group1')

        messages = []

        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end
        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages).to eq([evt_a2])
      end

      it 'schedules messages and handles them in the future, setting correlation IDs properly' do
        cmd_a = Tests::DoSomething.parse(stream_id: 's1', correlation_id: SecureRandom.uuid, seq: 1, payload: { account_id: 1 })
        evt_b = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })

        reactor1 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::DoSomething, Tests::SomethingHappened1]
          end
        end

        backend.register_consumer_group('group1')
        backend.append_to_stream('s1', [cmd_a])

        now = Time.now

        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.map { |msg, _| [Sourced::Actions::Schedule.new([evt_b], at: now + 10), msg] }
        end

        expect(backend.read_stream('s1')).to eq([cmd_a])

        Timecop.freeze(now + 11) do
          backend.update_schedule!
          messages = backend.read_stream('s1')
          expect(messages.map(&:id)).to eq([cmd_a, evt_b].map(&:id))
          expect(messages[1]).to be_a(Tests::SomethingHappened1)
          expect(messages[1].causation_id).to eq(messages[0].id)
          expect(messages[1].correlation_id).to eq(messages[0].correlation_id)

          # Check that initial message cmd_a was ACKed already
          # it won't be claimed again
          # only the new evt_b can be claimed now
          list = []
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.each { |msg, _| list << msg }
            batch.map { |msg, _| [Sourced::Actions::OK, msg] }
          end
          expect(list.map(&:id)).to eq([evt_b.id])
        end
      end

      it 'handles multiple actions' do
        cmd_a = Tests::DoSomething.parse(stream_id: 's1', correlation_id: SecureRandom.uuid, seq: 1, payload: { account_id: 1 })
        evt_b = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 2 })
        evt_c = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 3 })

        reactor1 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::DoSomething, Tests::SomethingHappened1]
          end
        end

        backend.append_to_stream('s1', [cmd_a])
        backend.register_consumer_group('group1')
        now = Time.now

        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.map do |msg, _|
            action1 = Sourced::Actions::AppendNext.new([evt_b])
            action2 = Sourced::Actions::Schedule.new([evt_c], at: now + 10)
            [[action1, action2], msg]
          end
        end

        messages = backend.read_stream('s1')
        expect(messages.map(&:id)).to eq([cmd_a, evt_b].map(&:id))

        Timecop.freeze(now + 11) do
          backend.update_schedule!
          messages = backend.read_stream('s1')
          expect(messages.map(&:id)).to eq([cmd_a, evt_b, evt_c].map(&:id))
        end
      end

      it 'appends messages and reserves them in order of arrival' do
        cmd_a = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        cmd_b = Tests::DoSomething.parse(stream_id: 's2', seq: 1, payload: { account_id: 2 })
        evt_a1 = cmd_a.with(metadata: { tid: 'evt_a1' }).follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd_a.payload.account_id)
        evt_a2 = cmd_a.with(metadata: { tid: 'evt_a2' }).follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd_a.payload.account_id)
        evt_b1 = cmd_b.with(metadata: { tid: 'evt_b1' }).follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd_b.payload.account_id)

        reactor1 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end

        reactor2 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group2')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end

        reactor3 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group3')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end

        backend.register_consumer_group('group1')
        backend.register_consumer_group('group2')
        backend.register_consumer_group('group3')

        backend.stop_consumer_group('group3')

        expect(backend.append_to_stream('s1', [cmd_a, evt_a1, evt_a2])).to be(true)
        expect(backend.append_to_stream('s2', [cmd_b, evt_b1])).to be(true)

        group1_messages = []
        group2_messages = []
        group3_messages = []

        # Test that concurrent consumers for the same group
        # never process events for the same stream
        Sourced.config.executor.start do |t|
          t.spawn do
            backend.reserve_next_for_reactor(reactor1) do |batch, _history|
              sleep 0.01
              batch.each { |msg, _| group1_messages << msg }
              batch.map { |msg, _| [Sourced::Actions::OK, msg] }
            end
          end
          t.spawn do
            backend.reserve_next_for_reactor(reactor1) do |batch, _history|
              batch.each { |msg, _| group1_messages << msg }
              batch.map { |msg, _| [Sourced::Actions::OK, msg] }
            end
          end
        end

        expect(group1_messages).to match_array([evt_b1, evt_a1])

        # Test that separate groups have their own cursors on streams
        backend.reserve_next_for_reactor(reactor2) do |batch, _history|
          batch.each { |msg, _| group2_messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(group2_messages).to eq([evt_a1])

        # Test stopped reactors are ignored
        backend.reserve_next_for_reactor(reactor3) do |batch, _history|
          batch.each { |msg, _| group3_messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(group3_messages).to eq([])

        # Test that NOOP handlers still advance the cursor
        backend.reserve_next_for_reactor(reactor2) do |batch, _history|
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        # Test returning RETRY does not advance the cursor
        backend.reserve_next_for_reactor(reactor2) do |batch, _history|
          Sourced::Actions::RETRY
        end

        # Verify state of groups with stats
        stats = backend.stats

        expect(stats.stream_count).to eq(2)
        expect(stats.max_global_seq).to eq(5)

        expect(stats.groups).to match_array([
          { group_id: 'group1', status: 'active', retry_at: nil, oldest_processed: 2, newest_processed: 5, stream_count: 2 },
          { group_id: 'group2', status: 'active', retry_at: nil, oldest_processed: 3, newest_processed: 3, stream_count: 1 },
          { group_id: 'group3', status: 'stopped', retry_at: nil, oldest_processed: 0, newest_processed: 0, stream_count: 0 }
        ])

        #  Test that reactors with events not in the stream do not advance the cursor
        reactor4 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group4')
          end

          def self.handled_messages
            [Tests::SomethingHappened2]
          end
        end

        backend.register_consumer_group('group4')

        group4_messages = []

        backend.reserve_next_for_reactor(reactor4) do |batch, _history|
          batch.each { |msg, _| group4_messages << msg }
          batch.map { |msg, _| [[], msg] }
        end

        expect(group4_messages).to eq([])

        expect(backend.stats.groups).to match_array([
          { group_id: 'group1', status: 'active', retry_at: nil, oldest_processed: 2, newest_processed: 5, stream_count: 2 },
          { group_id: 'group2', status: 'active', retry_at: nil, oldest_processed: 3, newest_processed: 3, stream_count: 1 },
          { group_id: 'group3', status: 'stopped', retry_at: nil, oldest_processed: 0, newest_processed: 0, stream_count: 0 },
          { group_id: 'group4', status: 'active', retry_at: nil, oldest_processed: 0, newest_processed: 0, stream_count: 0 }
        ])

        # Now append an event that Reactor4 cares about
        evt_a3 = cmd_a.follow_with_seq(Tests::SomethingHappened2, 4, account_id: cmd_a.payload.account_id)
        backend.append_to_stream('s1', [evt_a3])

        backend.reserve_next_for_reactor(reactor4) do |batch, _history|
          batch.each { |msg, _| group4_messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(group4_messages).to eq([evt_a3])

        expect(backend.stats.groups).to match_array([
          { group_id: 'group1', status: 'active', retry_at: nil, oldest_processed: 2, newest_processed: 5, stream_count: 2 },
          { group_id: 'group2', status: 'active', retry_at: nil, oldest_processed: 3, newest_processed: 3, stream_count: 1 },
          { group_id: 'group3', status: 'stopped', retry_at: nil, oldest_processed: 0, newest_processed: 0, stream_count: 0 },
          { group_id: 'group4', status: 'active', retry_at: nil, oldest_processed: 6, newest_processed: 6, stream_count: 1 }
        ])

        #  Test that #reserve_next_for returns next event, or nil
        evt = backend.reserve_next_for_reactor(reactor2) { |batch, _| batch.map { |msg, _| [Sourced::Actions::OK, msg] } }
        expect(evt).to eq(evt_b1)

        evt = backend.reserve_next_for_reactor(reactor2) { |batch, _| batch.map { |msg, _| [Sourced::Actions::OK, msg] } }
        expect(evt).to be(nil)
      end
    end

    describe '#reserve_next_for_reactor and #reset_consumer_group' do
      it 'reserves events again after reset, yields batch with replaying flags' do
        evt_a1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', [evt_a1])

        reactor1 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end

        backend.register_consumer_group('group1')

        messages = []
        replaying = []

        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.each { |msg, is_replaying| messages << msg; replaying << is_replaying }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        # This is a noop since the event is already processed
        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.each { |msg, is_replaying| messages << msg; replaying << is_replaying }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages).to eq([evt_a1])

        # Anything that responds to #consumer_info.group_id
        expect(backend.reset_consumer_group(reactor1)).to be(true)

        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.each { |msg, is_replaying| messages << msg; replaying << is_replaying }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages).to eq([evt_a1, evt_a1])
        expect(replaying).to eq([false, true])
      end
    end

    context '#reserve_next_for_reactor handling Sourced::Actions types' do
      let(:reactor1) do
        Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end
      end

      let(:evt1) do
        Tests::SomethingHappened1.parse(
          stream_id: 's1', 
          seq: 1, 
          correlation_id:,
          metadata: { tid: 'evt1' },
          payload: { account_id: 1 }
        )
      end

      let(:correlation_id) { SecureRandom.uuid }

      before do
        backend.register_consumer_group('group1')

        backend.append_to_stream(evt1.stream_id, [evt1])
      end

      describe 'returning Sourced::Actions::OK' do
        it 'ACKS the message' do
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [Sourced::Actions::OK, msg] }
          end

          expect(backend.stats.groups.first[:newest_processed]).to eq(1)
        end
      end

      describe 'returning Sourced::Actions::RETRY' do
        it 'does not ACK the message' do
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            Sourced::Actions::RETRY
          end

          expect(backend.stats.groups.first[:newest_processed]).to eq(0)
        end
      end

      describe 'returning Sourced::Actions::AppendNext' do
        before do
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [Sourced::Actions::AppendNext.new([Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 2 })]), msg] }
          end
        end

        it 'appends messages to stream and auto-increments seq' do
          events = backend.read_stream('s1')
          expect(events.map(&:seq)).to eq([1, 2])
          expect(events.map(&:payload).map(&:account_id)).to eq([1, 2])
        end

        it 'ACKs processed message' do
          expect(backend.stats.groups.first[:newest_processed]).to eq(1)
        end

        it 'correlates messages and copies metadata' do
          events = backend.read_stream('s1')
          expect(events.map(&:causation_id)).to eq([evt1.id, evt1.id])
          expect(events.map(&:correlation_id)).to eq([correlation_id, correlation_id])
          expect(events.map(&:metadata).map { |m| m[:tid] }).to eq(['evt1', 'evt1'])
        end
      end

      describe 'returning Sourced::Actions::Ack' do
        it 'ACKS the specific message ID passed' do
          evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 3 })
          backend.append_to_stream(evt2.stream_id, [evt2])
          # First message yielded will be evt1
          # but we'll ACK evt2 directly
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [Sourced::Actions::Ack.new(evt2.id), msg] }
          end

          new_messages = []
          # No new messages to fetch now, because we ACKed the last one
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.each { |msg, _| new_messages << msg }; batch.map { |msg, _| [[], msg] }
          end

          expect(new_messages).to eq([])
          expect(backend.stats.groups.first[:newest_processed]).to eq(2)
        end
      end

      describe 'returning Sourced::Actions::AppendAfter' do
        it 'appends messages to stream if sequence do not conflict' do
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message]), msg] }
          end

          events = backend.read_stream('s1')
          expect(events.map(&:seq)).to eq([1, 2])
          expect(events.map(&:payload).map(&:account_id)).to eq([1, 2])
        end

        it 'raises Sourced::ConcurrentAppendError if sequences conflict' do
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 2 })
          expect do
            backend.reserve_next_for_reactor(reactor1) do |batch, _history|
              batch.map { |msg, _| [Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message]), msg] }
            end
          end.to raise_error(Sourced::ConcurrentAppendError)
        end

        it 'does not append messages if #ack_event raises' do
          pending 'how to test that a failed trasaction rolls back appending messages?'
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
          # allow(backend).to receive(:ack_event).and_raise(StandardError)

          expect do
            backend.reserve_next_for_reactor(reactor1) do |batch, _history|
              batch.map { |msg, _| [Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message]), msg] }
            end
          end.to raise_error(StandardError)

          expect(backend.read_stream('s1').map(&:seq)).to eq([1])
        end

        it 'correlates messages and copies metadata' do
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message]), msg] }
          end

          events = backend.read_stream('s1')
          expect(events.map(&:causation_id)).to eq([evt1.id, evt1.id])
          expect(events.map(&:correlation_id)).to eq([correlation_id, correlation_id])
          expect(events.map(&:metadata).map { |m| m[:tid] }).to eq(['evt1', 'evt1'])
        end
      end

      describe 'returning Sourced::Actions::Sync' do
        it 'runs sync work within transaction' do
          worked = false
          work = proc do
            worked = true
          end

          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [Sourced::Actions::Sync.new(work), msg] }
          end

          expect(worked).to be(true)
        end

        it 'acks' do
          work = proc{}

          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [Sourced::Actions::Sync.new(work), msg] }
          end

          expect(backend.stats.groups.first[:newest_processed]).to eq(1)
        end
      end

      describe 'returning empty actions' do
        it 'acks by default' do
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [[], msg] }
          end

          expect(backend.stats.groups.first[:newest_processed]).to eq(1)
        end

        it 'acks if nil action' do
          backend.reserve_next_for_reactor(reactor1) do |batch, _history|
            batch.map { |msg, _| [[nil], msg] }
          end

          expect(backend.stats.groups.first[:newest_processed]).to eq(1)
        end
      end
    end

    describe '#reserve_next_for_reactor with batch_size' do
      let(:reactor1) do
        Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end
      end

      before do
        backend.register_consumer_group('group1')
      end

      it 'fetches and processes multiple messages from the same stream in one call' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 3 })
        backend.append_to_stream('s1', [evt1, evt2, evt3])

        messages = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages.map(&:id)).to eq([evt1, evt2, evt3].map(&:id))
      end

      it 'returns the first message from the batch' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        backend.append_to_stream('s1', [evt1, evt2])

        result = backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(result.id).to eq(evt1.id)
      end

      it 'limits to batch_size messages' do
        evts = 5.times.map do |i|
          Tests::SomethingHappened1.parse(stream_id: 's1', seq: i + 1, payload: { account_id: i })
        end
        backend.append_to_stream('s1', evts)

        messages = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 3) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages.size).to eq(3)
        expect(messages.map(&:id)).to eq(evts[0..2].map(&:id))

        # Next batch picks up remaining messages
        messages2 = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 3) do |batch, _history|
          batch.each { |msg, _| messages2 << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages2.size).to eq(2)
        expect(messages2.map(&:id)).to eq(evts[3..4].map(&:id))
      end

      it 'ACKs all messages on success' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        backend.append_to_stream('s1', [evt1, evt2])

        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        # Nothing left to process
        messages = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages).to be_empty
      end

      it 'returns RETRY for all-or-nothing retry' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 3 })
        backend.append_to_stream('s1', [evt1, evt2, evt3])

        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |_batch, _history|
          Sourced::Actions::RETRY
        end

        # No messages were ACKed, all should be available on next call
        remaining = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.each { |msg, _| remaining << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(remaining.map(&:id)).to eq([evt1, evt2, evt3].map(&:id))
      end

      it 'releases offset without ACK when RETRY returned' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        backend.append_to_stream('s1', [evt1, evt2])

        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |_batch, _history|
          Sourced::Actions::RETRY
        end

        # All messages should still be available
        messages = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages.map(&:id)).to eq([evt1, evt2].map(&:id))
      end

      it 'only fetches messages matching handled_messages types' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened2.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 3 })
        backend.append_to_stream('s1', [evt1, evt2, evt3])

        messages = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        # reactor1 only handles SomethingHappened1, not SomethingHappened2
        expect(messages.map(&:id)).to eq([evt1, evt3].map(&:id))
      end

      it 'sets replaying flag correctly per message' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        backend.append_to_stream('s1', [evt1, evt2])

        # Process both messages first
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        # Reset consumer group to trigger replaying
        backend.reset_consumer_group(reactor1)

        # Add a new message (not replaying)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 3 })
        backend.append_to_stream('s1', [evt3])

        replaying_flags = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          replaying_flags = batch.map { |_, is_replaying| is_replaying }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        # evt1 and evt2 are replaying (global_seq <= highest_global_seq), evt3 is not
        expect(replaying_flags).to eq([true, true, false])
      end

      it 'handles AppendNext actions within a batch' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        backend.append_to_stream('s1', [evt1, evt2])

        new_cmd = Tests::DoSomething.parse(stream_id: 's2', payload: { account_id: 99 })

        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.map.with_index do |(msg, _), idx|
            if idx == 0
              [Sourced::Actions::AppendNext.new([new_cmd]), msg]
            else
              [Sourced::Actions::OK, msg]
            end
          end
        end

        # Both messages processed, and the AppendNext command was appended
        expect(backend.read_stream('s2').map(&:id)).to eq([new_cmd.id])
      end

      it 'returns nil when there are no messages to process' do
        result = backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(result).to be_nil
      end

      it 'handles a single message in batch mode' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', [evt1])

        messages = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages.map(&:id)).to eq([evt1.id])
      end

      it 'releases offset when block returns empty action_pairs' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', [evt1])

        # Return empty action_pairs — nothing to ACK
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |_batch, _history|
          []
        end

        # Offset should have been released, so the message is available again
        messages = []
        backend.reserve_next_for_reactor(reactor1, batch_size: 10) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages.map(&:id)).to eq([evt1.id])
      end
    end

    describe '#reserve_next_for_reactor with_history' do
      let(:reactor1) do
        Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end
      end

      before do
        backend.register_consumer_group('group1')
      end

      it 'yields history containing all stream messages when with_history: true' do
        cmd = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 3 })
        backend.append_to_stream('s1', [cmd, evt1, evt2])

        yielded_history = nil
        backend.reserve_next_for_reactor(reactor1, batch_size: 10, with_history: true) do |batch, history|
          yielded_history = history
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        # History should include ALL stream messages (cmd + evt1 + evt2), not just batch
        expect(yielded_history).not_to be_nil
        expect(yielded_history.map(&:id)).to eq([cmd, evt1, evt2].map(&:id))
      end

      it 'yields nil history when with_history: false (default)' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', [evt1])

        yielded_history = :not_set
        backend.reserve_next_for_reactor(reactor1) do |batch, history|
          yielded_history = history
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(yielded_history).to be_nil
      end

      it 'history does not include messages from other streams' do
        evt_s1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt_s2 = Tests::SomethingHappened1.parse(stream_id: 's2', seq: 1, payload: { account_id: 2 })
        backend.append_to_stream('s1', [evt_s1])
        backend.append_to_stream('s2', [evt_s2])

        yielded_history = nil
        backend.reserve_next_for_reactor(reactor1, with_history: true) do |batch, history|
          yielded_history = history
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(yielded_history.map(&:stream_id).uniq).to eq([yielded_history.first.stream_id])
      end
    end

    describe '#reserve_next_for_reactor with retry_at' do
      it 'does not fetch events until retry_at is up' do
        now = Time.now

        evt_a1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', [evt_a1])

        reactor1 = Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end

        backend.register_consumer_group('group1')
        backend.updating_consumer_group('group1') do |gr|
          gr.retry(now + 4)
        end

        messages = []

        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages.any?).to be(false)

        backend.updating_consumer_group('group1') do |gr|
          gr.retry(now - 1)
        end

        backend.reserve_next_for_reactor(reactor1) do |batch, _history|
          batch.each { |msg, _| messages << msg }
          batch.map { |msg, _| [Sourced::Actions::OK, msg] }
        end

        expect(messages.any?).to be(true)
      end
    end

    describe '#ack_on' do
      let(:reactor) do
        Class.new do
          def self.consumer_info
            Sourced::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_messages
            [Tests::SomethingHappened1]
          end
        end
      end

      let(:evt1) { Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 }) }
      let(:evt2) { Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 1 }) }

      before do
        backend.append_to_stream('s1', [evt1, evt2])
      end

      it 'advances a group_id/stream_id offset if no exception' do
        backend.ack_on(reactor.consumer_info.group_id, evt1.id) { true }

        backend.reserve_next_for_reactor(reactor) { |batch, _| batch.map { |msg, _| [Sourced::Actions::OK, msg] } }

        expect(backend.stats.groups.first[:oldest_processed]).to eq(2)
      end

      it 'does not advance offset if exception' do
        begin
          backend.ack_on(reactor.consumer_info.group_id, evt1.id) do
            raise RuntimeError
          end
        rescue RuntimeError
        end

        expect(backend.stats.groups.size).to eq(0)
      end

    end

    describe '#read_correlation_batch' do
      specify 'given an event ID, it returns the list of correlated events' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = cmd1.follow_with_seq(Tests::SomethingHappened1, 2, cmd1.payload)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 1 })
        evt2 = cmd1.follow_with_seq(Tests::SomethingHappened1, 4, cmd1.payload)

        expect(backend.append_to_stream('s1', [cmd1, evt1, evt2, evt3])).to be(true)

        events = backend.read_correlation_batch(evt2.id)
        expect(events).to eq([cmd1, evt1, evt2])
      end

      it 'returns empty list if no event found' do
        no = SecureRandom.uuid
        events = backend.read_correlation_batch(no)
        expect(events.empty?).to be(true)
      end
    end

    describe '#append_to_stream' do
      it 'accepts a single event' do
        evt = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        expect(backend.append_to_stream('s1', evt)).to be(true)
        
        events = backend.read_stream('s1')
        expect(events.size).to eq(1)
        expect(events.first).to eq(evt)
      end

      it 'accepts an array of events' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        expect(backend.append_to_stream('s1', [evt1, evt2])).to be(true)
        
        events = backend.read_stream('s1')
        expect(events.size).to eq(2)
        expect(events).to eq([evt1, evt2])
      end

      it 'handles empty array' do
        expect(backend.append_to_stream('s1', [])).to be(false)
        events = backend.read_stream('s1')
        expect(events).to eq([])
      end

      it 'fails if duplicate [stream_id, seq]' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', evt1)

        expect do
          backend.append_to_stream('s1', evt2)
        end.to raise_error(Sourced::ConcurrentAppendError)
      end
    end

    describe '#recent_streams' do
      it 'returns streams ordered by most recent activity first' do
        now = Time.now

        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's2', seq: 10, payload: { account_id: 1 })

        # Create first stream
        backend.append_to_stream('s1', [evt1])
        
        # Create second stream 5 seconds later
        Timecop.freeze(now + 5) do
          backend.append_to_stream('s2', [evt2])
        end

        streams = backend.recent_streams(limit: 20)
        
        # Should be ordered by most recent first (s2, then s1)
        expect(streams.map(&:stream_id)).to eq(['s2', 's1'])
        expect(streams.map(&:seq)).to eq([10, 1])
        expect(streams.first.updated_at).to be_a(Time)
        expect(streams.size).to eq(2)
      end

      it 'respects the limit parameter' do
        # Create 5 streams with different timestamps
        5.times do |i|
          evt = Tests::SomethingHappened1.parse(stream_id: "s#{i}", seq: 1, payload: { account_id: 1 })
          Timecop.freeze(Time.now + i) do
            backend.append_to_stream("s#{i}", [evt])
          end
        end

        # Test limit smaller than total streams
        streams = backend.recent_streams(limit: 3)
        expect(streams.size).to eq(3)
        expect(streams.map(&:stream_id)).to eq(['s4', 's3', 's2']) # Most recent first

        # Test limit larger than total streams
        streams = backend.recent_streams(limit: 10)
        expect(streams.size).to eq(5) # Should return all 5 streams
        expect(streams.map(&:stream_id)).to eq(['s4', 's3', 's2', 's1', 's0'])

        # Test limit of 1
        streams = backend.recent_streams(limit: 1)
        expect(streams.size).to eq(1)
        expect(streams.first.stream_id).to eq('s4') # Most recent
      end

      it 'uses default limit when not specified' do
        # Create more streams than the default limit (10)
        12.times do |i|
          evt = Tests::SomethingHappened1.parse(stream_id: "s#{i}", seq: 1, payload: { account_id: 1 })
          backend.append_to_stream("s#{i}", [evt])
        end

        streams = backend.recent_streams # No limit specified
        expect(streams.size).to eq(10) # Should default to 10
      end

      it 'handles edge cases for limit parameter' do
        # Create a few streams
        3.times do |i|
          evt = Tests::SomethingHappened1.parse(stream_id: "s#{i}", seq: 1, payload: { account_id: 1 })
          backend.append_to_stream("s#{i}", [evt])
        end

        # Test limit of 0
        streams = backend.recent_streams(limit: 0)
        expect(streams.size).to eq(0)

        # Test very large limit
        streams = backend.recent_streams(limit: 1000)
        expect(streams.size).to eq(3) # Should return all available streams
      end

      it 'validates input parameters' do
        # Test negative limit
        expect {
          backend.recent_streams(limit: -1)
        }.to raise_error(ArgumentError, "limit must be a positive integer")

        expect {
          backend.recent_streams(limit: -5)
        }.to raise_error(ArgumentError, "limit must be a positive integer")
      end
    end

    describe '#read_stream' do
      it 'reads full event stream in order' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = cmd1.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd1.payload.account_id)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's2', seq: 4, payload: { account_id: 1 })
        expect(backend.append_to_stream('s1', [evt1, evt2])).to be(true)
        expect(backend.append_to_stream('s2', [evt3])).to be(true)
        events = backend.read_stream('s1')
        expect(events).to eq([evt1, evt2])
      end

      it ':upto and :after' do
        e1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        e2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        e3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 2 })
        expect(backend.append_to_stream('s1', [e1, e2, e3])).to be(true)
        events = backend.read_stream('s1', upto: 2)
        expect(events).to eq([e1, e2])

        events = backend.read_stream('s1', after: 1)
        expect(events).to eq([e2, e3])
      end
    end

    describe '#updating_consumer_group' do
      specify '#retry_at(Time)' do
        later = Time.now + 10
        counts = []
        backend.register_consumer_group('group1')
        backend.updating_consumer_group('group1') do |group|
          counts << group.error_context[:retry_count]
          group.retry(later, retry_count: 1)
        end
        backend.updating_consumer_group('group1') do |group|
          counts << group.error_context[:retry_count]
          group.retry(later)
        end
        gr = backend.stats.groups.first
        expect(gr[:group_id]).to eq('group1')
        expect(gr[:status]).to eq('active')
        expect(gr[:retry_at]).to eq(later)
        expect(counts).to eq([nil, 1])
      end

      specify '#stop(error)' do
        backend.register_consumer_group('group1')
        backend.updating_consumer_group('group1') do |group|
          group.stop(StandardError.new('boom'))
        end

        gr = backend.stats.groups.first
        expect(gr[:group_id]).to eq('group1')
        expect(gr[:status]).to eq('stopped')

        backend.start_consumer_group('group1')
        gr = backend.stats.groups.first
        expect(gr[:group_id]).to eq('group1')
        expect(gr[:status]).to eq('active')
      end
    end

    describe '#notifier' do
      it 'returns an object responding to subscribe, publish, notify_new_messages, notify_reactor_resumed, start, stop' do
        n = backend.notifier
        expect(n).to respond_to(:subscribe)
        expect(n).to respond_to(:publish)
        expect(n).to respond_to(:notify_new_messages)
        expect(n).to respond_to(:notify_reactor_resumed)
        expect(n).to respond_to(:start)
        expect(n).to respond_to(:stop)
      end

      it 'returns the same instance on repeated calls' do
        expect(backend.notifier).to be(backend.notifier)
      end
    end
  end

  class Migrator
    attr_reader :migration_version, :table_prefix

    def initialize(table_prefix: 'sors', root_dir: File.expand_path('../..', __dir__))
      @table_prefix = table_prefix
      @root_dir = root_dir
      @migration_version = "[#{ActiveRecord::VERSION::STRING.to_f}]"
      @migdir = File.join(@root_dir, 'spec', 'db', 'migrate')
      @migfilename = File.join(@migdir, 'create_sors_tables.rb')
    end

    def up
      return if Sourced::Backends::ActiveRecordBackend.installed?

      migfile = File.read(File.join(@root_dir, 'lib', 'sors', 'rails', 'templates', 'create_sors_tables.rb.erb'))
      migcontent = ERB.new(migfile).result(binding)
      FileUtils.mkdir_p(@migdir)
      File.write(@migfilename, migcontent)
      require @migfilename.sub('.rb', '')
      CreateSorsTables.new.change
    end

    def down
      Sourced::Backends::ActiveRecordBackend.uninstall!
      File.delete(@migfilename) if File.exist?(@migfilename)
    end
  end
end
