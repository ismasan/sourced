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
        expect(backend.read_event_stream('s1').any?).to be(false)
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
        expect(backend.read_event_stream('s1')).to eq([msg0])

        Timecop.freeze(now + 3) do
          backend.update_schedule!
          expect(backend.read_event_stream('s1').map(&:id)).to eq([msg0, msg1, msg2].map(&:id))
        end

        Timecop.freeze(now + 11) do
          backend.update_schedule!
          messages = backend.read_event_stream('s1')
          expect(messages.map(&:id)).to eq([msg0, msg1, msg2, msg3].map(&:id))
          expect(messages.map(&:seq)).to eq([1, 2, 3, 4])
        end
      end

      it 'blocks concurrent workers from selecting the same messages' do
        now = Time.now - 10
        cmd1 = Tests::DoSomething.parse(stream_id: 'as1', payload: { account_id: 1 })
        cmd2 = Tests::DoSomething.parse(stream_id: 'as1', payload: { account_id: 1 })
        backend.schedule_messages([cmd1, cmd2], at: now)
        Sourced.config.executor.start do |t|
          2.times.each do
            t.spawn do
              backend.update_schedule!
            end
          end
        end

        expect(backend.read_event_stream('as1').map(&:id)).to eq([cmd1, cmd2].map(&:id))
      end
    end

    describe '#schedule_commands and #next_command' do
      it 'schedules command and fetches it back' do
        cmd = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        backend.schedule_commands([cmd], group_id: 'reactor1')
        cmd2 = backend.next_command
        expect(cmd2).to eq(cmd)
      end

      it 'schedules command and reserves it' do
        cmd = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        backend.schedule_commands([cmd], group_id: 'reactor1')
        cmd2 = nil
        backend.next_command do |c|
          cmd2 = c
        end
        expect(cmd2).to eq(cmd)
        expect(backend.next_command).to be(nil)
      end

      it 'skips command if target reactor is stopped' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        cmd2 = Tests::DoSomething.parse(stream_id: 's2', payload: { account_id: 1 })
        backend.schedule_commands([cmd1], group_id: 'reactor1')
        backend.schedule_commands([cmd2], group_id: 'reactor2')

        backend.stop_consumer_group('reactor1')

        cmds = []
        backend.next_command do |c|
          cmds << c
        end
        backend.next_command do |c|
          cmds << c
        end
        expect(cmds).to eq([cmd2])
        expect(backend.next_command).to be(nil)
      end

      it 'does not delete command if processing raises' do
        cmd = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        backend.schedule_commands([cmd], group_id: 'reactor1')
        begin
          backend.next_command do |_c|
            raise 'nope!'
          end
        rescue StandardError
          nil
        end
        expect(backend.next_command).to eq(cmd)
      end

      it 'does not delete command if processing return false' do
        cmd = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        backend.schedule_commands([cmd], group_id: 'reactor1')
        backend.next_command do |_c|
          false
        end
        expect(backend.next_command).to eq(cmd)
      end

      it 'blocks concurrent workers from processing the same command' do
        now = Time.now - 10
        cmd1 = Tests::DoSomething.parse(stream_id: 'as1', created_at: now, payload: { account_id: 1 })
        cmd2 = Tests::DoSomething.parse(stream_id: 'as2', created_at: now + 5, payload: { account_id: 1 })
        backend.schedule_commands([cmd1, cmd2], group_id: 'reactor1')
        results = Concurrent::Array.new
        Sourced.config.executor.start do |t|
          2.times.each do
            t.spawn do
              backend.next_command do |c|
                sleep 0.01
                results << c
              end
            end
          end
        end
        expect(results).to match_array([cmd2, cmd1])
      end

      it 'processes commands at a later time' do
        now = Time.now
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', created_at: now - 1, payload: { account_id: 1 })
        cmd2 = Tests::DoSomething.parse(stream_id: 's1', created_at: now + 10, payload: { account_id: 1 })
        backend.schedule_commands([cmd1, cmd2], group_id: 'reactor1')

        results = []
        backend.next_command do |c|
          results << c
        end
        backend.next_command do |c|
          results << c
        end
        expect(results).to eq([cmd1])

        Timecop.freeze(now + 15) do
          backend.next_command do |c|
            results << c
          end
        end

        expect(results).to eq([cmd1, cmd2])
      end

      it 'linearizes commands for the same stream' do
        now = Time.now
        cmd1 = Tests::DoSomething.parse(stream_id: 'ss1', created_at: now - 11, payload: { account_id: 1 })
        cmd2 = Tests::DoSomething.parse(stream_id: 'ss1', created_at: now - 10, payload: { account_id: 1 })
        cmd3 = Tests::DoSomething.parse(stream_id: 'ss2', created_at: now - 5, payload: { account_id: 1 })
        backend.schedule_commands([cmd1, cmd2, cmd3], group_id: 'reactor1')
        results = Concurrent::Array.new

        Sourced.config.executor.start do |t|
          2.times.each do
            t.spawn do
              backend.next_command do |c|
                sleep 0.01
                results << c
              end
            end
          end
        end

        expect(results).to match_array([cmd1, cmd3])

        backend.next_command do |c|
          results << c
        end
        expect(results).to match_array([cmd1, cmd3, cmd2])
      end
    end

    describe '#append_next_to_stream' do
      it 'appends single event to stream incrementing :seq automatically' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', evt1)
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 2 })
        expect(backend.append_next_to_stream('s1', evt2)).to be(true)
        events = backend.read_event_stream('s1')
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
        events = backend.read_event_stream('s1')
        expect(events.map(&:stream_id)).to eq(['s1', 's1', 's1', 's1'])
        expect(events.map(&:seq)).to eq([1, 2, 3, 4])
      end

      it 'handles empty array' do
        expect(backend.append_next_to_stream('s1', [])).to be(true)
        events = backend.read_event_stream('s1')
        expect(events).to eq([])
      end

      it 'creates new stream when appending to non-existent stream with array' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 2 })
        
        expect(backend.append_next_to_stream('s1', [evt1, evt2])).to be(true)
        events = backend.read_event_stream('s1')
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

        backend.reserve_next_for_reactor(reactor1) do |msg|
          messages << msg
          Sourced::Actions::OK
        end
        backend.reserve_next_for_reactor(reactor1) do |msg|
          messages << msg
          Sourced::Actions::OK
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

        backend.reserve_next_for_reactor(reactor1) do |msg|
          Sourced::Actions::Schedule.new([evt_b], at: now + 10)
        end

        expect(backend.read_event_stream('s1')).to eq([cmd_a])

        Timecop.freeze(now + 11) do
          backend.update_schedule!
          messages = backend.read_event_stream('s1')
          expect(messages.map(&:id)).to eq([cmd_a, evt_b].map(&:id))
          expect(messages[1]).to be_a(Tests::SomethingHappened1)
          expect(messages[1].causation_id).to eq(messages[0].id)
          expect(messages[1].correlation_id).to eq(messages[0].correlation_id)

          # Check that initial message cmd_a was ACKed already
          # it won't be claimed again
          # only the new evt_b can be claimed now
          list = []
          backend.reserve_next_for_reactor(reactor1) do |msg|
            list << msg
            Sourced::Actions::OK
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

        backend.reserve_next_for_reactor(reactor1) do |msg|
          action1 = Sourced::Actions::AppendNext.new([evt_b])
          action2 = Sourced::Actions::Schedule.new([evt_c], at: now + 10)
          [action1, action2]
        end

        messages = backend.read_event_stream('s1')
        expect(messages.map(&:id)).to eq([cmd_a, evt_b].map(&:id))

        Timecop.freeze(now + 11) do
          backend.update_schedule!
          messages = backend.read_event_stream('s1')
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
            backend.reserve_next_for_reactor(reactor1) do |msg|
              sleep 0.01
              group1_messages << msg
              Sourced::Actions::OK
            end
          end
          t.spawn do
            backend.reserve_next_for_reactor(reactor1) do |msg|
              group1_messages << msg
              Sourced::Actions::OK
            end
          end
        end

        expect(group1_messages).to eq([evt_b1, evt_a1])

        # Test that separate groups have their own cursors on streams
        backend.reserve_next_for_reactor(reactor2) do |msg|
          group2_messages << msg
          Sourced::Actions::OK
        end

        expect(group2_messages).to eq([evt_a1])

        # Test stopped reactors are ignored
        backend.reserve_next_for_reactor(reactor3) do |msg|
          group3_messages << msg
          Sourced::Actions::OK
        end

        expect(group3_messages).to eq([])

        # Test that NOOP handlers still advance the cursor
        backend.reserve_next_for_reactor(reactor2) { |_msg| Sourced::Actions::OK }

        # Test returning RETRY does not advance the cursor
        backend.reserve_next_for_reactor(reactor2) { |_msg| Sourced::Actions::RETRY }

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

        backend.reserve_next_for_reactor(reactor4) do |msg|
          group4_messages << msg
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

        backend.reserve_next_for_reactor(reactor4) do |msg|
          group4_messages << msg
          Sourced::Actions::OK
        end

        expect(group4_messages).to eq([evt_a3])

        expect(backend.stats.groups).to match_array([
          { group_id: 'group1', status: 'active', retry_at: nil, oldest_processed: 2, newest_processed: 5, stream_count: 2 },
          { group_id: 'group2', status: 'active', retry_at: nil, oldest_processed: 3, newest_processed: 3, stream_count: 1 },
          { group_id: 'group3', status: 'stopped', retry_at: nil, oldest_processed: 0, newest_processed: 0, stream_count: 0 },
          { group_id: 'group4', status: 'active', retry_at: nil, oldest_processed: 6, newest_processed: 6, stream_count: 1 }
        ])

        #  Test that #reserve_next_for returns next event, or nil
        evt = backend.reserve_next_for_reactor(reactor2) { Sourced::Actions::OK }
        expect(evt).to eq(evt_b1)

        evt = backend.reserve_next_for_reactor(reactor2) { Sourced::Actions::OK }
        expect(evt).to be(nil)
      end
    end

    describe '#reserve_next_for_reactor and #reset_consumer_group' do
      it 'reserves events again after reset, yields replaying boolean' do
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

        backend.reserve_next_for_reactor(reactor1) do |msg, is_replaying|
          messages << msg
          replaying << is_replaying
          Sourced::Actions::OK
        end

        # This is a noop since the event is already processed
        backend.reserve_next_for_reactor(reactor1) do |msg, is_replaying|
          messages << msg
          replaying << is_replaying
          Sourced::Actions::OK
        end

        expect(messages).to eq([evt_a1])

        expect(backend.reset_consumer_group('group1')).to be(true)

        backend.reserve_next_for_reactor(reactor1) do |msg, is_replaying|
          messages << msg
          replaying << is_replaying
          Sourced::Actions::OK
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
          backend.reserve_next_for_reactor(reactor1) do |_msg|
            Sourced::Actions::OK
          end

          expect(backend.stats.groups.first[:newest_processed]).to eq(1)
        end
      end

      describe 'returning Sourced::Actions::RETRY' do
        it 'does not ACK the message' do
          backend.reserve_next_for_reactor(reactor1) do |_msg|
            Sourced::Actions::RETRY
          end

          expect(backend.stats.groups.first[:newest_processed]).to eq(0)
        end
      end

      describe 'returning Sourced::Actions::AppendNext' do
        before do
          backend.reserve_next_for_reactor(reactor1) do |_msg|
            Sourced::Actions::AppendNext.new([Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 2 })])
          end
        end

        it 'appends messages to stream and auto-increments seq' do
          events = backend.read_event_stream('s1')
          expect(events.map(&:seq)).to eq([1, 2])
          expect(events.map(&:payload).map(&:account_id)).to eq([1, 2])
        end

        it 'ACKs processed message' do
          expect(backend.stats.groups.first[:newest_processed]).to eq(1)
        end

        it 'correlates messages and copies metadata' do
          events = backend.read_event_stream('s1')
          expect(events.map(&:causation_id)).to eq([evt1.id, evt1.id])
          expect(events.map(&:correlation_id)).to eq([correlation_id, correlation_id])
          expect(events.map(&:metadata).map { |m| m[:tid] }).to eq(['evt1', 'evt1'])
        end
      end

      describe 'returning Sourced::Actions::AppendAfter' do
        it 'appends messages to stream if sequence do not conflict' do
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
          backend.reserve_next_for_reactor(reactor1) do |_msg|
            Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message])
          end

          events = backend.read_event_stream('s1')
          expect(events.map(&:seq)).to eq([1, 2])
          expect(events.map(&:payload).map(&:account_id)).to eq([1, 2])
        end

        it 'raises Sourced::ConcurrentAppendError if sequences conflict' do
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 2 })
          expect do
            backend.reserve_next_for_reactor(reactor1) do |_msg|
              Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message])
            end
          end.to raise_error(Sourced::ConcurrentAppendError)
        end

        it 'does not append messages if #ack_event raises' do
          pending 'how to test that a failed trasaction rolls back appending messages?'
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
          # allow(backend).to receive(:ack_event).and_raise(StandardError)

          expect do
            backend.reserve_next_for_reactor(reactor1) do |_msg|
              Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message])
            end
          end.to raise_error(StandardError)

          expect(backend.read_event_stream('s1').map(&:seq)).to eq([1])
        end

        it 'correlates messages and copies metadata' do
          new_message = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
          backend.reserve_next_for_reactor(reactor1) do |_msg|
            Sourced::Actions::AppendAfter.new(new_message.stream_id, [new_message])
          end

          events = backend.read_event_stream('s1')
          expect(events.map(&:causation_id)).to eq([evt1.id, evt1.id])
          expect(events.map(&:correlation_id)).to eq([correlation_id, correlation_id])
          expect(events.map(&:metadata).map { |m| m[:tid] }).to eq(['evt1', 'evt1'])
        end
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

        backend.reserve_next_for_reactor(reactor1) do |msg|
          messages << msg
          Sourced::Actions::OK
        end

        expect(messages.any?).to be(false)

        backend.updating_consumer_group('group1') do |gr|
          gr.retry(now - 1)
        end

        backend.reserve_next_for_reactor(reactor1) do |msg|
          messages << msg
          Sourced::Actions::OK
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

        backend.reserve_next_for_reactor(reactor) { Sourced::Actions::OK }

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

      it 'raises exception if concurrently processed by the same group' do
        expect do
          Sourced.config.executor.start do |t|
            t.spawn do
              backend.ack_on(reactor.consumer_info.group_id, evt1.id) { sleep 0.01 }
            end
            t.spawn do
              backend.ack_on(reactor.consumer_info.group_id, evt2.id) { true }
            end
          end
        end.to raise_error(Sourced::ConcurrentAckError)
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
        
        events = backend.read_event_stream('s1')
        expect(events.size).to eq(1)
        expect(events.first).to eq(evt)
      end

      it 'accepts an array of events' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        expect(backend.append_to_stream('s1', [evt1, evt2])).to be(true)
        
        events = backend.read_event_stream('s1')
        expect(events.size).to eq(2)
        expect(events).to eq([evt1, evt2])
      end

      it 'handles empty array' do
        expect(backend.append_to_stream('s1', [])).to be(false)
        events = backend.read_event_stream('s1')
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

    describe '#read_event_stream' do
      it 'reads full event stream in order' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = cmd1.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd1.payload.account_id)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's2', seq: 4, payload: { account_id: 1 })
        expect(backend.append_to_stream('s1', [evt1, evt2])).to be(true)
        expect(backend.append_to_stream('s2', [evt3])).to be(true)
        events = backend.read_event_stream('s1')
        expect(events).to eq([evt1, evt2])
      end

      it ':upto and :after' do
        e1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        e2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        e3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 2 })
        expect(backend.append_to_stream('s1', [e1, e2, e3])).to be(true)
        events = backend.read_event_stream('s1', upto: 2)
        expect(events).to eq([e1, e2])

        events = backend.read_event_stream('s1', after: 1)
        expect(events).to eq([e2, e3])
      end
    end

    describe '#pubsub' do
      it 'publishes and subscribes to events' do
        channel1 = backend.pubsub.subscribe('test_channel')
        received = []

        Sourced.config.executor.start do |t|
          t.spawn do
            channel1.start do |event, _channel|
              received << event
              throw :stop if received.size == 2
            end
          end
          t.spawn do
            e1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
            e2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
            backend.pubsub.publish('test_channel', e1)
            backend.pubsub.publish('test_channel', e2)
          end
        end

        expect(received.map(&:type)).to eq(%w[tests.something_happened1 tests.something_happened1])
        expect(received.map(&:seq)).to eq([1, 2])
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
