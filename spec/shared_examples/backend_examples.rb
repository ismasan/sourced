# frozen_string_literal: true

module BackendExamples
  module Tests
    DoSomething = Sors::Message.define('tests.do_something') do
      attribute :account_id, Integer
    end
    SomethingHappened1 = Sors::Message.define('tests.something_happened1') do
      attribute :account_id, Integer
    end
    SomethingHappened2 = Sors::Message.define('tests.something_happened2') do
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
        expect(Sors::Backends::ActiveRecordBackend::EventRecord.order(global_seq: :asc).pluck(:global_seq))
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
        Sors::Configuration::BackendInterface.parse(backend)
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

    describe '#append_to_stream and #reserve_next_for_reactor' do
      it 'schedules messages and reserves them in order of arrival' do
        cmd_a = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        cmd_b = Tests::DoSomething.parse(stream_id: 's2', seq: 1, payload: { account_id: 2 })
        evt_a1 = cmd_a.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd_a.payload.account_id)
        evt_a2 = cmd_a.follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd_a.payload.account_id)
        evt_b1 = cmd_b.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd_b.payload.account_id)

        reactor1 = Class.new do
          def self.consumer_info
            Sors::Consumer::ConsumerInfo.new(group_id: 'group1')
          end

          def self.handled_events
            [Tests::SomethingHappened1]
          end
        end

        reactor2 = Class.new do
          def self.consumer_info
            Sors::Consumer::ConsumerInfo.new(group_id: 'group2')
          end

          def self.handled_events
            [Tests::SomethingHappened1]
          end
        end

        expect(backend.append_to_stream('s1', [cmd_a, evt_a1, evt_a2])).to be(true)
        expect(backend.append_to_stream('s2', [cmd_b, evt_b1])).to be(true)

        group1_messages = []
        group2_messages = []

        #  Test that concurrent consumers for the same group
        #  never process events for the same stream
        Sync do |task|
          task.async do
            backend.reserve_next_for_reactor(reactor1) do |msg|
              sleep 0.01
              group1_messages << msg
            end
          end
          task.async do
            backend.reserve_next_for_reactor(reactor1) do |msg|
              group1_messages << msg
            end
          end
        end

        expect(group1_messages).to eq([evt_b1, evt_a1])

        # Test that separate groups have their own cursors on streams
        backend.reserve_next_for_reactor(reactor2) do |msg|
          group2_messages << msg
        end

        expect(group2_messages).to eq([evt_a1])

        # Test that NOOP handlers still advance the cursor
        backend.reserve_next_for_reactor(reactor2) { |_msg| }
        #
        # Verify state of groups with stats
        stats = backend.stats

        expect(stats.stream_count).to eq(2)
        expect(stats.max_global_seq).to eq(5)
        expect(stats.groups).to match_array([
          { group_id: 'group2', oldest_processed: 3, newest_processed: 3, stream_count: 1 },
          { group_id: 'group1', oldest_processed: 2, newest_processed: 5, stream_count: 2 }
        ])

        # Test that reactors with events not in the stream do not advance the cursor
        reactor3 = Class.new do
          def self.consumer_info
            Sors::Consumer::ConsumerInfo.new(group_id: 'group3')
          end

          def self.handled_events
            [Tests::SomethingHappened2]
          end
        end

        group3_messages = []

        backend.reserve_next_for_reactor(reactor3) do |msg|
          group3_messages << msg
        end

        expect(group3_messages).to eq([])

        expect(backend.stats.groups).to match_array([
          { group_id: 'group2', oldest_processed: 3, newest_processed: 3, stream_count: 1 },
          { group_id: 'group1', oldest_processed: 2, newest_processed: 5, stream_count: 2 }
        ])

        # Now append an event that Reactor3 cares about
        evt_a3 = cmd_a.follow_with_seq(Tests::SomethingHappened2, 4, account_id: cmd_a.payload.account_id)
        backend.append_to_stream('s1', [evt_a3])

        backend.reserve_next_for_reactor(reactor3) do |msg|
          group3_messages << msg
        end

        expect(group3_messages).to eq([evt_a3])

        expect(backend.stats.groups).to match_array([
          { group_id: 'group1', oldest_processed: 2, newest_processed: 5, stream_count: 2 },
          { group_id: 'group2', oldest_processed: 3, newest_processed: 3, stream_count: 1 },
          { group_id: 'group3', oldest_processed: 6, newest_processed: 6, stream_count: 1 },
        ])

        #  Test that #reserve_next_for returns next event, or nil
        evt = backend.reserve_next_for_reactor(reactor2) { true }
        expect(evt).to eq(evt_b1)

        evt = backend.reserve_next_for_reactor(reactor2) { true }
        expect(evt).to be(nil)
      end
    end

    describe '#append_to_stream, #read_event_batch' do
      it 'reads event batch by causation_id' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = cmd1.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd1.payload.account_id)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 4, payload: { account_id: 1 })
        expect(backend.append_to_stream('s1', [evt1, evt2, evt3])).to be(true)

        events = backend.read_event_batch(cmd1.id)
        expect(events).to eq([evt1, evt2])
      end

      it 'fails if duplicate [stream_id, seq]' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_to_stream('s1', [evt1])

        expect do
          backend.append_to_stream('s1', [evt2])
        end.to raise_error(Sors::ConcurrentAppendError)
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
      return if Sors::Backends::ActiveRecordBackend.installed?

      migfile = File.read(File.join(@root_dir, 'lib', 'sors', 'rails', 'templates', 'create_sors_tables.rb.erb'))
      migcontent = ERB.new(migfile).result(binding)
      FileUtils.mkdir_p(@migdir)
      File.write(@migfilename, migcontent)
      require @migfilename.sub('.rb', '')
      CreateSorsTables.new.change
    end

    def down
      Sors::Backends::ActiveRecordBackend.uninstall!
      File.delete(@migfilename) if File.exist?(@migfilename)
    end
  end
end
