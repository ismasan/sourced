# frozen_string_literal: true

module BackendExamples
  module Tests
    DoSomething = Sors::Message.define('tests.do_something') do
      attribute :account_id, Integer
    end
    SomethingHappened1 = Sors::Message.define('tests.something_happened1') do
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
        backend.append_events([evt1, evt2, evt3])
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

    describe '#schedule_commands and #reserve_next' do
      it 'schedules commands and reserves them in order of arrival' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        cmd2 = Tests::DoSomething.parse(stream_id: 's2', payload: { account_id: 1 })
        expect(backend.schedule_commands([cmd1, cmd2])).to be(true)

        streams = []
        reserved_command = backend.reserve_next do |cmd|
          streams << cmd.stream_id
        end

        expect(reserved_command.id).to eq(cmd1.id)
        expect(streams).to eq(%w[s1])

        backend.reserve_next do |cmd|
          streams << cmd.stream_id
        end

        expect(streams).to eq(%w[s1 s2])

        backend.reserve_next do |cmd|
          streams << cmd.stream_id
        end

        expect(streams).to eq(%w[s1 s2])
      end

      it 'returns nil if no more commands to reserve' do
        reserved_command = backend.reserve_next do |cmd|
        end
        expect(reserved_command).to be(nil)
      end

      it 'does not reserve a command if the stream is locked' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        cmd2 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        cmd3 = Tests::DoSomething.parse(stream_id: 's3', payload: { account_id: 1 })
        backend.schedule_commands([cmd1, cmd2, cmd3])

        # Test that commands won't be reserved
        # while a previous command for the same stream is being worked on
        # #reserve_next should instead find a command for the next unlocked stream
        cmds = []
        backend.reserve_next do |cmd|
          cmds << cmd.id
          backend.reserve_next do |cmd|
            cmds << cmd.id
          end
        end
        expect(cmds).to eq([cmd1.id, cmd3.id])

        backend.reserve_next do |cmd|
          cmds << cmd.id
        end

        expect(cmds).to eq([cmd1.id, cmd3.id, cmd2.id])
      end
    end

    describe '#append_events, #read_event_batch' do
      it 'reads event batch by causation_id' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = cmd1.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd1.payload.account_id)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 4, payload: { account_id: 1 })
        expect(backend.append_events([evt1, evt2, evt3])).to be(true)

        events = backend.read_event_batch(cmd1.id)
        expect(events).to eq([evt1, evt2])
      end

      it 'fails if duplicate [stream_id, seq]' do
        evt1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        backend.append_events([evt1])

        expect do
          backend.append_events([evt2])
        end.to raise_error(Sors::ConcurrentAppendError)
      end
    end

    describe '#read_event_stream' do
      it 'reads full event stream in order' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        evt1 = cmd1.follow_with_seq(Tests::SomethingHappened1, 2, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow_with_seq(Tests::SomethingHappened1, 3, account_id: cmd1.payload.account_id)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's2', seq: 4, payload: { account_id: 1 })
        expect(backend.append_events([evt1, evt2, evt3])).to be(true)
        events = backend.read_event_stream('s1')
        expect(events).to eq([evt1, evt2])
      end

      it ':upto and :after' do
        e1 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        e2 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        e3 = Tests::SomethingHappened1.parse(stream_id: 's1', seq: 3, payload: { account_id: 2 })
        expect(backend.append_events([e1, e2, e3])).to be(true)
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
