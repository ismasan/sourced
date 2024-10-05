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

  RSpec.shared_examples 'a backend' do
    it 'is installed' do
      expect(backend.installed?).to be(true)
    end

    it 'supports the Backend interface' do
      expect do
        Sors::Configuration::BackendInterface.parse(backend)
      end.not_to raise_error(Plumb::ParseError)
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
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        evt1 = cmd1.follow(Tests::SomethingHappened1, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow(Tests::SomethingHappened1, account_id: cmd1.payload.account_id)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's1', payload: { account_id: 1 })
        expect(backend.append_events([evt1, evt2, evt3])).to be(true)

        events = backend.read_event_batch(cmd1.id)
        expect(events).to eq([evt1, evt2])
      end
    end

    describe '#read_event_stream' do
      it 'reads full event stream in order' do
        cmd1 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
        evt1 = cmd1.follow(Tests::SomethingHappened1, account_id: cmd1.payload.account_id)
        evt2 = cmd1.follow(Tests::SomethingHappened1, account_id: cmd1.payload.account_id)
        evt3 = Tests::SomethingHappened1.parse(stream_id: 's2', payload: { account_id: 1 })
        expect(backend.append_events([evt1, evt2, evt3])).to be(true)
        events = backend.read_event_stream('s1')
        expect(events).to eq([evt1, evt2])
      end
    end
  end
end
