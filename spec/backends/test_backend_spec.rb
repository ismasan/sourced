# frozen_string_literal: true

require 'spec_helper'
require 'sors/backends/test_backend'

module Tests
  DoSomething = Sors::Message.define('tests.do_something') do
    attribute :account_id, Integer
  end
  SomethingHappened1 = Sors::Message.define('tests.something_happened1')
end

RSpec.describe Sors::Backends::TestBackend do
  subject(:backend) { described_class.new }

  describe '#schedule_commands and #reserve_next' do
    it 'schedules commands and reserves them in order of arrival' do
      cmd1 = Tests::DoSomething.parse(stream_id: 's1', payload: { account_id: 1 })
      cmd2 = Tests::DoSomething.parse(stream_id: 's2', payload: { account_id: 1 })
      expect(backend.schedule_commands([cmd1, cmd2])).to be(true)

      streams = []
      backend.reserve_next do |cmd|
        streams << cmd.stream_id
      end

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
      evt1 = cmd1.follow(Tests::SomethingHappened1)
      evt2 = cmd1.follow(Tests::SomethingHappened1)
      evt3 = Tests::SomethingHappened1.parse(stream_id: 's1')
      expect(backend.append_events([evt1, evt2, evt3])).to be(true)

      events = backend.read_event_batch(cmd1.id)
      expect(events).to eq([evt1, evt2])
    end
  end
end
