# frozen_string_literal: true

require 'spec_helper'
require 'sors/backends/test_backend'

module Tests
  DoSomething = Sors::Message.define('tests.do_something') do
    attribute :account_id, Integer
  end
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
end
