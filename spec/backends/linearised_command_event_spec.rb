# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

module LinearExamples
  class Actor < Sourced::Actor
    state do |id|
      { id: id, uids: [] }
    end

    command :start, uid: String do |state, cmd|
      sleep(rand) # Simulate some processing time

      event :started, cmd.payload
    end

    event :started, uid: String do |state, evt|
      state[:uids] << evt.payload.uid
    end
  end
end

RSpec.describe 'linearising commands and events for the same stream' do
  subject(:backend) { Sourced::Backends::SequelBackend.new(db) }

  let(:db) do
    Sequel.postgres('sourced_test')
  end

  let(:router) { Sourced::Router.new(backend:) }
  let(:stream_id) { 's1' }

  before do
    backend.uninstall
    backend.install

    router.register LinearExamples::Actor
  end

  it 'linearises commands and events for the same stream' do
    workers = 2.times.map do |i|
      Sourced::Worker.new(name: "worker-#{i}", router:)
    end

    threads = workers.map do |worker|
      Thread.new do
        worker.poll
      end
    end

    # Schedule several start commands in a row
    # the next command should not be processed until the previous one is finished
    4.times do |seq|
      cmd = LinearExamples::Actor::Start.parse(stream_id:, seq: seq + 1, payload: { uid: "uid-#{seq}" })
      backend.append_to_stream(cmd.stream_id, [cmd])
    end
    sleep 2
    workers.each(&:stop)
    threads.each(&:join)
    actor = LinearExamples::Actor.new(id: stream_id)
    Sourced.load(actor, backend:)
    expect(actor.state[:uids]).to eq(['uid-0', 'uid-1', 'uid-2', 'uid-3'])
  end
end
