# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

RSpec.describe Sourced::Spawner do
  it "spawns into a context's spawn or async" do
    task = CollectingTask.new
    block = proc {}

    described_class.into(task, &block)

    expect(task.spawned).to eq([block])
  end

  it 'spawns into the current Async task for a context that can neither spawn nor async', :async do
    ran = Thread::Queue.new

    fiber_task = described_class.into(Thread.current) { ran << Fiber.current }

    fiber_task.wait
    expect(fiber_task).to be_a(Async::Task)
    expect(ran.pop(timeout: 1)).to be_a(Fiber)
  end

  it 'raises outside an Async task, for a context that can neither spawn nor async' do
    expect { described_class.into(Thread.current) { nil } }
      .to raise_error(Sourced::Spawner::NoTaskError, /Sync \{ \|task\| Sourced\.start!\(task\) \}/)
  end
end
