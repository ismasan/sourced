# frozen_string_literal: true

require 'spec_helper'
require 'sourced/async_executor'

RSpec.describe Sourced::AsyncExecutor do
  it 'runs blocks spawned before and after the task starts, and waits for both' do
    ran = []

    described_class.new.start do |task|
      task.spawn do
        ran << :first
        task.spawn { ran << :later } # ex. a component started by key, once everything is running
      end
    end

    expect(ran).to eq(%i[first later])
  end
end
