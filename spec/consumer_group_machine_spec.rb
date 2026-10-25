# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::ConsumerGroupMachine do
  let(:m) { described_class }
  let(:retry_at) { Time.utc(2026, 10, 25, 12, 0, 0) }

  def group(status, context = { retry_count: 0 }) = m.at([status], context, group_id: 'shop.orders')
  def cmd(klass, **payload) = klass.new(payload: { group_id: 'shop.orders', **payload })

  it 'accepts these commands in each status' do
    expect(m.configuration(['active']).available_commands).to eq(Set[m::Stop, m::Retry, m::Fail])
    expect(m.configuration(['retrying']).available_commands).to eq(Set[m::Start, m::Stop, m::Retry, m::Recover, m::Fail])
    expect(m.configuration(['stopped']).available_commands).to eq(Set[m::Start, m::Reset])
    expect(m.configuration(['failed']).available_commands).to eq(Set[m::Start, m::Stop, m::Reset])
  end

  it 'starts a group active' do
    instance = m.new(group_id: 'shop.orders')
    instance.decide(cmd(m::Start))
    expect(instance.machine.paths).to eq(['active'])
    expect(instance.context).to eq(retry_count: 0)
  end

  it 'tells operator commands from worker commands' do
    expect(m::OPERATOR_COMMANDS).to eq([m::Start, m::Stop, m::Reset])
    expect(m::WORKER_COMMANDS).to eq([m::Retry, m::Recover, m::Fail])
    expect(m.handled_commands.to_set).to eq((m::OPERATOR_COMMANDS + m::WORKER_COMMANDS).to_set)
  end

  it "doesn't accept commands a status doesn't" do
    expect(group('active').handles_command?(cmd(m::Start))).to be(false)
    expect(group('active').handles_command?(cmd(m::Reset))).to be(false)
    expect(group('active').handles_command?(cmd(m::Recover))).to be(false)
    expect(group('retrying').handles_command?(cmd(m::Reset))).to be(false)
    expect(group('stopped').handles_command?(cmd(m::Stop))).to be(false)
    expect(group('stopped').handles_command?(cmd(m::Fail, exception_class: 'Boom'))).to be(false)
    expect(group('failed').handles_command?(cmd(m::Retry, retry_at:))).to be(false)
  end

  describe 'retrying' do
    it 'records the retry and the failure, and counts retries' do
      instance = group('active')
      instance.decide(cmd(m::Retry, retry_at:, offset_id: 3, exception_class: 'Boom', exception_message: 'oops'))
      expect(instance.machine.paths).to eq(['retrying'])
      expect(instance.context).to eq(
        retry_count: 1, retry_at:, offset_id: 3, exception_class: 'Boom', exception_message: 'oops'
      )

      later = retry_at + 60
      instance.decide(cmd(m::Retry, retry_at: later, offset_id: 3, exception_class: 'Boom', exception_message: 'again'))
      expect(instance.machine.paths).to eq(['retrying'])
      expect(instance.context).to eq(
        retry_count: 2, retry_at: later, offset_id: 3, exception_class: 'Boom', exception_message: 'again'
      )
    end

    it 'recovers to active, clearing the context' do
      instance = group('retrying', { retry_count: 2, retry_at:, offset_id: 3 })
      instance.decide(cmd(m::Recover))
      expect(instance.machine.paths).to eq(['active'])
      expect(instance.context).to eq(retry_count: 0)
    end

    it 'starts on an operator command, without waiting' do
      instance = group('retrying', { retry_count: 2, retry_at:, offset_id: 3 })
      instance.decide(cmd(m::Start))
      expect(instance.machine.paths).to eq(['active'])
      expect(instance.context).to eq(retry_count: 0)
    end

    it 'fails, keeping the failure and its backtrace but not the retry time' do
      backtrace = ["app/orders.rb:12:in 'decide'", "lib/sourced/router.rb:100:in 'handle'"]
      instance = group('retrying', { retry_count: 3, retry_at:, offset_id: 3 })
      instance.decide(cmd(m::Fail, exception_class: 'Boom', exception_message: 'gave up', exception_backtrace: backtrace))
      expect(instance.machine.paths).to eq(['failed'])
      expect(instance.context).to eq(
        retry_count: 3, offset_id: 3, exception_class: 'Boom', exception_message: 'gave up', exception_backtrace: backtrace
      )
    end

    it 'fails without a backtrace' do
      instance = group('active')
      instance.decide(cmd(m::Fail, exception_class: 'Boom'))
      expect(instance.context).to eq(retry_count: 0, exception_class: 'Boom')
    end
  end

  describe 'stopping' do
    it 'records the message, and no retry time' do
      instance = group('retrying', { retry_count: 1, retry_at: })
      instance.decide(cmd(m::Stop, message: 'maintenance'))
      expect(instance.machine.paths).to eq(['stopped'])
      expect(instance.context).to eq(retry_count: 1, message: 'maintenance')
    end

    it 'stops a failed group, keeping its failure' do
      instance = group('failed', { retry_count: 3, exception_class: 'Boom' })
      instance.decide(cmd(m::Stop))
      expect(instance.machine.paths).to eq(['stopped'])
      expect(instance.context).to eq(retry_count: 3, exception_class: 'Boom')
    end
  end

  it 'resets a stopped or failed group, staying there' do
    %w[stopped failed].each do |status|
      instance = group(status, { retry_count: 3, exception_class: 'Boom' })
      instance.decide(cmd(m::Reset))
      expect(instance.machine.paths).to eq([status])
      expect(instance.context).to eq(retry_count: 3, exception_class: 'Boom')
    end
  end

  it 'starts a stopped or failed group, clearing the context' do
    %w[stopped failed].each do |status|
      instance = group(status, { retry_count: 3, exception_class: 'Boom', message: 'maintenance' })
      instance.decide(cmd(m::Start))
      expect(instance.machine.paths).to eq(['active'])
      expect(instance.context).to eq(retry_count: 0)
    end
  end

  describe 'context codec' do
    it 'round-trips a context through JSON' do
      context = {
        retry_count: 1, retry_at:, offset_id: 3, exception_class: 'Boom', exception_message: 'oops',
        exception_backtrace: ["app/orders.rb:12:in 'decide'"]
      }
      json = JSON.parse(JSON.dump(m.encode_context(context)))
      expect(json).to eq(
        'retry_count' => 1, 'retry_at' => '2026-10-25T12:00:00.000000Z', 'offset_id' => 3,
        'exception_class' => 'Boom', 'exception_message' => 'oops',
        'exception_backtrace' => ["app/orders.rb:12:in 'decide'"]
      )
      expect(m.decode_context(json)).to eq(context)
    end

    it 'decodes a missing retry count as 0' do
      expect(m.decode_context({})).to eq(retry_count: 0)
    end

    it 'raises for a context that is no Context' do
      expect { m.decode_context('retry_count' => 'x') }.to raise_error(Plumb::ParseError)
      expect { m.encode_context(retry_count: 1, retry_at: 'tomorrow') }.to raise_error(Plumb::ParseError)
      expect { m.decode_context('retry_count' => 0, 'exception_backtrace' => [1, 2]) }.to raise_error(Plumb::ParseError)
    end
  end
end
