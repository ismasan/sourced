# frozen_string_literal: true

require 'spec_helper'
require 'sourced/durable_workflow'

module DurableTests
  FilledStringArray = Sourced::Types::Array[String].with(size: 1..)

  class IPResolver
    def self.resolve = '11.111.111'
  end

  class Geolocator
    def self.locate(ip) = 'London, UK'
  end

  class Task < Sourced::DurableWorkflow
    def execute(name)
      ip = get_ip
      location = geolocate(ip)
      "Hello #{name}, your IP is #{ip} and its location is #{location}"
    end

    durable def get_ip
      IPResolver.resolve
    end

    durable def geolocate(ip)
      Geolocator.locate(ip)
    end
  end
end

RSpec.describe Sourced::DurableWorkflow do
  let(:stream_id) { 'durable-test-1' }
  let(:name) { 'Joe' }

  context 'with happy path' do
    it 'starts and produces new messages until completing workflow' do
      started = DurableTests::Task::WorkflowStarted.parse(stream_id:, payload: { args: [name] })
      history = [started]

      until history.last.is_a?(DurableTests::Task::WorkflowComplete)
        next_action = DurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::Actions::AppendAfter
        history += next_action.messages
      end

      assert_messages(history, [
        [DurableTests::Task::WorkflowStarted, stream_id, args: [name]],
        [DurableTests::Task::StepStarted, stream_id, step_name: :get_ip, args: []],
        [DurableTests::Task::StepComplete, stream_id, step_name: :get_ip, output: '11.111.111'],
        [DurableTests::Task::StepStarted, stream_id, step_name: :geolocate, args: ['11.111.111']],
        [DurableTests::Task::StepComplete, stream_id, step_name: :geolocate, output: 'London, UK'],
        [DurableTests::Task::WorkflowComplete, stream_id, output: 'Hello Joe, your IP is 11.111.111 and its location is London, UK']
      ])
    end
  end

  context 'with failed steps' do
    it 'produces StepFailed message' do
      expect(DurableTests::IPResolver).to receive(:resolve).and_raise('Network Error!')

      started = DurableTests::Task::WorkflowStarted.parse(stream_id:, payload: { args: [name] })
      history = [started]

      until history.last.is_a?(DurableTests::Task::StepFailed)
        next_action = DurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::Actions::AppendAfter
        history += next_action.messages
      end

      assert_messages(history, [
        [DurableTests::Task::WorkflowStarted, stream_id, args: [name]],
        [DurableTests::Task::StepStarted, stream_id, step_name: :get_ip, args: []],
        [DurableTests::Task::StepFailed, stream_id, step_name: :get_ip, error_class: 'RuntimeError', backtrace: DurableTests::FilledStringArray],
      ])
    end
  end

  context 'with previously successful step' do
    it 'does not invoke step again, using cached result instead' do
      history = build_history([
        [DurableTests::Task::WorkflowStarted, stream_id, args: [name]],
        [DurableTests::Task::StepStarted, stream_id, step_name: :get_ip, args: []],
        [DurableTests::Task::StepComplete, stream_id, step_name: :get_ip, output: '11.111.111'],
        [DurableTests::Task::StepStarted, stream_id, step_name: :geolocate, args: ['11.111.111']],
      ])

      expect(DurableTests::IPResolver).not_to receive(:resolve)
      expect(DurableTests::Geolocator).to receive(:locate).with('11.111.111').and_return 'Santiago, Chile'

      until history.last.is_a?(DurableTests::Task::WorkflowComplete)
        next_action = DurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::Actions::AppendAfter
        history += next_action.messages
      end

      task = DurableTests::Task.from(history)
      expect(task.status).to eq(:complete)
      expect(task.output).to eq('Hello Joe, your IP is 11.111.111 and its location is Santiago, Chile')
    end
  end

  context 'when workflow is finally failed' do
    it 'does not try again' do
      history = build_history([
        [DurableTests::Task::WorkflowStarted, stream_id, args: [name]],
        [DurableTests::Task::StepStarted, stream_id, step_name: :get_ip, args: []],
        [DurableTests::Task::StepFailed, stream_id, step_name: :get_ip, error_class: 'NewtworkError', backtrace: []],
        [DurableTests::Task::WorkflowFailed, stream_id, nil],
      ])

      step_started = DurableTests::Task::StepStarted.parse(stream_id:, payload: { step_name: :get_ip, args: [] })

      next_action = DurableTests::Task.handle(step_started, history:)
      expect(next_action).to eq(Sourced::Actions::OK)
    end
  end

  private

  # assert_messages(
  #   messages,
  #   [
  #     [SomeMessage, some_stream, some_payload]
  #   ]
  # )
  def assert_messages(messages, expected_message_tuples)
    expect(messages.size).to eq(expected_message_tuples.size)

    messages.each.with_index do |m, idx|
      e = expected_message_tuples[idx]
      expect(m).to be_a(e[0])
      expect(m.stream_id).to eq(e[1])
      payload = m.payload.to_h
      e[2].each do |k, v|
        expect(v).to be === payload[k]
      end if e[2]
    end
  end

  def build_history(message_tuples)
    message_tuples.map do |(message_class, stream_id, payload)|
      message_class.parse(stream_id:, payload:)
    end
  end
end

