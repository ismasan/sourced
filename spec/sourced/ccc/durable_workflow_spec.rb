# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sourced/ccc/store'
require 'sequel'

module CCCDurableTests
  FilledStringArray = Sourced::Types::Array[String].with(size: 1..)

  class IPResolver
    def self.resolve = '11.111.111'
  end

  class Geolocator
    def self.locate(_ip) = 'London, UK'
  end

  class Task < Sourced::CCC::DurableWorkflow
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

  class AnotherTask < Sourced::CCC::DurableWorkflow
  end

  class Doubler
    def self.double(num) = num * 2
  end

  class MultiArgTask < Sourced::CCC::DurableWorkflow
    def execute
      double(2) + double(4) + double(2)
    end

    durable def double(num)
      Doubler.double(num)
    end
  end

  class Retryable < Sourced::CCC::DurableWorkflow
    def execute
      compute
    end

    def compute
      raise 'nope'
    end

    durable :compute, retries: 2
  end

  class WithContext < Sourced::CCC::DurableWorkflow
    context do
      { index: 0, results: [] }
    end

    def execute
      @numbers = [1, 2, 3, 4, 5]
      iterate
      context[:results]
    end

    durable def iterate
      index = context[:index]
      @numbers[index..].each do |n|
        raise 'oopsie!' if n == 3 && index == 0

        context[:index] += 1
        context[:results] << n * 2
      end
    end
  end

  class WithDelay < Sourced::CCC::DurableWorkflow
    def execute
      name = get_name
      wait 10
      notify(name)
    end

    durable def get_name
      'Joe'
    end

    durable def notify(_name)
      true
    end
  end
end

RSpec.describe Sourced::CCC::DurableWorkflow do
  let(:workflow_id) { 'durable-test-1' }
  let(:name) { 'Joe' }

  def make_started(klass, args: [], workflow_id: 'durable-test-1')
    klass::WorkflowStarted.new(payload: { workflow_id:, args: })
  end

  context 'with happy path' do
    it 'starts and produces new messages until completing workflow' do
      started = make_started(CCCDurableTests::Task, args: [name])
      history = [started]

      until history.last.is_a?(CCCDurableTests::Task::WorkflowComplete)
        next_action = CCCDurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::CCC::Actions::Append
        history += next_action.messages
      end

      expect(history.map(&:class)).to eq([
        CCCDurableTests::Task::WorkflowStarted,
        CCCDurableTests::Task::StepStarted,
        CCCDurableTests::Task::StepComplete,
        CCCDurableTests::Task::StepStarted,
        CCCDurableTests::Task::StepComplete,
        CCCDurableTests::Task::WorkflowComplete
      ])

      last = history.last
      expect(last.payload.output).to eq('Hello Joe, your IP is 11.111.111 and its location is London, UK')
      expect(last.payload.workflow_id).to eq(workflow_id)
    end
  end

  context 'with failed steps' do
    it 'produces StepFailed message' do
      expect(CCCDurableTests::IPResolver).to receive(:resolve).and_raise('Network Error!')

      started = make_started(CCCDurableTests::Task, args: [name])
      history = [started]

      until history.last.is_a?(CCCDurableTests::Task::StepFailed)
        next_action = CCCDurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::CCC::Actions::Append
        history += next_action.messages
      end

      expect(history.map(&:class)).to eq([
        CCCDurableTests::Task::WorkflowStarted,
        CCCDurableTests::Task::StepStarted,
        CCCDurableTests::Task::StepFailed
      ])
      expect(history.last.payload.error_class).to eq('RuntimeError')
      expect(CCCDurableTests::FilledStringArray).to be === history.last.payload.backtrace
    end
  end

  context 'with previously successful step' do
    it 'does not invoke step again, using cached result instead' do
      get_ip_key = Sourced::CCC::DurableWorkflow.step_key(:get_ip, [])
      geolocate_key = Sourced::CCC::DurableWorkflow.step_key(:geolocate, ['11.111.111'])

      history = [
        CCCDurableTests::Task::WorkflowStarted.new(payload: { workflow_id:, args: [name] }),
        CCCDurableTests::Task::StepStarted.new(payload: { workflow_id:, key: get_ip_key, step_name: :get_ip, args: [] }),
        CCCDurableTests::Task::StepComplete.new(payload: { workflow_id:, key: get_ip_key, step_name: :get_ip, output: '11.111.111' }),
        CCCDurableTests::Task::StepStarted.new(payload: { workflow_id:, key: geolocate_key, step_name: :geolocate, args: ['11.111.111'] })
      ]

      expect(CCCDurableTests::IPResolver).not_to receive(:resolve)
      expect(CCCDurableTests::Geolocator).to receive(:locate).with('11.111.111').and_return('Santiago, Chile')

      until history.last.is_a?(CCCDurableTests::Task::WorkflowComplete)
        next_action = CCCDurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::CCC::Actions::Append
        history += next_action.messages
      end

      task = CCCDurableTests::Task.from(history)
      expect(task.status).to eq(:complete)
      expect(task.output).to eq('Hello Joe, your IP is 11.111.111 and its location is Santiago, Chile')
    end
  end

  context 'when workflow is finally failed' do
    it 'does not try again' do
      get_ip_key = Sourced::CCC::DurableWorkflow.step_key(:get_ip, [])

      history = [
        CCCDurableTests::Task::WorkflowStarted.new(payload: { workflow_id:, args: [name] }),
        CCCDurableTests::Task::StepStarted.new(payload: { workflow_id:, key: get_ip_key, step_name: :get_ip, args: [] }),
        CCCDurableTests::Task::StepFailed.new(payload: { workflow_id:, key: get_ip_key, step_name: :get_ip, error_class: 'NetworkError', error_message: 'foo', backtrace: [] }),
        CCCDurableTests::Task::WorkflowFailed.new(payload: { workflow_id: })
      ]

      step_started = CCCDurableTests::Task::StepStarted.new(payload: { workflow_id:, key: get_ip_key, step_name: :get_ip, args: [] })
      next_action = CCCDurableTests::Task.handle(step_started, history:)
      expect(next_action).to eq(Sourced::CCC::Actions::OK)
    end
  end

  context 'with a different workflow handling irrelevant messages' do
    it 'blows up' do
      started = make_started(CCCDurableTests::AnotherTask, args: [name])
      history = [started]

      expect {
        CCCDurableTests::Task.handle(history.last, history:)
      }.to raise_error(Sourced::CCC::DurableWorkflow::UnknownMessageError)
    end
  end

  describe 'caching method calls by signature' do
    it 'only invokes methods with the same arguments once per workflow' do
      started = make_started(CCCDurableTests::MultiArgTask)
      history = [started]

      allow(CCCDurableTests::Doubler).to receive(:double).and_call_original

      until history.last.is_a?(CCCDurableTests::MultiArgTask::WorkflowComplete)
        next_action = CCCDurableTests::MultiArgTask.handle(history.last, history:)
        expect(next_action).to be_a Sourced::CCC::Actions::Append
        history += next_action.messages
      end

      expect(history.last.payload.output).to eq(16)
      expect(CCCDurableTests::Doubler).to have_received(:double).with(2).once
      expect(CCCDurableTests::Doubler).to have_received(:double).with(4).once
    end
  end

  describe 'limited retries' do
    it 'retries the configured number of times until it fails the workflow' do
      started = make_started(CCCDurableTests::Retryable)
      history = [started]

      6.times do
        next_action = CCCDurableTests::Retryable.handle(history.last, history:)
        history += next_action.messages if next_action.respond_to?(:messages)
      end

      task = CCCDurableTests::Retryable.from(history)
      expect(task.status).to eq(:failed)

      expect(history.map(&:class)).to eq([
        CCCDurableTests::Retryable::WorkflowStarted,
        CCCDurableTests::Retryable::StepStarted,
        CCCDurableTests::Retryable::StepFailed,
        CCCDurableTests::Retryable::StepStarted,
        CCCDurableTests::Retryable::StepFailed,
        CCCDurableTests::Retryable::WorkflowFailed
      ])
    end
  end

  context 'with context preserved across failures' do
    it 'tracks context changes in event history' do
      started = make_started(CCCDurableTests::WithContext)
      history = [started]

      until history.last.is_a?(CCCDurableTests::WithContext::WorkflowComplete)
        next_action = CCCDurableTests::WithContext.handle(history.last, history:)
        history += next_action.messages
      end

      task = CCCDurableTests::WithContext.from(history)
      expect(task.output).to eq([2, 4, 6, 8, 10])

      expect(history.map(&:class)).to eq([
        CCCDurableTests::WithContext::WorkflowStarted,
        CCCDurableTests::WithContext::StepStarted,
        CCCDurableTests::WithContext::StepFailed,
        CCCDurableTests::WithContext::ContextUpdated,
        CCCDurableTests::WithContext::StepStarted,
        CCCDurableTests::WithContext::StepComplete,
        CCCDurableTests::WithContext::ContextUpdated,
        CCCDurableTests::WithContext::WorkflowComplete
      ])

      ctx_events = history.select { |m| m.is_a?(CCCDurableTests::WithContext::ContextUpdated) }
      expect(ctx_events[0].payload.context).to eq(index: 2, results: [2, 4])
      expect(ctx_events[1].payload.context).to eq(index: 5, results: [2, 4, 6, 8, 10])
    end
  end

  describe '#wait' do
    it 'schedules a WaitStarted and WaitEnded combo' do
      now = Time.now

      Timecop.freeze(now) do
        started = make_started(CCCDurableTests::WithDelay)
        history = [started]

        next_action = CCCDurableTests::WithDelay.handle(history.last, history:)
        history += next_action.messages # StepStarted

        next_action = CCCDurableTests::WithDelay.handle(history.last, history:)
        history += next_action.messages # StepComplete

        next_action = CCCDurableTests::WithDelay.handle(history.last, history:)
        history += next_action.messages # WaitStarted

        expect(history.last).to be_a(CCCDurableTests::WithDelay::WaitStarted)
        expect(history.last.payload.at).to eq(now + 10)

        next_action = CCCDurableTests::WithDelay.handle(history.last, history:)
        expect(next_action).to be_a(Sourced::CCC::Actions::Schedule)
        expect(next_action.at).to eq(now + 10)
        expect(next_action.messages.first).to be_a(CCCDurableTests::WithDelay::WaitEnded)
        expect(next_action.messages.first.payload.workflow_id).to eq('durable-test-1')

        history << next_action.messages.first

        until history.last.is_a?(CCCDurableTests::WithDelay::WorkflowComplete)
          next_action = CCCDurableTests::WithDelay.handle(history.last, history:)
          history += next_action.messages
        end

        expect(history.map(&:class)).to eq([
          CCCDurableTests::WithDelay::WorkflowStarted,
          CCCDurableTests::WithDelay::StepStarted,
          CCCDurableTests::WithDelay::StepComplete,
          CCCDurableTests::WithDelay::WaitStarted,
          CCCDurableTests::WithDelay::WaitEnded,
          CCCDurableTests::WithDelay::StepStarted,
          CCCDurableTests::WithDelay::StepComplete,
          CCCDurableTests::WithDelay::WorkflowComplete
        ])
      end
    end
  end

  describe 'end-to-end via store + router' do
    let(:db) { Sequel.sqlite }
    let(:store) { Sourced::CCC::Store.new(db) }
    let(:router) { Sourced::CCC::Router.new(store:) }

    before { store.install! }

    it 'drains two concurrent workflows to completion' do
      router.register(CCCDurableTests::Task)

      wf1_id = "wf-#{SecureRandom.uuid}"
      wf2_id = "wf-#{SecureRandom.uuid}"
      store.append([CCCDurableTests::Task::WorkflowStarted.new(payload: { workflow_id: wf1_id, args: ['Alice'] })])
      store.append([CCCDurableTests::Task::WorkflowStarted.new(payload: { workflow_id: wf2_id, args: ['Bob'] })])

      router.drain

      wf1, = Sourced::CCC.load(CCCDurableTests::Task, store:, workflow_id: wf1_id)
      wf2, = Sourced::CCC.load(CCCDurableTests::Task, store:, workflow_id: wf2_id)

      expect(wf1.status).to eq(:complete)
      expect(wf1.output).to include('Alice')
      expect(wf2.status).to eq(:complete)
      expect(wf2.output).to include('Bob')
    end
  end
end
