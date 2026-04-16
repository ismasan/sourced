# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/store'
require 'sourced/testing/rspec'
require 'sequel'

module DurableTests
  FilledStringArray = Sourced::Types::Array[String].with(size: 1..)

  class IPResolver
    def self.resolve = '11.111.111'
  end

  class Geolocator
    def self.locate(_ip) = 'London, UK'
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

  class AnotherTask < Sourced::DurableWorkflow
  end

  class Doubler
    def self.double(num) = num * 2
  end

  class MultiArgTask < Sourced::DurableWorkflow
    def execute
      double(2) + double(4) + double(2)
    end

    durable def double(num)
      Doubler.double(num)
    end
  end

  class Retryable < Sourced::DurableWorkflow
    def execute
      compute
    end

    def compute
      raise 'nope'
    end

    durable :compute, retries: 2
  end

  class WithContext < Sourced::DurableWorkflow
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

  class WithDelay < Sourced::DurableWorkflow
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

RSpec.describe Sourced::DurableWorkflow do
  include Sourced::Testing::RSpec

  let(:workflow_id) { 'durable-test-1' }
  let(:name) { 'Joe' }

  def make_started(klass, args: [], workflow_id: 'durable-test-1')
    klass::WorkflowStarted.new(payload: { workflow_id:, args: })
  end

  context 'with happy path' do
    it 'starts and produces new messages until completing workflow' do
      started = make_started(DurableTests::Task, args: [name])
      history = [started]

      until history.last.is_a?(DurableTests::Task::WorkflowComplete)
        next_action = DurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::Actions::Append
        history += next_action.messages
      end

      expect(history.map(&:class)).to eq([
        DurableTests::Task::WorkflowStarted,
        DurableTests::Task::StepStarted,
        DurableTests::Task::StepComplete,
        DurableTests::Task::StepStarted,
        DurableTests::Task::StepComplete,
        DurableTests::Task::WorkflowComplete
      ])

      last = history.last
      expect(last.payload.output).to eq('Hello Joe, your IP is 11.111.111 and its location is London, UK')
      expect(last.payload.workflow_id).to eq(workflow_id)
    end
  end

  context 'with failed steps' do
    it 'produces StepFailed message' do
      expect(DurableTests::IPResolver).to receive(:resolve).and_raise('Network Error!')

      started = make_started(DurableTests::Task, args: [name])
      history = [started]

      until history.last.is_a?(DurableTests::Task::StepFailed)
        next_action = DurableTests::Task.handle(history.last, history:)
        expect(next_action).to be_a Sourced::Actions::Append
        history += next_action.messages
      end

      expect(history.map(&:class)).to eq([
        DurableTests::Task::WorkflowStarted,
        DurableTests::Task::StepStarted,
        DurableTests::Task::StepFailed
      ])
      expect(history.last.payload.error_class).to eq('RuntimeError')
      expect(DurableTests::FilledStringArray).to be === history.last.payload.backtrace
    end
  end

  context 'with previously successful step' do
    it 'does not invoke step again, using cached result instead' do
      get_ip_key = Sourced::DurableWorkflow.step_key(:get_ip, [])
      geolocate_key = Sourced::DurableWorkflow.step_key(:geolocate, ['11.111.111'])

      expect(DurableTests::IPResolver).not_to receive(:resolve)
      expect(DurableTests::Geolocator).to receive(:locate).with('11.111.111').and_return('Santiago, Chile')

      with_reactor(DurableTests::Task, workflow_id: workflow_id)
        .given(DurableTests::Task::WorkflowStarted, workflow_id:, args: [name])
        .given(DurableTests::Task::StepStarted, workflow_id:, key: get_ip_key, step_name: :get_ip, args: [])
        .given(DurableTests::Task::StepComplete, workflow_id:, key: get_ip_key, step_name: :get_ip, output: '11.111.111')
        .when(DurableTests::Task::StepStarted, workflow_id:, key: geolocate_key, step_name: :geolocate, args: ['11.111.111'])
        .then(
          DurableTests::Task::StepComplete.new(payload: {
            workflow_id:, key: geolocate_key, step_name: :geolocate, output: 'Santiago, Chile'
          })
        )
    end
  end

  context 'when workflow is finally failed' do
    it 'does not try again' do
      get_ip_key = Sourced::DurableWorkflow.step_key(:get_ip, [])

      with_reactor(DurableTests::Task, workflow_id: workflow_id)
        .given(DurableTests::Task::WorkflowStarted, workflow_id:, args: [name])
        .given(DurableTests::Task::StepStarted, workflow_id:, key: get_ip_key, step_name: :get_ip, args: [])
        .given(DurableTests::Task::StepFailed, workflow_id:, key: get_ip_key, step_name: :get_ip, error_class: 'NetworkError', error_message: 'foo', backtrace: [])
        .given(DurableTests::Task::WorkflowFailed, workflow_id:)
        .when(DurableTests::Task::StepStarted, workflow_id:, key: get_ip_key, step_name: :get_ip, args: [])
        .then(Sourced::Testing::RSpec::NONE)
    end
  end

  context 'with a different workflow handling irrelevant messages' do
    it 'blows up' do
      with_reactor(DurableTests::Task, workflow_id: workflow_id)
        .when(DurableTests::AnotherTask::WorkflowStarted, workflow_id:, args: [name])
        .then(Sourced::DurableWorkflow::UnknownMessageError)
    end
  end

  describe 'caching method calls by signature' do
    it 'only invokes methods with the same arguments once per workflow' do
      started = make_started(DurableTests::MultiArgTask)
      history = [started]

      allow(DurableTests::Doubler).to receive(:double).and_call_original

      until history.last.is_a?(DurableTests::MultiArgTask::WorkflowComplete)
        next_action = DurableTests::MultiArgTask.handle(history.last, history:)
        expect(next_action).to be_a Sourced::Actions::Append
        history += next_action.messages
      end

      expect(history.last.payload.output).to eq(16)
      expect(DurableTests::Doubler).to have_received(:double).with(2).once
      expect(DurableTests::Doubler).to have_received(:double).with(4).once
    end
  end

  describe 'limited retries' do
    it 'retries the configured number of times until it fails the workflow' do
      started = make_started(DurableTests::Retryable)
      history = [started]

      6.times do
        next_action = DurableTests::Retryable.handle(history.last, history:)
        history += next_action.messages if next_action.respond_to?(:messages)
      end

      task = DurableTests::Retryable.from(history)
      expect(task.status).to eq(:failed)

      expect(history.map(&:class)).to eq([
        DurableTests::Retryable::WorkflowStarted,
        DurableTests::Retryable::StepStarted,
        DurableTests::Retryable::StepFailed,
        DurableTests::Retryable::StepStarted,
        DurableTests::Retryable::StepFailed,
        DurableTests::Retryable::WorkflowFailed
      ])
    end
  end

  context 'with context preserved across failures' do
    it 'tracks context changes in event history' do
      started = make_started(DurableTests::WithContext)
      history = [started]

      until history.last.is_a?(DurableTests::WithContext::WorkflowComplete)
        next_action = DurableTests::WithContext.handle(history.last, history:)
        history += next_action.messages
      end

      task = DurableTests::WithContext.from(history)
      expect(task.output).to eq([2, 4, 6, 8, 10])

      expect(history.map(&:class)).to eq([
        DurableTests::WithContext::WorkflowStarted,
        DurableTests::WithContext::StepStarted,
        DurableTests::WithContext::StepFailed,
        DurableTests::WithContext::ContextUpdated,
        DurableTests::WithContext::StepStarted,
        DurableTests::WithContext::StepComplete,
        DurableTests::WithContext::ContextUpdated,
        DurableTests::WithContext::WorkflowComplete
      ])

      ctx_events = history.select { |m| m.is_a?(DurableTests::WithContext::ContextUpdated) }
      expect(ctx_events[0].payload.context).to eq(index: 2, results: [2, 4])
      expect(ctx_events[1].payload.context).to eq(index: 5, results: [2, 4, 6, 8, 10])
    end
  end

  describe '#wait' do
    it 'schedules a WaitStarted and WaitEnded combo' do
      now = Time.now

      Timecop.freeze(now) do
        started = make_started(DurableTests::WithDelay)
        history = [started]

        next_action = DurableTests::WithDelay.handle(history.last, history:)
        history += next_action.messages # StepStarted

        next_action = DurableTests::WithDelay.handle(history.last, history:)
        history += next_action.messages # StepComplete

        next_action = DurableTests::WithDelay.handle(history.last, history:)
        history += next_action.messages # WaitStarted

        expect(history.last).to be_a(DurableTests::WithDelay::WaitStarted)
        expect(history.last.payload.at).to eq(now + 10)

        next_action = DurableTests::WithDelay.handle(history.last, history:)
        expect(next_action).to be_a(Sourced::Actions::Schedule)
        expect(next_action.at).to eq(now + 10)
        expect(next_action.messages.first).to be_a(DurableTests::WithDelay::WaitEnded)
        expect(next_action.messages.first.payload.workflow_id).to eq('durable-test-1')

        history << next_action.messages.first

        until history.last.is_a?(DurableTests::WithDelay::WorkflowComplete)
          next_action = DurableTests::WithDelay.handle(history.last, history:)
          history += next_action.messages
        end

        expect(history.map(&:class)).to eq([
          DurableTests::WithDelay::WorkflowStarted,
          DurableTests::WithDelay::StepStarted,
          DurableTests::WithDelay::StepComplete,
          DurableTests::WithDelay::WaitStarted,
          DurableTests::WithDelay::WaitEnded,
          DurableTests::WithDelay::StepStarted,
          DurableTests::WithDelay::StepComplete,
          DurableTests::WithDelay::WorkflowComplete
        ])
      end
    end
  end

  describe 'end-to-end via store + router' do
    let(:db) { Sequel.sqlite }
    let(:store) { Sourced::Store.new(db) }
    let(:router) { Sourced::Router.new(store:) }

    before { store.install! }

    it 'drains two concurrent workflows to completion' do
      router.register(DurableTests::Task)

      wf1_id = "wf-#{SecureRandom.uuid}"
      wf2_id = "wf-#{SecureRandom.uuid}"
      store.append([DurableTests::Task::WorkflowStarted.new(payload: { workflow_id: wf1_id, args: ['Alice'] })])
      store.append([DurableTests::Task::WorkflowStarted.new(payload: { workflow_id: wf2_id, args: ['Bob'] })])

      router.drain

      wf1, = Sourced.load(DurableTests::Task, store:, workflow_id: wf1_id)
      wf2, = Sourced.load(DurableTests::Task, store:, workflow_id: wf2_id)

      expect(wf1.status).to eq(:complete)
      expect(wf1.output).to include('Alice')
      expect(wf2.status).to eq(:complete)
      expect(wf2.output).to include('Bob')
    end
  end
end
