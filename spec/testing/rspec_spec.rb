# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/testing/rspec'

# Reuse message definitions from decider_spec and projector_spec
module GWTTestMessages
  DeviceRegistered = Sourced::Message.define('gwt_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  DeviceBound = Sourced::Message.define('gwt_test.device.bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  BindDevice = Sourced::Message.define('gwt_test.bind_device') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  NotifyBound = Sourced::Message.define('gwt_test.notify_bound') do
    attribute :device_id, String
  end

  NoopCommand = Sourced::Message.define('gwt_test.noop_command') do
    attribute :device_id, String
  end

  ItemAdded = Sourced::Message.define('gwt_test.item.added') do
    attribute :list_id, String
    attribute :name, String
  end

  ItemArchived = Sourced::Message.define('gwt_test.item.archived') do
    attribute :list_id, String
    attribute :name, String
  end

  NotifyArchive = Sourced::Message.define('gwt_test.notify_archive') do
    attribute :list_id, String
  end
end

class GWTTestDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'gwt_test_decider'

  context { { exists: false, bound: false } }

  evolve GWTTestMessages::DeviceRegistered do |context, _evt|
    context[:exists] = true
  end

  evolve GWTTestMessages::DeviceBound do |context, _evt|
    context[:bound] = true
  end

  command GWTTestMessages::BindDevice do |context, cmd|
    raise 'Not found' unless context[:exists]
    raise 'Already bound' if context[:bound]
    event GWTTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  command GWTTestMessages::NoopCommand do |_context, _cmd|
    # intentionally produces no events
  end

  reaction GWTTestMessages::DeviceBound do |_context, evt|
    GWTTestMessages::NotifyBound.new(payload: { device_id: evt.payload.device_id })
  end

  sync do |context:, messages:, events:|
    context[:synced] = true
  end

  after_sync do |context:, messages:, events:|
    context[:after_synced] = true
  end
end

# Decider without reactions (produces only events)
class GWTTestSimpleDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'gwt_test_simple_decider'

  context { { exists: false } }

  evolve GWTTestMessages::DeviceRegistered do |context, _evt|
    context[:exists] = true
  end

  command GWTTestMessages::BindDevice do |context, cmd|
    raise 'Not found' unless context[:exists]
    event GWTTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end
end

class GWTTestStateStoredProjector < Sourced::Projector::StateStored
  partition_by :list_id
  consumer_group 'gwt_test_ss_projector'

  context do
    { items: [], synced: false, after_synced: false }
  end

  evolve GWTTestMessages::ItemAdded do |context, msg|
    context[:items] << msg.payload.name
  end

  evolve GWTTestMessages::ItemArchived do |context, msg|
    context[:items].delete(msg.payload.name)
  end

  sync do |context:, messages:, replaying:|
    context[:synced] = true
  end

  after_sync do |context:, messages:, replaying:|
    context[:after_synced] = true
  end
end

class GWTTestEventSourcedProjector < Sourced::Projector::EventSourced
  partition_by :list_id
  consumer_group 'gwt_test_es_projector'

  context do
    { items: [], synced: false, after_synced: false }
  end

  evolve GWTTestMessages::ItemAdded do |context, msg|
    context[:items] << msg.payload.name
  end

  evolve GWTTestMessages::ItemArchived do |context, msg|
    context[:items].delete(msg.payload.name)
  end

  sync do |context:, messages:, replaying:|
    context[:synced] = true
  end

  after_sync do |context:, messages:, replaying:|
    context[:after_synced] = true
  end
end

class GWTTestProjection < Sourced::Projection
  context do
    { list_id: nil, items: [] }
  end

  evolve GWTTestMessages::ItemAdded do |context, msg|
    raise ArgumentError, 'no blanks' if msg.payload.name.empty?

    context[:list_id] = msg.payload.list_id
    context[:items] << msg.payload.name
  end

  evolve GWTTestMessages::ItemArchived do |context, msg|
    context[:items].delete(msg.payload.name)
  end
end

RSpec.describe Sourced::Testing::RSpec do
  include Sourced::Testing::RSpec

  describe 'Decider' do
    it 'given history + when command → then event (reactions are deferred)' do
      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then(
          GWTTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' })
        )
    end

    it 'when reaction-triggering event → then reaction message' do
      # Reactions run on a separate claim cycle from the originating command.
      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::DeviceBound, device_id: 'd1', asset_id: 'a1')
        .then(
          GWTTestMessages::NotifyBound.new(payload: { device_id: 'd1' })
        )
    end

    it 'then with shorthand (Class, **payload) for single expected message' do
      with_reactor(GWTTestSimpleDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then(GWTTestMessages::DeviceBound, device_id: 'd1', asset_id: 'a1')
    end

    it 'then compares tags when the expected message has some' do
      gwt = lambda do
        with_reactor(GWTTestSimpleDecider, device_id: 'd1')
          .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
          .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
      end
      bound = GWTTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' })

      gwt.().then(bound)
      expect { gwt.().then(bound.with_tags(asset_id: 'a1')) }
        .to raise_error(RSpec::Expectations::ExpectationNotMetError, /expected tags \{asset_id: "a1"\}, got \{\}/)
    end

    it 'no given + when command → then exception (invariant violation)' do
      with_reactor(GWTTestDecider, device_id: 'd1')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then(RuntimeError, 'Not found')
    end

    it 'then with block form yields action pairs' do
      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then { |r|
          expect(r.pairs).to be_a(Array)
          actions, _source = r.pairs.first
          append_actions = Array(actions).select { |a| a.respond_to?(:messages) }
          expect(append_actions).not_to be_empty
        }
    end

    it 'then with [] expects no messages' do
      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::NoopCommand, device_id: 'd1')
        .then([])
    end

    it 'then! runs sync and after_sync actions' do
      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then! { |r|
          expect(r.pairs).to be_a(Array)
        }
    end

    it 'given with message instances' do
      reg = GWTTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })

      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(reg)
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then(
          GWTTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' })
        )
    end

    it 'supports .and as alias for .given' do
      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .and(GWTTestMessages::DeviceBound, device_id: 'd1', asset_id: 'a1')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a2')
        .then(RuntimeError, 'Already bound')
    end
  end

  describe 'Decider hooks under then! with a block' do
    it 'receive events: correlated to the command, and run once' do
      seen = []
      klass = Class.new(GWTTestSimpleDecider) do
        consumer_group 'gwt_test_simple_decider_recording'
        after_sync { |events:, **| seen.concat(events) }
      end
      cmd = GWTTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })

      with_reactor(klass, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(cmd)
        .then! { |r| expect(r.messages.size).to eq(1) }

      expect(seen.size).to eq(1)
      expect(seen.first).to be_a(GWTTestMessages::DeviceBound)
      expect(seen.first.causation_id).to eq(cmd.id)
      expect(seen.first.correlation_type).to eq(cmd.type)
    end
  end

  describe 'Projector (StateStored)' do
    # A subclass whose hooks record what they were handed and how often.
    let(:recording_projector) do
      seen = []
      calls = []
      Class.new(GWTTestStateStoredProjector).tap do |klass|
        klass.consumer_group 'gwt_test_ss_projector_recording'
        klass.after_sync { |messages:, **| seen.concat(messages); calls << :after_sync }
        klass.sync { |**| calls << :sync }
        klass.define_singleton_method(:seen) { seen }
        klass.define_singleton_method(:calls) { calls }
      end
    end

    it 'given events are the batch: then! hands them to the hooks as messages:' do
      apple = GWTTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' })
      pear = GWTTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Pear' })

      with_reactor(recording_projector, list_id: 'L1')
        .given(apple)
        .given(pear)
        .then!([])

      expect(recording_projector.seen).to eq([apple, pear])
    end

    it 'starts context from load_context' do
      projector = Class.new(GWTTestStateStoredProjector) do
        load_context { |values| { list_id: values[:list_id], items: ['Stored'] } }
      end

      with_reactor(projector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then { |r| expect(r.context[:items]).to eq(%w[Stored Apple]) }
    end

    it 'refuses when: a projector consumes events that already exist' do
      expect {
        with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
          .when(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
      }.to raise_error(ArgumentError, /projector.*given/)
    end

    it 'then! runs each hook once, with or without a block' do
      with_reactor(recording_projector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then!([])
      expect(recording_projector.calls).to contain_exactly(:sync, :after_sync)

      recording_projector.calls.clear
      with_reactor(recording_projector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.context[:synced]).to be true }
      expect(recording_projector.calls).to contain_exactly(:sync, :after_sync)
    end

    it 'given events → then block asserts evolved context' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then { |r| expect(r.context[:items]).to eq(['Apple']) }
    end

    it 'then! runs sync actions before yielding context' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.context[:synced]).to be true }
    end

    it 'then! runs after_sync actions before yielding context' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.context[:after_synced]).to be true }
    end
  end

  describe 'Projector (EventSourced)' do
    it 'given events are both history and batch: context evolves and hooks see them' do
      seen = []
      klass = Class.new(GWTTestEventSourcedProjector) do
        consumer_group 'gwt_test_es_projector_recording'
        after_sync { |context:, messages:, **| seen << [context[:items].dup, messages.size] }
      end

      with_reactor(klass, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Banana')
        .then!([])

      expect(seen).to eq([[%w[Apple Banana], 2]])
    end

    it 'given events → then block asserts evolved context' do
      with_reactor(GWTTestEventSourcedProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Banana')
        .then { |r| expect(r.context[:items]).to eq(['Apple', 'Banana']) }
    end

    it 'then! runs sync actions before yielding context' do
      with_reactor(GWTTestEventSourcedProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.context[:synced]).to be true }
    end

    it 'then! runs after_sync actions before yielding context' do
      with_reactor(GWTTestEventSourcedProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.context[:after_synced]).to be true }
    end

  end

  describe 'Projection' do
    it 'evolves given messages and yields the context' do
      with_reactor(GWTTestProjection)
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .and(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Pear')
        .and(GWTTestMessages::ItemArchived, list_id: 'L1', name: 'Apple')
        .then { |r| expect(r.context).to eq(list_id: 'L1', items: ['Pear']) }
    end

    it 'yields no messages' do
      with_reactor(GWTTestProjection)
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then { |r| expect([r.pairs, r.messages]).to eq([[], []]) }
    end

    it 'accepts then! and an empty expectation' do
      with_reactor(GWTTestProjection)
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.context[:items]).to eq(['Apple']) }

      with_reactor(GWTTestProjection)
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then(Sourced::Testing::RSpec::NONE)
    end

    it 'asserts errors raised while evolving' do
      with_reactor(GWTTestProjection)
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: '')
        .then(ArgumentError, 'no blanks')
    end

    it 'works with any class that evolves context and has no handle_batch' do
      counter = Class.new do
        attr_reader :context

        def initialize = @context = 0
        def evolve(messages) = @context += Array(messages).size
      end

      with_reactor(counter)
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Pear')
        .then { |r| expect(r.context).to eq(2) }
    end

    it 'refuses partition values: a projection takes none' do
      expect { with_reactor(GWTTestProjection, list_id: 'L1') }
        .to raise_error(ArgumentError, /takes no partition values/)
    end

    it 'refuses when: a projection consumes events that already exist' do
      expect {
        with_reactor(GWTTestProjection)
          .when(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
      }.to raise_error(ArgumentError, /projection.*given/)
    end

    it 'refuses expected messages: a projection produces none' do
      expect {
        with_reactor(GWTTestProjection)
          .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
          .then(GWTTestMessages::ItemArchived, list_id: 'L1', name: 'Apple')
      }.to raise_error(ArgumentError, /produces no messages/)
    end
  end
end
