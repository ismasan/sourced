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
  consumer_group 'gwt-test-decider'

  state { |_| { exists: false, bound: false } }

  evolve GWTTestMessages::DeviceRegistered do |state, _evt|
    state[:exists] = true
  end

  evolve GWTTestMessages::DeviceBound do |state, _evt|
    state[:bound] = true
  end

  command GWTTestMessages::BindDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already bound' if state[:bound]
    event GWTTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  command GWTTestMessages::NoopCommand do |_state, _cmd|
    # intentionally produces no events
  end

  reaction GWTTestMessages::DeviceBound do |_state, evt|
    GWTTestMessages::NotifyBound.new(payload: { device_id: evt.payload.device_id })
  end

  sync do |state:, messages:, events:|
    state[:synced] = true
  end

  after_sync do |state:, messages:, events:|
    state[:after_synced] = true
  end
end

# Decider without reactions (produces only events)
class GWTTestSimpleDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'gwt-test-simple-decider'

  state { |_| { exists: false } }

  evolve GWTTestMessages::DeviceRegistered do |state, _evt|
    state[:exists] = true
  end

  command GWTTestMessages::BindDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    event GWTTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end
end

class GWTTestStateStoredProjector < Sourced::Projector::StateStored
  partition_by :list_id
  consumer_group 'gwt-test-ss-projector'

  state do |(list_id)|
    { list_id: list_id, items: [], synced: false, after_synced: false }
  end

  evolve GWTTestMessages::ItemAdded do |state, msg|
    state[:items] << msg.payload.name
  end

  evolve GWTTestMessages::ItemArchived do |state, msg|
    state[:items].delete(msg.payload.name)
  end

  sync do |state:, messages:, replaying:|
    state[:synced] = true
  end

  after_sync do |state:, messages:, replaying:|
    state[:after_synced] = true
  end
end

class GWTTestEventSourcedProjector < Sourced::Projector::EventSourced
  partition_by :list_id
  consumer_group 'gwt-test-es-projector'

  state do |(list_id)|
    { list_id: list_id, items: [], synced: false, after_synced: false }
  end

  evolve GWTTestMessages::ItemAdded do |state, msg|
    state[:items] << msg.payload.name
  end

  evolve GWTTestMessages::ItemArchived do |state, msg|
    state[:items].delete(msg.payload.name)
  end

  sync do |state:, messages:, replaying:|
    state[:synced] = true
  end

  after_sync do |state:, messages:, replaying:|
    state[:after_synced] = true
  end
end

RSpec.describe Sourced::Testing::RSpec do
  include Sourced::Testing::RSpec

  describe 'Decider' do
    it 'given history + when command → then expected messages (event + reaction)' do
      with_reactor(GWTTestDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then(
          GWTTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' }),
          GWTTestMessages::NotifyBound.new(payload: { device_id: 'd1' })
        )
    end

    it 'then with shorthand (Class, **payload) for single expected message' do
      with_reactor(GWTTestSimpleDecider, device_id: 'd1')
        .given(GWTTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(GWTTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then(GWTTestMessages::DeviceBound, device_id: 'd1', asset_id: 'a1')
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
          GWTTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' }),
          GWTTestMessages::NotifyBound.new(payload: { device_id: 'd1' })
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

  describe 'Projector (StateStored)' do
    it 'given events → then block asserts evolved state' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then { |r| expect(r.state[:items]).to eq(['Apple']) }
    end

    it 'given multiple events → then block sees cumulative state' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Banana')
        .then { |r| expect(r.state[:items]).to eq(['Apple', 'Banana']) }
    end

    it 'then! runs sync actions before yielding state' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.state[:synced]).to be true }
    end

    it 'then! runs after_sync actions before yielding state' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.state[:after_synced]).to be true }
    end

    it 'given events with archive → state reflects removal' do
      with_reactor(GWTTestStateStoredProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .and(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Banana')
        .and(GWTTestMessages::ItemArchived, list_id: 'L1', name: 'Apple')
        .then { |r| expect(r.state[:items]).to eq(['Banana']) }
    end
  end

  describe 'Projector (EventSourced)' do
    it 'given events → then block asserts evolved state' do
      with_reactor(GWTTestEventSourcedProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Banana')
        .then { |r| expect(r.state[:items]).to eq(['Apple', 'Banana']) }
    end

    it 'given events with archive → state reflects removal' do
      with_reactor(GWTTestEventSourcedProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .and(GWTTestMessages::ItemArchived, list_id: 'L1', name: 'Apple')
        .then { |r| expect(r.state[:items]).to eq([]) }
    end

    it 'then! runs sync actions before yielding state' do
      with_reactor(GWTTestEventSourcedProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.state[:synced]).to be true }
    end

    it 'then! runs after_sync actions before yielding state' do
      with_reactor(GWTTestEventSourcedProjector, list_id: 'L1')
        .given(GWTTestMessages::ItemAdded, list_id: 'L1', name: 'Apple')
        .then! { |r| expect(r.state[:after_synced]).to be true }
    end

  end
end
