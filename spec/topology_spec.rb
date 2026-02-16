# frozen_string_literal: true

require 'spec_helper'
require_relative 'support/unit_test_fixtures'

RSpec.describe Sourced::Topology do
  let(:nodes) { described_class.build(reactors) }

  def find_node(id)
    nodes.find { |n| n.id == id }
  end

  def find_nodes_by_type(type)
    nodes.select { |n| n.type == type }
  end

  context 'with ThingActor and NotifierActor' do
    let(:reactors) { [UnitTest::ThingActor, UnitTest::NotifierActor] }

    it 'builds command nodes for handled commands' do
      cmd_nodes = find_nodes_by_type('command')
      expect(cmd_nodes.map(&:id)).to contain_exactly(
        'unittest.create_thing',
        'unittest.notify_thing'
      )
    end

    it 'sets correct group_id on command nodes' do
      node = find_node('unittest.create_thing')
      expect(node.group_id).to eq('UnitTest::ThingActor')
    end

    it 'extracts produced events via Prism' do
      node = find_node('unittest.create_thing')
      expect(node.produces).to eq(['unittest.thing_created'])
    end

    it 'extracts produced events for NotifierActor' do
      node = find_node('unittest.notify_thing')
      expect(node.produces).to eq(['unittest.thing_notified'])
    end

    it 'sets command name from message class' do
      node = find_node('unittest.create_thing')
      expect(node.name).to eq('UnitTest::CreateThing')
    end

    it 'extracts schema from command payload' do
      node = find_node('unittest.create_thing')
      expect(node.schema).to include(
        'type' => 'object',
        'properties' => { 'name' => { 'type' => 'string' } }
      )
    end

    it 'builds event nodes deduplicated by type' do
      evt_nodes = find_nodes_by_type('event')
      evt_types = evt_nodes.map(&:id)
      expect(evt_types).to contain_exactly(
        'unittest.thing_created',
        'unittest.thing_notified'
      )
    end

    it 'assigns first-seen group_id to event nodes' do
      node = find_node('unittest.thing_created')
      expect(node.group_id).to eq('UnitTest::ThingActor')
    end

    it 'event nodes have empty produces' do
      evt_nodes = find_nodes_by_type('event')
      evt_nodes.each do |n|
        expect(n.produces).to eq([])
      end
    end

    it 'extracts schema from event payload' do
      node = find_node('unittest.thing_created')
      expect(node.schema).to include(
        'type' => 'object',
        'properties' => { 'name' => { 'type' => 'string' } }
      )
    end

    it 'builds automation nodes for reactions' do
      aut_nodes = find_nodes_by_type('automation')
      expect(aut_nodes.map(&:id)).to include(
        'unittest.thing_created-UnitTest::ThingActor-aut'
      )
    end

    it 'sets correct consumes on automation nodes' do
      node = find_node('unittest.thing_created-UnitTest::ThingActor-aut')
      expect(node.consumes).to eq(['unittest.thing_created'])
    end

    it 'extracts dispatched commands from reactions via Prism' do
      node = find_node('unittest.thing_created-UnitTest::ThingActor-aut')
      expect(node.produces).to eq(['unittest.notify_thing'])
    end

    it 'sets automation name from event class' do
      node = find_node('unittest.thing_created-UnitTest::ThingActor-aut')
      expect(node.name).to eq('reaction(UnitTest::ThingCreated)')
    end
  end

  context 'with ThingProjector (no commands, no reactions)' do
    let(:reactors) { [UnitTest::ThingProjector] }

    it 'does not build command nodes' do
      expect(find_nodes_by_type('command')).to be_empty
    end

    it 'does not build automation nodes' do
      expect(find_nodes_by_type('automation')).to be_empty
    end

    it 'builds event nodes from evolve handlers' do
      evt_nodes = find_nodes_by_type('event')
      expect(evt_nodes.map(&:id)).to eq(['unittest.thing_created'])
    end

    it 'sets projector group_id on event nodes' do
      node = find_node('unittest.thing_created')
      expect(node.group_id).to eq('UnitTest::ThingProjector')
    end
  end

  context 'with SchedulingActor (chained dispatch)' do
    let(:reactors) { [UnitTest::SchedulingActor] }

    it 'detects dispatch through .at() chain' do
      node = find_node('unittest.schedule_event-UnitTest::SchedulingActor-aut')
      expect(node).not_to be_nil
      expect(node.produces).to eq(['unittest.delayed_cmd'])
    end
  end

  context 'with LoopingActor (self-referencing)' do
    let(:reactors) { [UnitTest::LoopingActor] }

    it 'detects self-dispatched commands' do
      node = find_node('unittest.loop_event-UnitTest::LoopingActor-aut')
      expect(node).not_to be_nil
      expect(node.produces).to eq(['unittest.loop_cmd'])
    end

    it 'produces both command and event nodes' do
      expect(find_node('unittest.loop_cmd')).not_to be_nil
      expect(find_node('unittest.loop_event')).not_to be_nil
    end
  end

  context 'event deduplication across reactors' do
    let(:reactors) { [UnitTest::ThingActor, UnitTest::ThingProjector] }

    it 'deduplicates event nodes by type string' do
      evt_nodes = find_nodes_by_type('event').select { |n| n.id == 'unittest.thing_created' }
      expect(evt_nodes.size).to eq(1)
    end

    it 'uses first reactor as group_id owner' do
      node = find_node('unittest.thing_created')
      expect(node.group_id).to eq('UnitTest::ThingActor')
    end
  end

  context 'command deduplication across reactors' do
    let(:reactors) { [UnitTest::ThingActor, UnitTest::SyncActor] }

    it 'deduplicates command nodes by type string' do
      cmd_nodes = find_nodes_by_type('command').select { |n| n.id == 'unittest.create_thing' }
      expect(cmd_nodes.size).to eq(1)
    end

    it 'uses first reactor as group_id owner for commands' do
      node = find_node('unittest.create_thing')
      expect(node.group_id).to eq('UnitTest::ThingActor')
    end

    it 'produces no duplicate IDs' do
      ids = nodes.map(&:id)
      expect(ids).to eq(ids.uniq)
    end
  end

  context 'when handled_messages_for_evolve contains a command class' do
    let(:reactors) { [UnitTest::ThingActor] }

    around do |example|
      # Temporarily inject a command class into handled_messages_for_evolve
      UnitTest::ThingActor.handled_messages_for_evolve << UnitTest::CreateThing
      example.run
    ensure
      UnitTest::ThingActor.handled_messages_for_evolve.delete(UnitTest::CreateThing)
    end

    it 'skips command classes and does not create event nodes for them' do
      event_ids = find_nodes_by_type('event').map(&:id)
      expect(event_ids).not_to include('unittest.create_thing')
    end

    it 'produces no duplicate IDs' do
      ids = nodes.map(&:id)
      expect(ids).to eq(ids.uniq)
    end
  end

  context 'with all test reactors' do
    let(:reactors) do
      [
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        UnitTest::ThingProjector,
        UnitTest::SchedulingActor,
        UnitTest::LoopingActor
      ]
    end

    it 'returns flat array of node structs' do
      nodes.each do |n|
        expect(n).to be_a(Struct)
        expect(%w[command event automation]).to include(n.type)
      end
    end

    it 'all command nodes have produces arrays' do
      find_nodes_by_type('command').each do |n|
        expect(n.produces).to be_an(Array)
      end
    end

    it 'all automation nodes have consumes and produces arrays' do
      find_nodes_by_type('automation').each do |n|
        expect(n.consumes).to be_an(Array)
        expect(n.produces).to be_an(Array)
      end
    end
  end
end
