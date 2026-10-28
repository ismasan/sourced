# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

module TopologyTest
  # --- Messages ---
  CreateWidget = Sourced::Command.define('ccc_topo.create_widget') do
    attribute :widget_id, String
    attribute :name, String
  end

  WidgetCreated = Sourced::Event.define('ccc_topo.widget_created') do
    attribute :widget_id, String
    attribute :name, String
  end

  NotifyWidget = Sourced::Command.define('ccc_topo.notify_widget') do
    attribute :widget_id, String
  end

  WidgetNotified = Sourced::Event.define('ccc_topo.widget_notified') do
    attribute :widget_id, String
  end

  ArchiveWidget = Sourced::Command.define('ccc_topo.archive_widget') do
    attribute :widget_id, String
  end

  WidgetArchived = Sourced::Event.define('ccc_topo.widget_archived') do
    attribute :widget_id, String
  end

  DelayedCmd = Sourced::Command.define('ccc_topo.delayed_cmd') do
    attribute :widget_id, String
  end

  ScheduleEvent = Sourced::Event.define('ccc_topo.schedule_event') do
    attribute :widget_id, String
  end

  # --- Decider ---
  class WidgetDecider < Sourced::Decider
    partition_by :widget_id

    context { { exists: false } }

    evolve WidgetCreated do |context, _evt|
      context[:exists] = true
    end

    evolve WidgetArchived do |context, _evt|
      context[:exists] = false
    end

    command CreateWidget do |_context, cmd|
      event WidgetCreated, widget_id: cmd.payload.widget_id, name: cmd.payload.name
    end

    command ArchiveWidget do |_context, _cmd|
      event WidgetArchived, widget_id: 'x'
    end

    reaction WidgetCreated do |_context, evt|
      dispatch NotifyWidget, widget_id: evt.payload.widget_id
    end
  end

  # --- Notifier Decider ---
  class NotifierDecider < Sourced::Decider
    partition_by :widget_id

    context { {} }

    evolve WidgetNotified do |context, _evt|
      context[:notified] = true
    end

    command NotifyWidget do |_context, cmd|
      event WidgetNotified, widget_id: cmd.payload.widget_id
    end
  end

  # --- Materializer (StateStored, no reactions) ---
  class WidgetListMaterializer < Sourced::Materializer::StateStored
    partition_by :widget_id
    consumer_group 'TopologyTest::WidgetListMaterializer'

    context { { items: [] } }

    evolve WidgetCreated do |context, msg|
      context[:items] << msg.payload.name
    end
  end

  # --- Materializer partitioned by message id (exclusive, no partition_by) ---
  class QueueMaterializer < Sourced::Materializer::StateStored
    exclusive
    consumer_group 'TopologyTest::QueueMaterializer'

    context { { items: [] } }

    evolve WidgetArchived do |context, msg|
      context[:items] << msg.payload.widget_id
    end
  end

  # --- Materializer (EventSourced, with catch-all reaction) ---
  class ReactingMaterializer < Sourced::Materializer::EventSourced
    partition_by :widget_id
    consumer_group 'TopologyTest::ReactingMaterializer'

    context { { items: [] } }

    evolve WidgetCreated do |context, msg|
      context[:items] << msg.payload.name
    end

    reaction do |_context, evt|
      dispatch NotifyWidget, widget_id: 'x'
    end
  end

  # --- Materializer with specific + catch-all reactions ---
  class MixedReactingMaterializer < Sourced::Materializer::EventSourced
    partition_by :widget_id
    consumer_group 'TopologyTest::MixedReactingMaterializer'

    context { { items: [] } }

    evolve WidgetCreated do |context, msg|
      context[:items] << msg.payload.name
    end

    evolve WidgetArchived do |_context, _msg|
    end

    reaction WidgetCreated do |_context, evt|
      dispatch NotifyWidget, widget_id: evt.payload.widget_id
    end

    reaction do |_context, _evt|
      dispatch ArchiveWidget, widget_id: 'x'
    end
  end

  # --- Decider with chained dispatch (.at) ---
  class SchedulingDecider < Sourced::Decider
    partition_by :widget_id

    context { {} }

    evolve ScheduleEvent do |context, _evt|
      context[:scheduled] = true
    end

    command CreateWidget do |_context, cmd|
      event ScheduleEvent, widget_id: cmd.payload.widget_id
    end

    reaction ScheduleEvent do |_context, evt|
      dispatch(DelayedCmd, widget_id: evt.payload.widget_id).at(evt.created_at + 300)
    end
  end
end

RSpec.describe Sourced::Topology do
  let(:nodes) { described_class.build(reactors) }

  def find_node(id)
    nodes.find { |n| n.id == id }
  end

  def find_nodes_by_type(type)
    nodes.select { |n| n.type == type }
  end

  context 'with WidgetDecider and NotifierDecider' do
    let(:reactors) { [TopologyTest::WidgetDecider, TopologyTest::NotifierDecider] }

    it 'builds command nodes for handled commands' do
      cmd_nodes = find_nodes_by_type('command')
      expect(cmd_nodes.map(&:id)).to contain_exactly(
        'ccc_topo.create_widget',
        'ccc_topo.archive_widget',
        'ccc_topo.notify_widget'
      )
    end

    it 'sets correct group_id on command nodes' do
      node = find_node('ccc_topo.create_widget')
      expect(node.group_id).to eq('topology_test.widget_decider')
    end

    it 'extracts produced events via Prism' do
      node = find_node('ccc_topo.create_widget')
      expect(node.produces).to eq(['ccc_topo.widget_created'])
    end

    it 'extracts produced events for NotifierDecider' do
      node = find_node('ccc_topo.notify_widget')
      expect(node.produces).to eq(['ccc_topo.widget_notified'])
    end

    it 'sets command name from message class' do
      node = find_node('ccc_topo.create_widget')
      expect(node.name).to eq('TopologyTest::CreateWidget')
    end

    it 'extracts schema from command payload' do
      node = find_node('ccc_topo.create_widget')
      expect(node.schema).to include('type' => 'object')
      expect(node.schema['properties']).to include('widget_id', 'name')
    end

    it 'builds event nodes deduplicated by type' do
      evt_nodes = find_nodes_by_type('event')
      evt_types = evt_nodes.map(&:id)
      expect(evt_types).to contain_exactly(
        'ccc_topo.widget_created',
        'ccc_topo.widget_archived',
        'ccc_topo.widget_notified'
      )
    end

    it 'assigns first-seen group_id to event nodes' do
      node = find_node('ccc_topo.widget_created')
      expect(node.group_id).to eq('topology_test.widget_decider')
    end

    it 'event nodes have empty produces' do
      find_nodes_by_type('event').each { |n| expect(n.produces).to eq([]) }
    end

    it 'extracts schema from event payload' do
      node = find_node('ccc_topo.widget_created')
      expect(node.schema).to include('type' => 'object')
      expect(node.schema['properties']).to include('widget_id', 'name')
    end

    it 'builds automation nodes for reactions' do
      aut_nodes = find_nodes_by_type('automation')
      expect(aut_nodes.map(&:id)).to include(
        'ccc_topo.widget_created-topology_test.widget_decider-aut'
      )
    end

    it 'sets correct consumes on automation nodes' do
      node = find_node('ccc_topo.widget_created-topology_test.widget_decider-aut')
      expect(node.consumes).to eq(['ccc_topo.widget_created'])
    end

    it 'extracts dispatched commands from reactions via Prism' do
      node = find_node('ccc_topo.widget_created-topology_test.widget_decider-aut')
      expect(node.produces).to eq(['ccc_topo.notify_widget'])
    end

    it 'sets automation name from event class' do
      node = find_node('ccc_topo.widget_created-topology_test.widget_decider-aut')
      expect(node.name).to eq('reaction(TopologyTest::WidgetCreated)')
    end
  end

  context 'with WidgetListMaterializer (no reactions)' do
    let(:reactors) { [TopologyTest::WidgetListMaterializer] }

    it 'does not build command nodes' do
      expect(find_nodes_by_type('command')).to be_empty
    end

    it 'does not build automation nodes' do
      expect(find_nodes_by_type('automation')).to be_empty
    end

    it 'builds event nodes from evolve handlers' do
      evt_nodes = find_nodes_by_type('event')
      expect(evt_nodes.map(&:id)).to eq(['ccc_topo.widget_created'])
    end

    it 'builds a readmodel node' do
      node = find_node('topology_test.widget_list_materializer-rm')
      expect(node).not_to be_nil
      expect(node.type).to eq('readmodel')
    end

    it 'sets correct name and group_id on readmodel node' do
      node = find_node('topology_test.widget_list_materializer-rm')
      expect(node.name).to eq('TopologyTest::WidgetListMaterializer')
      expect(node.group_id).to eq('topology_test.widget_list_materializer')
    end

    it 'readmodel consumes the evolved event types' do
      node = find_node('topology_test.widget_list_materializer-rm')
      expect(node.consumes).to eq(['ccc_topo.widget_created'])
    end

    it 'readmodel produces nothing when there are no reactions' do
      node = find_node('topology_test.widget_list_materializer-rm')
      expect(node.produces).to eq([])
    end
  end

  context 'with a materializer partitioned by message id' do
    let(:reactors) { [TopologyTest::QueueMaterializer] }

    it 'consumes the evolved event types, which it has no history selector for' do
      expect(TopologyTest::QueueMaterializer.history_selector).to eq(Sourced::Selector::NONE)
      expect(find_node('topology_test.queue_materializer-rm').consumes).to eq(['ccc_topo.widget_archived'])
      expect(find_nodes_by_type('event').map(&:id)).to eq(['ccc_topo.widget_archived'])
    end
  end

  context 'with ReactingMaterializer (catch-all reaction)' do
    let(:reactors) { [TopologyTest::ReactingMaterializer] }
    let(:rm_id) { 'topology_test.reacting_materializer-rm' }
    let(:aut_id) { 'topology_test.reacting_materializer-aut' }

    it 'builds a readmodel node' do
      node = find_node(rm_id)
      expect(node).not_to be_nil
      expect(node.type).to eq('readmodel')
    end

    it 'readmodel consumes the evolved event types' do
      node = find_node(rm_id)
      expect(node.consumes).to eq(['ccc_topo.widget_created'])
    end

    it 'readmodel produces a single automation node' do
      node = find_node(rm_id)
      expect(node.produces).to eq([aut_id])
    end

    it 'builds a single catch-all automation node' do
      aut_nodes = find_nodes_by_type('automation')
      expect(aut_nodes.size).to eq(1)
      node = aut_nodes.first
      expect(node.id).to eq(aut_id)
      expect(node.name).to eq('reaction(TopologyTest::ReactingMaterializer)')
    end

    it 'automation node consumes the readmodel' do
      node = find_node(aut_id)
      expect(node.consumes).to eq([rm_id])
    end

    it 'automation node produces dispatched commands' do
      node = find_node(aut_id)
      expect(node.produces).to eq(['ccc_topo.notify_widget'])
    end
  end

  context 'with MixedReactingMaterializer (specific + catch-all reactions)' do
    let(:reactors) { [TopologyTest::MixedReactingMaterializer] }
    let(:rm_id) { 'topology_test.mixed_reacting_materializer-rm' }
    let(:specific_aut_id) { 'ccc_topo.widget_created-topology_test.mixed_reacting_materializer-aut' }
    let(:catchall_aut_id) { 'topology_test.mixed_reacting_materializer-aut' }

    it 'builds two automation nodes: one specific and one catch-all' do
      aut_nodes = find_nodes_by_type('automation')
      expect(aut_nodes.map(&:id)).to contain_exactly(specific_aut_id, catchall_aut_id)
    end

    it 'specific automation is named after the event' do
      node = find_node(specific_aut_id)
      expect(node.name).to eq('reaction(TopologyTest::WidgetCreated)')
    end

    it 'catch-all automation is named after the reactor' do
      node = find_node(catchall_aut_id)
      expect(node.name).to eq('reaction(TopologyTest::MixedReactingMaterializer)')
    end

    it 'both automation nodes consume the readmodel' do
      [specific_aut_id, catchall_aut_id].each do |id|
        node = find_node(id)
        expect(node.consumes).to eq([rm_id])
      end
    end

    it 'readmodel produces both automation node IDs' do
      node = find_node(rm_id)
      expect(node.produces).to contain_exactly(specific_aut_id, catchall_aut_id)
    end
  end

  context 'with SchedulingDecider (chained dispatch)' do
    let(:reactors) { [TopologyTest::SchedulingDecider] }

    it 'detects dispatch through .at() chain' do
      node = find_node('ccc_topo.schedule_event-topology_test.scheduling_decider-aut')
      expect(node).not_to be_nil
      expect(node.produces).to eq(['ccc_topo.delayed_cmd'])
    end
  end

  context 'event deduplication across reactors' do
    let(:reactors) { [TopologyTest::WidgetDecider, TopologyTest::WidgetListMaterializer] }

    it 'deduplicates event nodes by type string' do
      evt_nodes = find_nodes_by_type('event').select { |n| n.id == 'ccc_topo.widget_created' }
      expect(evt_nodes.size).to eq(1)
    end

    it 'uses first reactor as group_id owner' do
      node = find_node('ccc_topo.widget_created')
      expect(node.group_id).to eq('topology_test.widget_decider')
    end
  end

  context 'command deduplication across reactors' do
    let(:reactors) { [TopologyTest::WidgetDecider, TopologyTest::SchedulingDecider] }

    it 'deduplicates command nodes by type string' do
      cmd_nodes = find_nodes_by_type('command').select { |n| n.id == 'ccc_topo.create_widget' }
      expect(cmd_nodes.size).to eq(1)
    end

    it 'produces no duplicate IDs' do
      ids = nodes.map(&:id)
      expect(ids).to eq(ids.uniq)
    end
  end

  context 'with all test reactors' do
    let(:reactors) do
      [
        TopologyTest::WidgetDecider,
        TopologyTest::NotifierDecider,
        TopologyTest::WidgetListMaterializer,
        TopologyTest::ReactingMaterializer,
        TopologyTest::SchedulingDecider
      ]
    end

    it 'returns flat array of node structs' do
      nodes.each do |n|
        expect(n).to be_a(Struct)
        expect(%w[command event automation readmodel]).to include(n.type)
      end
    end

    it 'all command nodes have produces arrays' do
      find_nodes_by_type('command').each { |n| expect(n.produces).to be_an(Array) }
    end

    it 'all automation nodes have consumes and produces arrays' do
      find_nodes_by_type('automation').each do |n|
        expect(n.consumes).to be_an(Array)
        expect(n.produces).to be_an(Array)
      end
    end
  end
end
