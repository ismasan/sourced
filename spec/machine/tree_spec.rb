# frozen_string_literal: true

require 'spec_helper'

module MachineTreeTest
  def self.command(name) = Sourced::Command.define("machine_tree_test.#{name}") { attribute :order_id, String }

  PlaceOrder = command(:place_order)
  StartPayment = command(:start_payment)
  Hold = command(:hold)
  StartPlanning = command(:start_planning)
  Agree = command(:agree)

  # Mounted twice, as two regions.
  class HoldMachine < Sourced::Machine
    Holding = Checkpoint(:holding)
    Held = Checkpoint(:held)

    initial { command Hold => Holding }
    checkpoint(Holding) { command Hold => Held }
    final Held
  end

  class OutlineMachine < Sourced::Machine
    Outline = Checkpoint(:outline)
    Agreed = Checkpoint(:agreed)

    started(Outline) { command Agree => Agreed }
    final Agreed
  end

  # Starts in a compound checkpoint.
  class PlanningMachine < Sourced::Machine
    Itinerary = Checkpoint(:itinerary)
    Finalised = Checkpoint(:finalised)

    started Itinerary, OutlineMachine, on_done: { OutlineMachine::Agreed => Finalised }
    final Finalised
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)
    PaymentStarted = Checkpoint(:started)
    PaymentCaptured = Checkpoint(:captured)
    Planning = Checkpoint(:planning)
    Completed = Checkpoint(:completed)

    initial do
      command PlaceOrder => Placed
      command StartPlanning => Planning
    end

    checkpoint Placed do
      region :payment do
        initial { command StartPayment => PaymentStarted }
        checkpoint PaymentStarted
        final PaymentCaptured
      end
      region :hotel, HoldMachine
      region :car, HoldMachine
      done Completed
    end

    checkpoint Planning, PlanningMachine

    final Completed
  end
end

RSpec.describe Sourced::Machine::Tree do
  let(:machine) { MachineTreeTest::OrderMachine }
  let(:tree) { machine.tree }

  def at(path) = tree.node_at(path)
  def initial_of(region) = at('placed').regions.find { |r| r.name == region }.initial_node

  it 'places a checkpoint at its path, in its region' do
    placed = at('placed.payment.started')

    expect(tree.placements(machine::PaymentStarted)).to eq([placed])
    expect(placed.node).to equal(machine.regions[:payment].nodes[machine::PaymentStarted])
    expect(placed.region.name).to eq(:payment)
    expect(placed.region.nodes[machine::PaymentStarted]).to equal(placed)
    expect(tree.placed(placed.node)).to eq([placed])
    expect(placed.machine).to eq(machine)
  end

  it 'paths initial positions under their region' do
    expect(tree.root.initial_node.path).to eq('initial')
    expect(at('placed').regions.first.initial_node.path).to eq('placed.payment.initial')
  end

  it 'links regions to the placed checkpoint they belong to' do
    order_placed = at('placed')
    payment = order_placed.regions.first

    expect(payment.parent).to equal(order_placed)
    expect(order_placed.regions.map(&:name)).to eq(%i[payment hotel car])
    expect(payment.lineage).to eq([payment, tree.root])
    expect(at('placed.payment.started').descendant_of?(order_placed)).to be(true)
    expect(at('placed.payment.started').descendant_of?(at('placed.hotel.holding'))).to be(false)
  end

  it "reuses a region's initial position" do
    region = at('placed').regions.first
    expect(region.initial_node).to equal(region.initial_node)
  end

  it 'raises for a path with no checkpoint' do
    expect { at('placed.nope') }.to raise_error(Sourced::Machine::Error, /no checkpoint at "placed.nope"/)
  end

  describe 'a machine mounted twice' do
    let(:holding) { MachineTreeTest::HoldMachine::Holding }
    let(:held) { MachineTreeTest::HoldMachine::Held }

    it 'places each of its checkpoints at two paths' do
      expect(tree.placements(holding).map(&:path)).to eq(%w[placed.hotel.holding placed.car.holding])
      expect(tree.placed(MachineTreeTest::HoldMachine.nodes[holding]).map(&:path))
        .to eq(%w[placed.hotel.holding placed.car.holding])
      expect(tree.mounted_machines.count(MachineTreeTest::HoldMachine)).to eq(1)
    end

    it 'resolves its targets within the same mount' do
      expect(tree.resolve(at('placed.car.holding'), held).path).to eq('placed.car.held')
      expect(tree.resolve(initial_of(:hotel), holding).path).to eq('placed.hotel.holding')
    end
  end

  describe '#resolve' do
    it 'reaches the declaring region, its ancestors, and checkpoints below the declaring one' do
      expect(tree.resolve(initial_of(:payment), machine::PaymentStarted).path).to eq('placed.payment.started')
      expect(tree.resolve(at('placed.payment.started'), machine::Completed).path).to eq('completed')
      expect(tree.resolve(at('placed'), machine::PaymentCaptured).path).to eq('placed.payment.captured')
    end

    it "doesn't reach sibling regions, ambiguous descendants, or unplaced handles" do
      expect { tree.resolve(at('placed.payment.started'), MachineTreeTest::HoldMachine::Holding) }
        .to raise_error(Sourced::Machine::DefinitionError, /can only reach region placed\.payment/)
      expect { tree.resolve(at('placed'), MachineTreeTest::HoldMachine::Holding) }
        .to raise_error(Sourced::Machine::DefinitionError, /below it 2 times \(placed\.hotel\.holding, placed\.car\.holding\)/)
      unplaced = Class.new(Sourced::Machine) { consumer_group 'machine_tree_test.unplaced' }.Checkpoint(:nope)
      expect { tree.resolve(at('placed'), unplaced) }
        .to raise_error(Sourced::Machine::DefinitionError, /isn't a checkpoint/)
    end
  end

  describe 'compound checkpoints' do
    it "places a compound checkpoint's sub-states right after it, without an initial position" do
      expect(at('planning.itinerary').region.compound?).to be(true)
      expect(at('planning.itinerary.outline').region.parent).to equal(at('planning.itinerary'))
      expect(tree.all_nodes.map(&:path)).not_to include('planning.initial', 'planning.itinerary.initial')
    end

    it 'enters compound starts all the way down by default' do
      expect(tree.default_entry(at('planning')).map(&:path)).to eq(%w[planning planning.itinerary planning.itinerary.outline])
      expect(tree.default_entry_paths(machine::Planning)).to eq(%w[planning planning.itinerary planning.itinerary.outline])
      expect(tree.default_entry(at('placed')).map(&:path)).to eq(%w[placed])
    end
  end

  it 'is built once the machine is validated' do
    machine.validate!
    expect(machine.tree).to equal(machine.tree)
  end

  it 'is rebuilt on each call while the class body runs' do
    draft = Class.new(Sourced::Machine) { partition_by :order_id }
    expect(draft.tree).not_to equal(draft.tree)
  end
end
