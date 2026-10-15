# frozen_string_literal: true

require 'spec_helper'

module MachineTreeTest
  def self.command(name) = Sourced::Command.define("machine_tree_test.#{name}") { attribute :order_id, String }

  PlaceOrder = command(:place_order)
  StartPayment = command(:start_payment)
  Check = command(:check)
  Settle = command(:settle)
  Approve = command(:approve)

  # Mounted twice, as two regions.
  class CheckMachine < Sourced::Machine
    Submitting = Checkpoint(:submitting)
    Submitted = Checkpoint(:submitted)

    initial { command Check => Submitting }
    checkpoint(Submitting) { command Check => Submitted }
    final Submitted
  end

  class DraftMachine < Sourced::Machine
    Draft = Checkpoint(:draft)
    Approved = Checkpoint(:approved)

    start(Draft) { command Approve => Approved }
    final Approved
  end

  # Starts in a compound checkpoint.
  class UnsettledMachine < Sourced::Machine
    Open = Checkpoint(:open)
    Sent = Checkpoint(:sent)

    start Open, DraftMachine, on_done: { DraftMachine::Approved => Sent }
    final Sent
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)
    PaymentStarted = Checkpoint(:started)
    PaymentCaptured = Checkpoint(:captured)
    Unsettled = Checkpoint(:unsettled)
    Completed = Checkpoint(:completed)

    initial do
      command PlaceOrder => Placed
      command Settle => Unsettled
    end

    checkpoint Placed do
      region :payment do
        initial { command StartPayment => PaymentStarted }
        checkpoint PaymentStarted
        final PaymentCaptured
      end
      region :check, CheckMachine
      region :atlas, CheckMachine
      done Completed
    end

    checkpoint Unsettled, UnsettledMachine

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
    expect(order_placed.regions.map(&:name)).to eq(%i[payment check atlas])
    expect(payment.lineage).to eq([payment, tree.root])
    expect(at('placed.payment.started').descendant_of?(order_placed)).to be(true)
    expect(at('placed.payment.started').descendant_of?(at('placed.check.submitting'))).to be(false)
  end

  it "reuses a region's initial position" do
    region = at('placed').regions.first
    expect(region.initial_node).to equal(region.initial_node)
  end

  it 'raises for a path with no checkpoint' do
    expect { at('placed.nope') }.to raise_error(Sourced::Machine::Error, /no checkpoint at "placed.nope"/)
  end

  describe 'a machine mounted twice' do
    let(:submitting) { MachineTreeTest::CheckMachine::Submitting }
    let(:submitted) { MachineTreeTest::CheckMachine::Submitted }

    it 'places each of its checkpoints at two paths' do
      expect(tree.placements(submitting).map(&:path)).to eq(%w[placed.check.submitting placed.atlas.submitting])
      expect(tree.placed(MachineTreeTest::CheckMachine.nodes[submitting]).map(&:path))
        .to eq(%w[placed.check.submitting placed.atlas.submitting])
      expect(tree.mounted_machines.count(MachineTreeTest::CheckMachine)).to eq(1)
    end

    it 'resolves its targets within the same mount' do
      expect(tree.resolve(at('placed.atlas.submitting'), submitted).path).to eq('placed.atlas.submitted')
      expect(tree.resolve(initial_of(:check), submitting).path).to eq('placed.check.submitting')
    end
  end

  describe '#resolve' do
    it 'reaches the declaring region, its ancestors, and checkpoints below the declaring one' do
      expect(tree.resolve(initial_of(:payment), machine::PaymentStarted).path).to eq('placed.payment.started')
      expect(tree.resolve(at('placed.payment.started'), machine::Completed).path).to eq('completed')
      expect(tree.resolve(at('placed'), machine::PaymentCaptured).path).to eq('placed.payment.captured')
    end

    it "doesn't reach sibling regions, ambiguous descendants, or unplaced handles" do
      expect { tree.resolve(at('placed.payment.started'), MachineTreeTest::CheckMachine::Submitting) }
        .to raise_error(Sourced::Machine::DefinitionError, /can only reach region placed\.payment/)
      expect { tree.resolve(at('placed'), MachineTreeTest::CheckMachine::Submitting) }
        .to raise_error(Sourced::Machine::DefinitionError, /below it 2 times \(placed\.check\.submitting, placed\.atlas\.submitting\)/)
      unplaced = Class.new(Sourced::Machine) { consumer_group 'machine_tree_test.unplaced' }.Checkpoint(:nope)
      expect { tree.resolve(at('placed'), unplaced) }
        .to raise_error(Sourced::Machine::DefinitionError, /isn't a checkpoint/)
    end
  end

  describe 'compound checkpoints' do
    it "places a compound checkpoint's sub-states right after it, without an initial position" do
      expect(at('unsettled.open').region.compound?).to be(true)
      expect(at('unsettled.open.draft').region.parent).to equal(at('unsettled.open'))
      expect(tree.all_nodes.map(&:path)).not_to include('unsettled.initial', 'unsettled.open.initial')
    end

    it 'enters compound starts all the way down by default' do
      expect(tree.default_entry(at('unsettled')).map(&:path)).to eq(%w[unsettled unsettled.open unsettled.open.draft])
      expect(tree.default_entry_paths(machine::Unsettled)).to eq(%w[unsettled unsettled.open unsettled.open.draft])
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
