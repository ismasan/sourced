# frozen_string_literal: true

require 'spec_helper'

module MachineTreeTest
  def self.command(name) = Sourced::Command.define("machine_tree_test.#{name}") { attribute :order_id, String }
  def self.event(name) = Sourced::Event.define("machine_tree_test.#{name}") { attribute :order_id, String }

  PlaceOrder = command(:place_order)
  OrderPlaced = event(:order_placed)
  StartPayment = command(:start_payment)
  PaymentStarted = event(:payment_started)
  PaymentCaptured = event(:payment_captured)
  OrderCompleted = event(:order_completed)

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    initial { command PlaceOrder => OrderPlaced }

    checkpoint OrderPlaced do
      region :payment do
        initial { command StartPayment => PaymentStarted }
        checkpoint PaymentStarted
        final PaymentCaptured
      end
      done OrderCompleted
    end

    final OrderCompleted
  end
end

RSpec.describe Sourced::Machine::Tree do
  let(:machine) { MachineTreeTest::OrderMachine }
  let(:tree) { machine.tree }

  it 'places every checkpoint once, in its region' do
    placed = tree.checkpoints[MachineTreeTest::PaymentStarted]

    expect(placed.node).to equal(machine.regions[:payment].nodes[MachineTreeTest::PaymentStarted])
    expect(placed.region).to equal(tree.regions[:payment])
    expect(tree.regions[:payment].nodes[MachineTreeTest::PaymentStarted]).to equal(placed)
    expect(tree.placed(placed.node)).to equal(placed)
    expect(placed.machine).to eq(machine)
  end

  it 'links regions to the placed checkpoint they belong to' do
    order_placed = tree.checkpoints[MachineTreeTest::OrderPlaced]
    payment = tree.regions[:payment]

    expect(payment.parent).to equal(order_placed)
    expect(order_placed.regions).to eq([payment])
    expect(payment.lineage).to eq([payment, tree.root])
    expect(tree.checkpoints[MachineTreeTest::PaymentStarted].descendant_of?(order_placed)).to be(true)
  end

  it "reuses a region's blank initial position" do
    region = tree.regions[:payment]
    expect(region.initial_node).to equal(region.initial_node)
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
