# frozen_string_literal: true

require 'spec_helper'

module MachineConfigurationTest
  def self.command(name) = Sourced::Command.define("machine_configuration_test.#{name}") { attribute :order_id, String }

  PlaceOrder = command(:place_order)
  Pay = command(:pay)
  Pack = command(:pack)
  Ship = command(:ship)
  Cancel = command(:cancel)

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    class Working < Sourced::Machine::Part
      part_of OrderMachine

      Progress = Command(:progress)

      Started = State(:started)
      Progressed = State(:progressed)

      started(Started) { command Progress => Progressed }
      final Progressed
    end

    Placed = State(:placed)
    Paid = State(:paid)
    Packed = State(:packed)
    Shipped = State(:shipped)
    Completed = State(:completed)
    Cancelled = State(:cancelled)

    context { {} }

    initial { command PlaceOrder => Placed }

    state Placed do
      command Cancel => Cancelled

      region :payment do
        initial { command Pay => Paid }
        final Paid
      end

      region :shipping do
        initial { command Pack => Packed }
        state(Packed) { command Ship => Working }
        state Working do
          on_final Working::Progressed => Shipped
        end
        final Shipped
      end

      done Completed
    end

    final Completed
    stopped Cancelled
  end
end

RSpec.describe Sourced::Machine::Configuration do
  let(:m) { MachineConfigurationTest }
  let(:machine) { m::OrderMachine }

  def cmd(klass) = klass.new(payload: { order_id: 'o1' })

  # What a projection keeps: the active states' paths, from each state event's exited and entered.
  def tracked(events, paths = [])
    events.select { |evt| machine.transition_classes.include?(evt.class) }
          .reduce(paths) { |acc, evt| (acc - evt.payload.exited) | evt.payload.entered }
  end

  it 'is at the initial position with no paths' do
    config = machine.configuration([])
    expect(config.paths).to eq([])
    expect(config.value).to eq(:initial)
    expect(config.current).to eq(:initial)
    expect(config.final?).to be(false)
    expect(config.available_commands).to eq(Set[m::PlaceOrder])
  end

  it 'reads the configuration at the given paths, in any order' do
    config = machine.configuration(['placed.shipping.packed', 'placed'])
    expect(config.paths).to eq(['placed', 'placed.shipping.packed'])
    expect(config.value).to eq(machine::Placed => { payment: :initial, shipping: machine::Packed })
    expect(config.in?(machine::Packed)).to be(true)
    expect(config.in?(:payment, :initial)).to be(true)
    expect(config.current(:shipping)).to eq(machine::Packed)
    expect(config.available_commands).to eq(Set[m::Cancel, m::Pay, m::Ship, machine::Stop])
    expect(config.available_commands).to be_frozen
  end

  it "reads a part's states" do
    config = machine.configuration(%w[placed placed.shipping.working placed.shipping.working.started])
    expect(config.in?(machine::Working)).to be(true)
    expect(config.available_commands).to eq(Set[m::Cancel, m::Pay, machine::Working::Progress, machine::Stop])
  end

  it 'accepts nothing at a final state' do
    config = machine.configuration(['cancelled'])
    expect(config.final?).to be(true)
    expect(config.available_commands).to be_empty
  end

  it "matches a live machine's, with the paths a projection keeps from its events" do
    instance = machine.new(order_id: 'o1')
    paths = []
    [m::PlaceOrder, m::Pack, m::Ship, machine::Working::Progress, m::Pay].each do |klass|
      paths = tracked(instance.decide(cmd(klass)), paths)
      config = machine.configuration(paths)
      expect(config.value).to eq(instance.machine.value)
      expect(config.paths).to eq(instance.machine.paths)
      expect(config.available_commands).to eq(instance.available_commands)
    end
    expect(instance.machine.value).to eq(machine::Completed)
  end

  it "lists every state's commands as handled_commands, wherever it is" do
    all = Set[m::PlaceOrder, m::Cancel, m::Pay, m::Pack, m::Ship, machine::Working::Progress, machine::Stop]
    expect(machine.configuration([]).handled_commands).to eq(all)
    expect(machine.configuration(['cancelled']).handled_commands).to eq(all)
    expect(machine.configuration([]).handled_commands).to be_frozen

    instance = machine.new(order_id: 'o1')
    expect(instance.handled_commands).to eq(all)
    instance.decide(cmd(m::PlaceOrder))
    expect(instance.handled_commands).to eq(all)
    expect(instance.machine.handled_commands).to equal(instance.handled_commands)
  end

  it 'is what a machine instance reads as #machine' do
    instance = machine.new(order_id: 'o1')
    instance.decide(cmd(m::PlaceOrder))
    expect(instance.machine).to be_a(described_class)
    expect(instance.machine.paths).to eq(['placed'])
  end

  it 'raises for paths that are no configuration' do
    expect { machine.configuration(['nope']) }.to raise_error(Sourced::Machine::UnresolvablePathError)
    expect { machine.configuration(['placed.payment.initial']) }.to raise_error(ArgumentError, /initial position/)
    expect { machine.configuration(['placed.shipping.packed']) }.to raise_error(ArgumentError, /inactive region/)
    expect { machine.configuration(%w[placed cancelled]) }.to raise_error(ArgumentError, /in one region/)
  end
end
