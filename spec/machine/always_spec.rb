# frozen_string_literal: true

require 'spec_helper'
require_relative '../support/machine_helpers'

module MachineAlwaysTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_always_test.#{name}") { ([:run_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_always_test.#{name}") { ([:run_id] + attrs).each { |a| attribute a, String } }

  OpenRun = command(:open_run)
  AddItem = command(:add_item, :status)
  ItemAdded = event(:item_added, :status)
  RollUp = command(:roll_up)
  Settle = command(:settle)
  NotifyRollingUp = command(:notify_rolling_up)
  ShipA = command(:ship_a)
  ShipB = command(:ship_b)

  class RunState
    attr_reader :items

    def initialize = @items = []
    def any?(status) = items.include?(status)
    def any_failed? = any?('failed')
    def all_paid? = items.any? && items.all?('paid')
    def enough? = items.size >= 2
  end

  # Guards as callables: a module and an instance.
  module AnyVoid
    def self.call(run) = run.any?('void')
  end

  class AtLeast
    def initialize(count) = @count = count
    def call(run) = run.items.size >= @count
  end

  class RunMachine < Sourced::Machine
    partition_by :run_id

    Open = Checkpoint(:open)
    RollingUp = Checkpoint(:rolling_up)
    Voided = Checkpoint(:voided)
    Failed = Checkpoint(:failed)
    Paid = Checkpoint(:paid)
    Settling = Checkpoint(:settling)

    context { RunState.new }
    evolve(ItemAdded) { |run, evt| run.items << evt.payload.status }

    initial { command OpenRun => Open }

    checkpoint Open do
      command(AddItem) { |_run, cmd| event ItemAdded, status: cmd.payload.status }
      command RollUp => RollingUp
    end

    checkpoint RollingUp do
      entry { |_run, _evt| dispatch NotifyRollingUp }
      always Voided, if: ->(run) { run.any?('void') }
      always Failed, if: :any_failed?
      always Paid, if: :all_paid?
      always Settling
    end

    checkpoint Settling do
      command Settle => Paid
    end

    final Voided
    final Failed
    final Paid
  end

  # A guarded always on a checkpoint that accepts commands: taken once the guard passes.
  class CollectingMachine < Sourced::Machine
    partition_by :run_id

    Collecting = Checkpoint(:collecting)
    Ready = Checkpoint(:ready)

    context { RunState.new }
    evolve(ItemAdded) { |run, evt| run.items << evt.payload.status }

    initial { command OpenRun => Collecting }

    checkpoint Collecting do
      always Ready, if: :enough?
      command(AddItem) { |_run, cmd| event ItemAdded, status: cmd.payload.status }
    end

    final Ready
  end

  # Guarded joins, and a transient checkpoint completing a region before its join.
  class ShipmentMachine < Sourced::Machine
    partition_by :run_id

    Shipping = Checkpoint(:shipping)
    Delivered = Checkpoint(:delivered)
    Partial = Checkpoint(:partial)

    context { RunState.new }
    evolve(ItemAdded) { |run, evt| run.items << evt.payload.status }

    initial do
      command OpenRun => Shipping
      command(AddItem) { |_run, cmd| event ItemAdded, status: cmd.payload.status }
    end

    checkpoint Shipping do
      command(AddItem) { |_run, cmd| event ItemAdded, status: cmd.payload.status }

      region :a do
        initial { command ShipA => :a_checking }
        checkpoint(:a_checking) { always :a_shipped }
        final :a_shipped
      end
      region :b do
        initial { command ShipB => :b_shipped }
        final :b_shipped
      end

      done Delivered, if: :all_paid?
      done Partial
    end

    final Delivered
    final Partial
  end
end

RSpec.describe 'Sourced::Machine always' do
  include MachineHelpers

  MA = MachineAlwaysTest

  def msg(klass, **attrs) = klass.new(payload: { run_id: 'r1', **attrs })
  def decided(machine, *commands) = machine.new(run_id: 'r1').tap { |m| commands.each { |cmd| m.decide(cmd) } }
  def summary(events) = events.map { |e| [e.class, e.payload.to_h] }
  def moves(machine, *transitions) = summary(transitions.map { |trigger, opts| moved(machine, trigger, **opts, run_id: 'r1') })
  def items(*statuses) = statuses.map { |status| msg(MA::AddItem, status:) }

  describe 'always' do
    it 'moves on in the deciding decision, with the first passing guard' do
      instance = decided(MA::RunMachine, msg(MA::OpenRun), *items('paid', 'paid'))
      events = instance.decide(msg(MA::RollUp))

      expect(summary(events)).to eq(moves(
        MA::RunMachine,
        ['machine_always_test.roll_up', { from: 'open', to: 'rolling_up', exited: %w[open] }],
        ['always', { from: 'rolling_up', to: 'paid', exited: %w[rolling_up] }]
      ))
      expect(instance.machine.value).to eq(MA::RunMachine::Paid)
    end

    it 'evaluates guards in order: lambdas, Symbols, then the unguarded fallback' do
      expect(decided(MA::RunMachine, msg(MA::OpenRun), *items('failed', 'void'), msg(MA::RollUp)).machine.value).to eq(MA::RunMachine::Voided)
      expect(decided(MA::RunMachine, msg(MA::OpenRun), *items('paid', 'failed'), msg(MA::RollUp)).machine.value).to eq(MA::RunMachine::Failed)
      expect(decided(MA::RunMachine, msg(MA::OpenRun), *items('paid', 'pending'), msg(MA::RollUp)).machine.value).to eq(MA::RunMachine::Settling)
    end

    it 'takes any #call(context) as a guard, and renders its name' do
      machine = Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::Callables'
        partition_by :run_id
        context { MA::RunState.new }
        evolve(MA::ItemAdded) { |run, evt| run.items << evt.payload.status }
        initial { command MA::OpenRun => :open }
        checkpoint(:open) do
          command(MA::AddItem) { |_run, cmd| event MA::ItemAdded, status: cmd.payload.status }
          command MA::RollUp => :rolling_up
        end
        checkpoint(:rolling_up) do
          always :voided, if: MA::AnyVoid
          always :plenty, if: MA::AtLeast.new(2)
          always :settling
        end
        final :voided
        final :plenty
        final :settling
      end

      checkpoints = machine.declared_checkpoints
      expect(decided(machine, msg(MA::OpenRun), *items('void'), msg(MA::RollUp)).machine.value).to eq(checkpoints.fetch(:voided))
      expect(decided(machine, msg(MA::OpenRun), *items('paid', 'paid'), msg(MA::RollUp)).machine.value).to eq(checkpoints.fetch(:plenty))
      expect(machine.to_h[:states]['rolling_up'][:always]).to eq([
        { target: 'voided', guard: 'MachineAlwaysTest::AnyVoid' },
        { target: 'plenty', guard: 'MachineAlwaysTest::AtLeast' },
        { target: 'settling' }
      ])
    end

    it "runs a transient checkpoint's entry actions when it's claimed" do
      instance = decided(MA::RunMachine, msg(MA::OpenRun))
      rolling_up = instance.decide(msg(MA::RollUp)).first

      expect(instance.__react(rolling_up).map(&:class)).to eq([MA::NotifyRollingUp])
    end

    it 'is re-evaluated after every decision while its checkpoint is active' do
      instance = decided(MA::CollectingMachine, msg(MA::OpenRun))
      expect(summary(instance.decide(items('paid').first))).to eq(summary([msg(MA::ItemAdded, status: 'paid')]))

      events = instance.decide(items('paid').first)
      expect(summary(events)).to eq(
        summary([msg(MA::ItemAdded, status: 'paid')]) +
        moves(MA::CollectingMachine, ['always', { from: 'collecting', to: 'ready', exited: %w[collecting] }])
      )
    end

    it 'is never decided on replay' do
      instance = MA::RunMachine.new(run_id: 'r1')
      instance.evolve([
        moved(MA::RunMachine, 'machine_always_test.open_run', from: 'initial', to: 'open', run_id: 'r1'),
        moved(MA::RunMachine, 'machine_always_test.roll_up', from: 'open', to: 'rolling_up', exited: %w[open], run_id: 'r1')
      ])

      expect(instance.machine.value).to eq(MA::RunMachine::RollingUp)
    end

    it 'resolves a transient checkpoint before its parent checkpoint joins' do
      instance = decided(MA::ShipmentMachine, msg(MA::OpenRun), *items('paid'), msg(MA::ShipB))
      events = instance.decide(msg(MA::ShipA))

      expect(events.map { |e| e.payload.trigger }).to eq(['machine_always_test.ship_a', 'always', 'done'])
      expect(instance.machine.value).to eq(MA::ShipmentMachine::Delivered)
    end

    it 'raises when always checkpoints loop' do
      looping = Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::Looping'
        partition_by :run_id

        initial { command MA::OpenRun => :ping }
        checkpoint(:ping) { always :pong }
        checkpoint(:pong) { always :ping }
      end

      expect { looping.new(run_id: 'r1').decide(msg(MA::OpenRun)) }.to raise_error(Sourced::Machine::Error, /more than 100 joins/)
    end
  end

  describe 'guarded done' do
    it 'takes the first passing guard' do
      instance = decided(MA::ShipmentMachine, msg(MA::OpenRun), *items('paid'), msg(MA::ShipB), msg(MA::ShipA))
      expect(instance.machine.value).to eq(MA::ShipmentMachine::Delivered)
    end

    it 'falls back to the unguarded done' do
      instance = decided(MA::ShipmentMachine, msg(MA::OpenRun), *items('pending'), msg(MA::ShipB), msg(MA::ShipA))
      expect(instance.machine.value).to eq(MA::ShipmentMachine::Partial)
    end

    it 'stays when no guard passes' do
      guarded = Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::GuardedDone'
        partition_by :run_id
        context { MA::RunState.new }

        initial { command MA::OpenRun => :shipping }
        checkpoint :shipping do
          region(:a) { initial { command MA::ShipA => :shipped }; final :shipped }
          done :delivered, if: :all_paid?
        end
        final :delivered
      end

      instance = decided(guarded, msg(MA::OpenRun), msg(MA::ShipA))
      expect(instance.machine.value).to eq(guarded.declared_checkpoints.fetch(:shipping) => { a: guarded.declared_checkpoints.fetch(:shipped) })
    end
  end

  describe 'definition checks' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::Checks'
        partition_by :run_id
        instance_eval(&block)
      end
    end

    it "rejects commands and timers on a transient checkpoint" do
      with_command = machine do
        checkpoint(:a) { always :b; command MA::Settle => :b }
        final :b
      end
      with_timer = machine do
        checkpoint(:a) { always :b; after(60) { dispatch MA::Settle } }
        final :b
      end

      expect { with_command.validate! }.to raise_error(Sourced::Machine::DefinitionError, /a is transient \(always .*b\), so it can't accept commands/i)
      expect { with_timer.validate! }.to raise_error(Sourced::Machine::DefinitionError, /a is transient \(always .*b\), so it can't have timers/i)
    end

    it 'rejects regions and joins on a transient checkpoint, which it leaves before they could run' do
      with_regions = machine do
        checkpoint(:a) { always :b; region(:r) { initial } }
        final :b
      end
      with_done = machine do
        checkpoint(:a) { always :b; done :c }
        final :b
        final :c
      end

      expect { with_regions.validate! }.to raise_error(Sourced::Machine::DefinitionError, /so it can't have regions/)
      expect { with_done.validate! }.to raise_error(Sourced::Machine::DefinitionError, /so it can't join with done or children_done/)
    end

    it 'rejects unreachable always and done' do
      expect { machine { checkpoint(:a) { always :b; always :c }; final :b; final :c } }
        .to raise_error(Sourced::Machine::DefinitionError, /always .*c is unreachable, after an unguarded always/i)
      expect do
        machine do
          checkpoint(:a) { region(:r) { initial { command MA::ShipA => :x }; final :x }; done :b; done :c }
          final :b
          final :c
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /done .*c is unreachable, after an unguarded done/i)
    end

    it 'rejects always on the initial position or a final checkpoint' do
      expect { machine { initial { always :a }; final :a } }
        .to raise_error(Sourced::Machine::DefinitionError, /the initial position is never entered, so no always/)
      expect { machine { final(:a) { always :b }; final :b } }
        .to raise_error(Sourced::Machine::DefinitionError, /final checkpoint .*a can't have always/i)
    end

    it 'rejects guards that are neither a Symbol nor callable' do
      expect { machine { checkpoint(:a) { always :b, if: 'nope' }; final :b } }
        .to raise_error(Sourced::Machine::DefinitionError, /if: takes a Symbol or a #call\(context\), got "nope"/)
    end

    it 'checks always targets as command targets' do
      out_of_reach = machine do
        initial { command MA::OpenRun => :a }
        checkpoint :a do
          region(:left) { initial { command MA::ShipA => :l }; checkpoint(:l) { always :r } }
          region(:right) { initial { command MA::ShipB => :r }; checkpoint :r }
        end
      end

      expect { out_of_reach.validate! }.to raise_error(Sourced::Machine::DefinitionError, /can only reach/)
    end
  end

  describe 'rendering' do
    it 'describes always and guarded done in to_h' do
      rolling_up = MA::RunMachine.to_h[:states]['rolling_up']
      expect(rolling_up[:always]).to eq([
        { target: 'voided', guard: 'guard' },
        { target: 'failed', guard: 'any_failed?' },
        { target: 'paid', guard: 'all_paid?' },
        { target: 'settling' }
      ])

      shipping = MA::ShipmentMachine.to_h[:states]['shipping']
      expect(shipping[:on_done]).to eq([{ target: 'delivered', guard: 'all_paid?' }, { target: 'partial' }])
      expect(shipping[:states]['a'][:states]['a_checking'][:always]).to eq([{ target: 'shipping.a.a_shipped' }])
      expect(MA::RunMachine.to_h[:states]['open'][:always]).to eq([])
    end

    it 'draws always edges, labelled with their guards' do
      diagram = MA::RunMachine.to_mermaid
      expect(diagram).to include('rolling_up --> voided : always [guard]')
      expect(diagram).to include('rolling_up --> failed : always [any_failed?]')
      expect(diagram).to include('rolling_up --> settling : always')
      expect(MA::ShipmentMachine.to_mermaid).to include('shipping --> delivered : done [all_paid?]', 'shipping --> partial : done')
    end

    it "has commands produce the checkpoints their targets' always can reach" do
      nodes = Sourced::Topology.build([MA::RunMachine])
      roll_up = nodes.find { |n| n.type == 'command' && n.id == MA::RollUp.type }
      types = %i[rolling_up voided failed paid settling].map { |name| "machine_always_test.run_machine.runtime.#{name}" }

      expect(roll_up.produces).to contain_exactly(*types)
    end

    it 'has a guarded join produce each of its targets' do
      nodes = Sourced::Topology.build([MA::ShipmentMachine])
      join = nodes.find { |n| n.type == 'automation' && n.name == 'done(shipping)' }
      ship_a = nodes.find { |n| n.type == 'command' && n.id == MA::ShipA.type }

      expect(join.produces).to eq(%w[delivered partial].map { |name| "machine_always_test.shipment_machine.runtime.#{name}" })
      expect(ship_a.produces).to contain_exactly(*%w[a_checking a_shipped].map { |name| "machine_always_test.shipment_machine.runtime.#{name}" })
    end
  end
end
