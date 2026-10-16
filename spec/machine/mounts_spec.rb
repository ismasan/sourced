# frozen_string_literal: true

require 'spec_helper'
require_relative '../support/machine_helpers'

module MachineMountsTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_mounts_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_mounts_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }

  PlaceOrder = command(:place_order)
  CancelOrder = command(:cancel_order)

  StartPayment = command(:start_payment, :card)
  PaymentInfoAdded = event(:payment_info_added, :card)
  Capture = command(:capture)
  ExpirePayment = command(:expire_payment)
  ChargeCard = command(:charge_card, :card)
  VoidPayment = command(:void_payment)

  Ship = command(:ship)

  AddLine = command(:add_line, :line_id)
  StartCheck = command(:start_check)
  PassCheck = command(:pass_check, :check_id)

  class OrderState
    attr_accessor :card
  end

  # The payment region on its own: its context is the parent's when mounted.
  class PaymentMachine < Sourced::Machine
    Authorizing = Checkpoint(:authorizing)
    Captured = Checkpoint(:captured)
    Expired = Checkpoint(:expired)

    evolve(PaymentInfoAdded) { |order, evt| order.card = evt.payload.card }

    initial do
      command StartPayment => Authorizing do |_order, cmd|
        event PaymentInfoAdded, card: cmd.payload.card
      end
    end

    checkpoint Authorizing do
      entry { |order, _evt| dispatch ChargeCard, card: order.card }
      exit { |_order, _evt| dispatch VoidPayment }
      after(3600) { |_order, _evt| dispatch ExpirePayment }
      command Capture => Captured
      command ExpirePayment => Expired
    end

    final Captured
    final Expired
  end

  class MountedOrder < Sourced::Machine
    partition_by :order_id
    context { OrderState.new }

    Placed = Checkpoint(:placed)
    Completed = Checkpoint(:completed)
    Cancelled = Checkpoint(:cancelled)

    initial { command PlaceOrder => Placed }

    checkpoint Placed do
      command CancelOrder => Cancelled
      region :payment, PaymentMachine, on_done: { PaymentMachine::Expired => Cancelled }
      region :shipping do
        initial { command Ship => :shipped }
        final :shipped
      end
      done Completed
    end

    final Completed
    final Cancelled
  end

  # The same machine, with the payment region inline.
  class InlineOrder < Sourced::Machine
    partition_by :order_id
    context { OrderState.new }
    evolve(PaymentInfoAdded) { |order, evt| order.card = evt.payload.card }

    Placed = Checkpoint(:placed)
    Authorizing = Checkpoint(:authorizing)
    Completed = Checkpoint(:completed)
    Cancelled = Checkpoint(:cancelled)

    initial { command PlaceOrder => Placed }

    checkpoint Placed do
      command CancelOrder => Cancelled
      region :payment do
        initial do
          command StartPayment => Authorizing do |_order, cmd|
            event PaymentInfoAdded, card: cmd.payload.card
          end
        end

        checkpoint Authorizing do
          entry { |order, _evt| dispatch ChargeCard, card: order.card }
          exit { |_order, _evt| dispatch VoidPayment }
          after(3600) { |_order, _evt| dispatch ExpirePayment }
          command Capture => :captured
          command ExpirePayment => :expired
        end

        final :captured
        final :expired
      end
      region :shipping do
        initial { command Ship => :shipped }
        final :shipped
      end
      done Completed
    end

    final Completed
    final Cancelled
  end

  # PaymentMachine mounted twice: a deposit while placed, and a balance once shipped.
  class TwicePaidOrder < Sourced::Machine
    partition_by :order_id
    context { OrderState.new }

    Placed = Checkpoint(:placed)
    Shipped = Checkpoint(:shipped)

    initial { command PlaceOrder => Placed }

    checkpoint Placed do
      region :deposit, PaymentMachine
      region :balance do
        initial { command Ship => Shipped }
        checkpoint Shipped do
          region :payment, PaymentMachine
        end
      end
    end
  end

  # A child machine a mounted region's handler starts.
  class LineMachine < Sourced::Machine
    partition_by :order_id, :line_id

    Added = Checkpoint(:added)

    started Added
    stopped :cancelled
  end

  class LinesRegion < Sourced::Machine
    initial do
      command(AddLine, starts: LineMachine) { |_order, cmd| start LineMachine, line_id: cmd.payload.line_id }
    end
  end

  class OrderWithLines < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)

    initial { command PlaceOrder => Placed }
    checkpoint Placed do
      region :lines, LinesRegion
    end
  end

  # A child machine a mounted region's checkpoint starts.
  class CheckMachine < Sourced::Machine
    partition_by :order_id, :check_id

    Running = Checkpoint(:running)
    Passed = Checkpoint(:passed)

    started Running do
      command PassCheck => Passed
    end
    final Passed
    stopped :cancelled
  end

  class ChecksRegion < Sourced::Machine
    initial { command StartCheck => :checking }
    checkpoint :checking do
      start CheckMachine do
        payload { |_order, _checkpoint| { check_id: 'c1' } }
        on_done CheckMachine::Passed => :checked
      end
    end
    final :checked
  end

  class OrderWithChecks < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)

    initial { command PlaceOrder => Placed }
    checkpoint Placed do
      region :checks, ChecksRegion
    end
  end
end

RSpec.describe 'Sourced::Machine region mounts' do
  include MachineHelpers

  MM = MachineMountsTest

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })

  # A fresh instance of +machine+ that has decided +commands+, in order.
  def decided(machine, *commands) = machine.new(order_id: 'o1').tap { |m| commands.each { |cmd| m.decide(cmd) } }

  def summary(events) = events.map { |e| [e.class, e.payload.to_h] }

  # Expected transitions, as [trigger, { from:, to:, … }] pairs.
  def moves(machine, *transitions) = summary(transitions.map { |trigger, opts| moved(machine, trigger, **opts, order_id: 'o1') })

  shared_examples 'a payment region' do
    let(:place) { msg(MM::PlaceOrder) }
    let(:start) { msg(MM::StartPayment, card: 'tok') }

    it 'activates the region with its checkpoint' do
      expect(decided(machine, place).machine.value).to eq(
        machine::Placed => { payment: :initial, shipping: :initial }
      )
    end

    it 'decides in the region, and evolves its data into the context' do
      instance = decided(machine, place)
      events = instance.decide(start)

      expect(summary(events)).to eq(
        summary([msg(MM::PaymentInfoAdded, card: 'tok')]) +
        moves(machine, ['machine_mounts_test.start_payment', { from: 'placed.payment.initial', to: 'placed.payment.authorizing' }])
      )
      expect(instance.context.card).to eq('tok')
      expect(instance.machine.in?(:payment, authorizing)).to be(true)
      expect(instance.machine.in?('placed.payment.authorizing')).to be(true)
    end

    it "accepts the region's commands only where it is" do
      expect(decided(machine, place).handles_command?(msg(MM::Capture))).to be(false)
      expect(decided(machine, place, start).handles_command?(msg(MM::Capture))).to be(true)
    end

    it 'runs entry actions and starts timers' do
      instance = decided(machine, place)
      transitioned = instance.decide(start).last
      charge, expire = instance.__react(transitioned)

      expect(charge).to be_a(MM::ChargeCard)
      expect(charge.payload.card).to eq('tok')
      expect(expire).to be_a(MM::ExpirePayment)
      expect(expire.created_at).to eq(transitioned.created_at + 3600)
      expect(expire.metadata[Sourced::Machine::ENTERED_BY]).to eq(transitioned.id)
    end

    it 'exits innermost first when the checkpoint is left' do
      instance = decided(machine, place, start)
      cancelled = instance.decide(msg(MM::CancelOrder))

      expect(summary(cancelled)).to eq(moves(machine, [
        'machine_mounts_test.cancel_order',
        { from: 'placed', to: 'cancelled', exited: %w[placed.payment.authorizing placed] }
      ]))
      expect(instance.__react(cancelled.last).map(&:class)).to eq([MM::VoidPayment])
    end

    it 'joins once every region is final, in the deciding decision' do
      events = decided(machine, place, start, msg(MM::Capture)).decide(msg(MM::Ship))

      expect(summary(events)).to eq(moves(
        machine,
        ['machine_mounts_test.ship', { from: 'placed.shipping.initial', to: 'placed.shipping.shipped' }],
        ['done', { from: 'placed', to: 'completed', exited: %w[placed.payment.captured placed.shipping.shipped placed] }]
      ))
    end
  end

  describe 'inline' do
    let(:machine) { MM::InlineOrder }
    let(:authorizing) { MM::InlineOrder::Authorizing }

    it_behaves_like 'a payment region'
  end

  describe 'mounted' do
    let(:machine) { MM::MountedOrder }
    let(:authorizing) { MM::PaymentMachine::Authorizing }

    it_behaves_like 'a payment region'

    it "places the mounted machine's checkpoints in the tree" do
      region = machine.tree.root.nodes.fetch(MM::MountedOrder::Placed).regions.find { |r| r.name == :payment }

      expect(region.machine).to eq(MM::PaymentMachine)
      expect(region.parent.key).to eq(MM::MountedOrder::Placed)
      expect(region.nodes.keys).to eq([:initial, MM::PaymentMachine::Authorizing, MM::PaymentMachine::Captured, MM::PaymentMachine::Expired])
      expect(machine.handled_commands).to include(MM::StartPayment, MM::Capture)
      expect(machine.handled_messages_for_evolve).to include(MM::PaymentInfoAdded, machine.checkpoint_class(authorizing))
      expect(machine.handled_messages_for_evolve).not_to include(authorizing)
    end
  end

  describe 'on_done' do
    it "maps the mounted machine's final onto ours, in the deciding decision" do
      events = decided(MM::MountedOrder, msg(MM::PlaceOrder), msg(MM::StartPayment, card: 'tok')).decide(msg(MM::ExpirePayment))

      expect(summary(events)).to eq(moves(
        MM::MountedOrder,
        ['machine_mounts_test.expire_payment', { from: 'placed.payment.authorizing', to: 'placed.payment.expired', exited: %w[placed.payment.authorizing] }],
        ['on_done', { from: 'placed.payment.expired', to: 'cancelled', exited: %w[placed.payment.expired placed] }]
      ))
    end
  end

  describe 'the same machine mounted twice' do
    let(:machine) { MM::TwicePaidOrder }
    let(:authorizing) { MM::PaymentMachine::Authorizing }

    it 'places its checkpoints at two paths' do
      expect(machine.tree.placements(authorizing).map(&:path))
        .to eq(%w[placed.deposit.authorizing placed.balance.shipped.payment.authorizing])
    end

    it 'keeps an independent position at each path' do
      instance = decided(machine, msg(MM::PlaceOrder), msg(MM::StartPayment, card: 'dep'), msg(MM::Ship))
      expect(instance.machine.value).to eq(
        MM::TwicePaidOrder::Placed => { deposit: authorizing, balance: { MM::TwicePaidOrder::Shipped => { payment: :initial } } }
      )

      events = instance.decide(msg(MM::StartPayment, card: 'bal'))
      expect(events.map(&:class)).to eq([MM::PaymentInfoAdded, machine.checkpoint_class(authorizing)])
      expect(events.last.payload.target).to eq('placed.balance.shipped.payment.authorizing')
      expect(instance.machine.in?('placed.deposit.authorizing')).to be(true)
      expect(instance.machine.in?(:payment, authorizing)).to be(true)
      expect(instance.machine.entered_by(:payment)).to equal(events.last)
      expect(instance.machine.entered_by(:deposit)).not_to equal(events.last)
    end

    it 'routes a command both accept to both regions, one transition each' do
      instance = decided(machine, msg(MM::PlaceOrder), msg(MM::StartPayment, card: 'dep'), msg(MM::Ship), msg(MM::StartPayment, card: 'bal'))
      events = instance.decide(msg(MM::Capture))

      expect(summary(events)).to eq(moves(
        machine,
        ['machine_mounts_test.capture', { from: 'placed.deposit.authorizing', to: 'placed.deposit.captured', exited: %w[placed.deposit.authorizing] }],
        ['machine_mounts_test.capture', {
          from: 'placed.balance.shipped.payment.authorizing', to: 'placed.balance.shipped.payment.captured',
          exited: %w[placed.balance.shipped.payment.authorizing]
        }]
      ))
      expect(instance.__react(events.first).map(&:class)).to eq([MM::VoidPayment])
    end
  end

  describe 'children of a mounted machine' do
    it 'starts and tracks them as the parent' do
      machine = MM::OrderWithLines
      instance = decided(machine, msg(MM::PlaceOrder))
      events = instance.decide(msg(MM::AddLine, line_id: 'l1'))

      expect(summary(events)).to eq(summary([MM::LineMachine::Started.new(payload: {
        order_id: 'o1', line_id: 'l1', input: {}, trigger: 'start', source: 'initial', target: 'added', exited: [], entered: %w[added]
      })]))
      expect(instance.machine.children(MM::LineMachine).map(&:keys)).to eq([{ line_id: 'l1' }])
      expect(instance.machine.children(MM::LineMachine).map(&:state)).to eq([MM::LineMachine::Added])
      expect(machine.handled_messages).to include(MM::LineMachine::Started, MM::LineMachine::Finished)
    end
  end

  describe "children a mounted machine's checkpoint starts" do
    it 'starts the child when its checkpoint is entered, and maps its final' do
      instance = decided(MM::OrderWithChecks, msg(MM::PlaceOrder))
      events = instance.decide(msg(MM::StartCheck))
      expect(events.map(&:class)).to eq([MM::OrderWithChecks.checkpoint_class(MM::ChecksRegion::Checking), MM::CheckMachine::Started])
      expect(events.last.payload.check_id).to eq('c1')

      child = MM::CheckMachine.new(order_id: 'o1', check_id: 'c1').tap { |m| m.evolve([events.last]) }
      finished = child.decide(msg(MM::PassCheck, check_id: 'c1')).last
      expect(finished).to be_a(MM::CheckMachine::Finished)

      instance.evolve([finished])
      expect(instance.machine.children(MM::CheckMachine).map(&:final?)).to eq([true])
      expect(instance.machine.children(MM::CheckMachine).map(&:state)).to eq([MM::CheckMachine::Passed])
      expect(summary(instance.__decide_eventless)).to eq(moves(
        MM::OrderWithChecks,
        [MM::CheckMachine::Finished.type, { from: 'placed.checks.checking', to: 'placed.checks.checked', exited: %w[placed.checks.checking] }]
      ))
    end
  end

  describe 'rendering' do
    let(:machine) { MM::MountedOrder }

    it 'includes the mounted region in to_h and to_mermaid' do
      placed = machine.to_h[:states]['placed']
      expect(placed[:type]).to eq('parallel')
      expect(placed[:states].keys).to eq(%w[payment shipping])
      expect(placed[:states]['payment'][:states].keys).to include('authorizing', 'captured')
      expect(placed[:states]['payment'][:states]['authorizing'][:machine]).to eq(MM::PaymentMachine.name)

      expect(machine.to_mermaid).to include('placed_payment_authorizing --> placed_payment_captured : Capture')
    end

    it 'includes the on_done mappings in to_h and to_mermaid' do
      expect(machine.to_h[:states]['placed'][:mounts]).to eq([{
        region: 'payment', src: MM::PaymentMachine.name, on_done: { 'placed.payment.expired' => 'cancelled' }
      }])
      expect(machine.to_mermaid).to include('placed_payment_expired --> cancelled : on_done')
    end

    it "includes the mounted machine's commands and actions in the topology" do
      nodes = Sourced::Topology.build([machine])
      commands = nodes.select { |n| n.type == 'command' }
      automations = nodes.select { |n| n.type == 'automation' }.map(&:name)

      expect(commands.map(&:name)).to include(MM::StartPayment.name, MM::Capture.name)
      expect(commands.find { |n| n.name == MM::StartPayment.name }.produces)
        .to contain_exactly(machine.checkpoint_class(MM::PaymentMachine::Authorizing).type, MM::PaymentInfoAdded.type)
      expect(automations).to include('actions(placed.payment.authorizing)')
    end
  end

  describe 'definition checks' do
    def machine(&block)
      stub_const('MachineMountsSpecParent', Class.new(Sourced::Machine) do
        partition_by :order_id
        consumer_group 'machine_mounts_spec_parent'
        Checkpoint(:placed)
        initial { command MachineMountsTest::PlaceOrder => :placed }
        instance_eval(&block)
      end)
    end

    it 'rejects a mounted machine that targets a checkpoint outside itself' do
      parent = stub_const('MachineMountsSpecOutward', Class.new(Sourced::Machine) { partition_by :order_id })
      placed = parent.Checkpoint(:placed)
      outward = Class.new(Sourced::Machine) do
        initial { command MachineMountsTest::ExpirePayment => placed }
      end
      parent.class_eval do
        initial { command MachineMountsTest::PlaceOrder => placed }
        checkpoint(placed) { region :payment, outward }
      end

      expect { parent.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /another machine's checkpoint that isn't below it/)
    end

    it 'rejects reactions and sync blocks on a mounted machine' do
      reacting = Class.new(Sourced::Machine) do
        reaction(MachineMountsTest::PaymentInfoAdded) { |_context, _evt| nil }
      end
      syncing = Class.new(Sourced::Machine) { sync { |**| nil } }

      expect { machine { checkpoint(:placed) { region :payment, reacting } } }
        .to raise_error(Sourced::Machine::DefinitionError, /reactions or sync blocks/)
      expect { machine { checkpoint(:placed) { region :payment, syncing } } }
        .to raise_error(Sourced::Machine::DefinitionError, /reactions or sync blocks/)
    end

    it "rejects another machine's checkpoints, and clashing region names" do
      expect do
        machine do
          checkpoint(:placed) { region :payment, MachineMountsTest::PaymentMachine }
          checkpoint MachineMountsTest::PaymentMachine::Captured
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /PaymentMachine's checkpoint, not/)

      expect do
        machine do
          checkpoint(:placed) do
            region(:payment) { initial }
            region :payment, MachineMountsTest::PaymentMachine
          end
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /region :payment is declared twice/)
    end

    it 'rejects a machine with children mounted twice' do
      twice = machine do
        checkpoint(:placed) do
          region :lines, MachineMountsTest::LinesRegion
          region :more_lines, MachineMountsTest::LinesRegion
        end
      end
      expect { twice.validate! }.to raise_error(Sourced::Machine::DefinitionError, /LinesRegion is mounted twice, and declares children/)
    end

    it 'rejects on_done keys that are not root finals, and unreachable targets' do
      not_final = machine do
        checkpoint(:placed) do
          region :payment, MachineMountsTest::PaymentMachine, on_done: { MachineMountsTest::PaymentMachine::Authorizing => :cancelled }
        end
        final :cancelled
      end
      expect { not_final.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Authorizing isn't a final checkpoint/)

      elsewhere = machine do
        checkpoint(:placed) do
          region :payment, MachineMountsTest::PaymentMachine, on_done: { MachineMountsTest::PaymentMachine::Expired => :refunded }
        end
        checkpoint(:returned) { region(:refund) { final :refunded } }
      end
      expect { elsewhere.validate! }.to raise_error(Sourced::Machine::DefinitionError, /can only reach/)

      unplaced = machine do
        checkpoint(:placed) do
          region :payment, MachineMountsTest::PaymentMachine, on_done: { MachineMountsTest::PaymentMachine::Expired => :cancelled }
        end
      end
      expect { unplaced.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Cancelled declared but never placed/)
    end

    it 'rejects a block with a machine, and on_done without one' do
      expect do
        machine { checkpoint(:placed) { region(:payment, MachineMountsTest::PaymentMachine) { initial } } }
      end.to raise_error(Sourced::Machine::DefinitionError, /a machine or a block, not both/)
      expect do
        machine { checkpoint(:placed) { region(:payment, on_done: { 1 => 2 }) { initial } } }
      end.to raise_error(Sourced::Machine::DefinitionError, /on_done is for a mounted machine/)
    end
  end
end
