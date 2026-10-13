# frozen_string_literal: true

require 'spec_helper'

module MachineMountsTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_mounts_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_mounts_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }

  PlaceOrder = command(:place_order)
  OrderPlaced = event(:order_placed)
  CancelOrder = command(:cancel_order)
  OrderCancelled = event(:order_cancelled)
  OrderCompleted = event(:order_completed)

  StartPayment = command(:start_payment, :card)
  PaymentStarted = event(:payment_started)
  PaymentInfoAdded = event(:payment_info_added, :card)
  Capture = command(:capture)
  PaymentCaptured = event(:payment_captured)
  ExpirePayment = command(:expire_payment)
  PaymentExpired = event(:payment_expired)
  ChargeCard = command(:charge_card, :card)
  VoidPayment = command(:void_payment)

  Ship = command(:ship)
  Shipped = event(:shipped)

  AddLine = command(:add_line, :line_id)
  LineAdded = Sourced::Event.define('machine_mounts_test.line_added') do
    attribute :order_id, String
    attribute :line_id, String
  end
  LineCancelled = Sourced::Event.define('machine_mounts_test.line_cancelled') do
    attribute :order_id, String
    attribute :line_id, String
  end

  class OrderState
    attr_accessor :card
  end

  # The payment region on its own: its context is the parent's when mounted.
  class PaymentMachine < Sourced::Machine
    evolve(PaymentInfoAdded) { |order, evt| order.card = evt.payload.card }

    initial do
      command StartPayment => PaymentStarted do |_order, cmd|
        event PaymentStarted
        event PaymentInfoAdded, card: cmd.payload.card
      end
    end

    checkpoint PaymentStarted do
      entry { |order, _evt| dispatch ChargeCard, card: order.card }
      exit { |_order, _evt| dispatch VoidPayment }
      after(3600) { |_order, _evt| dispatch ExpirePayment }
      command Capture => PaymentCaptured
      command ExpirePayment => PaymentExpired
    end

    final PaymentCaptured
    final PaymentExpired
  end

  class MountedOrder < Sourced::Machine
    partition_by :order_id
    context { OrderState.new }

    initial { command PlaceOrder => OrderPlaced }

    checkpoint OrderPlaced do
      command CancelOrder => OrderCancelled
      region :payment, PaymentMachine, on_done: { PaymentExpired => OrderCancelled }
      region :shipping do
        initial { command Ship => Shipped }
        final Shipped
      end
      done OrderCompleted
    end

    final OrderCompleted
    final OrderCancelled
  end

  # The same machine, with the payment region inline.
  class InlineOrder < Sourced::Machine
    partition_by :order_id
    context { OrderState.new }
    evolve(PaymentInfoAdded) { |order, evt| order.card = evt.payload.card }

    initial { command PlaceOrder => OrderPlaced }

    checkpoint OrderPlaced do
      command CancelOrder => OrderCancelled
      region :payment do
        initial do
          command StartPayment => PaymentStarted do |_order, cmd|
            event PaymentStarted
            event PaymentInfoAdded, card: cmd.payload.card
          end
        end

        checkpoint PaymentStarted do
          entry { |order, _evt| dispatch ChargeCard, card: order.card }
          exit { |_order, _evt| dispatch VoidPayment }
          after(3600) { |_order, _evt| dispatch ExpirePayment }
          command Capture => PaymentCaptured
          command ExpirePayment => PaymentExpired
        end

        final PaymentCaptured
        final PaymentExpired
      end
      region :shipping do
        initial { command Ship => Shipped }
        final Shipped
      end
      done OrderCompleted
    end

    final OrderCompleted
    final OrderCancelled
  end

  # A child machine spawned from a mounted region.
  class LineMachine < Sourced::Machine
    partition_by :order_id, :line_id
    start LineAdded
    on_stop LineCancelled
  end

  class LinesRegion < Sourced::Machine
    spawns LineMachine, as: :lines

    initial do
      command(AddLine) { |_order, cmd| spawn :lines, line_id: cmd.payload.line_id }
    end
  end

  class OrderWithLines < Sourced::Machine
    partition_by :order_id

    initial { command PlaceOrder => OrderPlaced }
    checkpoint OrderPlaced do
      region :lines, LinesRegion
    end
  end
end

RSpec.describe 'Sourced::Machine region mounts' do
  include MachineMountsTest

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })

  shared_examples 'a payment region' do
    let(:placed) { msg(MachineMountsTest::OrderPlaced) }
    let(:started) { msg(MachineMountsTest::PaymentStarted) }
    let(:info) { msg(MachineMountsTest::PaymentInfoAdded, card: 'tok') }

    def after(*events) = machine.new(order_id: 'o1').tap { |m| m.evolve(events) }

    it 'activates the region with its checkpoint' do
      expect(after(placed).machine.value).to eq(
        MachineMountsTest::OrderPlaced => { payment: :initial, shipping: :initial }
      )
    end

    it 'decides in the region, and evolves its data into the context' do
      instance = after(placed)
      events = instance.decide(msg(MachineMountsTest::StartPayment, card: 'tok'))

      expect(events.map(&:class)).to eq([MachineMountsTest::PaymentStarted, MachineMountsTest::PaymentInfoAdded])
      expect(instance.context.card).to eq('tok')
      expect(instance.machine.in?(:payment, MachineMountsTest::PaymentStarted)).to be(true)
    end

    it "accepts the region's commands only where it is" do
      expect(after(placed).handles_command?(msg(MachineMountsTest::Capture))).to be(false)
      expect(after(placed, started).handles_command?(msg(MachineMountsTest::Capture))).to be(true)
    end

    it 'runs entry actions and starts timers' do
      dispatched = after(placed, started, info).__react(started)

      charge, expire = dispatched
      expect(charge).to be_a(MachineMountsTest::ChargeCard)
      expect(charge.payload.card).to eq('tok')
      expect(expire).to be_a(MachineMountsTest::ExpirePayment)
      expect(expire.created_at).to eq(started.created_at + 3600)
    end

    it 'exits innermost first when the checkpoint is left' do
      cancelled = msg(MachineMountsTest::OrderCancelled)
      dispatched = after(placed, started, cancelled).__react(cancelled)

      expect(dispatched.map(&:class)).to eq([MachineMountsTest::VoidPayment])
    end

    it 'joins once every region is final, in the deciding decision' do
      instance = after(placed, started, msg(MachineMountsTest::PaymentCaptured))
      events = instance.decide(msg(MachineMountsTest::Ship))

      expect(events.map(&:class)).to eq([MachineMountsTest::Shipped, MachineMountsTest::OrderCompleted])
    end
  end

  describe 'inline' do
    let(:machine) { MachineMountsTest::InlineOrder }
    it_behaves_like 'a payment region'
  end

  describe 'mounted' do
    let(:machine) { MachineMountsTest::MountedOrder }
    it_behaves_like 'a payment region'

    it "places the mounted machine's checkpoints in the tree" do
      region = machine.tree.regions[:payment]
      expect(region.machine).to eq(MachineMountsTest::PaymentMachine)
      expect(region.parent.key).to eq(MachineMountsTest::OrderPlaced)
      expect(machine.handled_commands).to include(MachineMountsTest::StartPayment, MachineMountsTest::Capture)
      expect(machine.handled_messages_for_evolve).to include(MachineMountsTest::PaymentInfoAdded, MachineMountsTest::PaymentExpired)
    end
  end

  describe 'on_done' do
    def after(*events) = MachineMountsTest::MountedOrder.new(order_id: 'o1').tap { |m| m.evolve(events) }

    let(:placed) { msg(MachineMountsTest::OrderPlaced) }
    let(:started) { msg(MachineMountsTest::PaymentStarted) }

    it "maps the mounted machine's final onto ours, in the deciding decision" do
      events = after(placed, started).decide(msg(MachineMountsTest::ExpirePayment))
      expect(events.map(&:class)).to eq([MachineMountsTest::PaymentExpired, MachineMountsTest::OrderCancelled])
    end

    it 'maps a final appended elsewhere when the machine claims it' do
      expired = msg(MachineMountsTest::PaymentExpired)
      expect(after(placed, started, expired).__decide_eventless.map(&:class)).to eq([MachineMountsTest::OrderCancelled])
    end
  end

  describe 'children of a mounted machine' do
    it 'spawns and tracks them as the parent' do
      machine = MachineMountsTest::OrderWithLines
      instance = machine.new(order_id: 'o1').tap { |m| m.evolve([msg(MachineMountsTest::OrderPlaced)]) }
      events = instance.decide(msg(MachineMountsTest::AddLine, line_id: 'l1'))

      expect(events.map(&:class)).to eq([MachineMountsTest::LineAdded])
      expect(instance.machine.children(:lines).map(&:keys)).to eq([{ line_id: 'l1' }])
      expect(machine.handled_messages).to include(MachineMountsTest::LineAdded, MachineMountsTest::LineCancelled)
    end
  end

  describe 'rendering' do
    let(:machine) { MachineMountsTest::MountedOrder }

    it 'includes the mounted region in to_h and to_mermaid' do
      states = machine.to_h[:states]
      order_placed = states.values.find { |s| s[:class] == MachineMountsTest::OrderPlaced.name }
      expect(order_placed[:type]).to eq('parallel')
      expect(order_placed[:states].keys).to eq(%w[payment shipping])
      expect(order_placed[:states]['payment'][:states].keys).to include('PaymentStarted', 'PaymentCaptured')

      expect(machine.to_mermaid).to include('MachineMountsTest_PaymentStarted --> MachineMountsTest_PaymentCaptured : Capture')
    end

    it "includes the mounted machine's commands and actions in the topology" do
      nodes = Sourced::Topology.build([machine])
      commands = nodes.select { |n| n.type == 'command' }.map(&:name)
      automations = nodes.select { |n| n.type == 'automation' }.map(&:name)

      expect(commands).to include(MachineMountsTest::StartPayment.name, MachineMountsTest::Capture.name)
      expect(automations).to include("actions(#{MachineMountsTest::PaymentStarted.name})")
    end
  end

  describe 'definition checks' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        partition_by :order_id
        initial { command MachineMountsTest::PlaceOrder => MachineMountsTest::OrderPlaced }
        instance_eval(&block)
      end
    end

    it 'rejects a mounted machine that targets a checkpoint outside itself' do
      escaping = Class.new(Sourced::Machine) do
        initial { command MachineMountsTest::ExpirePayment => MachineMountsTest::OrderCancelled }
      end
      parent = machine do
        checkpoint(MachineMountsTest::OrderPlaced) { region :payment, escaping }
        final MachineMountsTest::OrderCancelled
      end

      expect { parent.validate! }.to raise_error(Sourced::Machine::DefinitionError, /OrderCancelled, which isn't a checkpoint/)
    end

    it 'rejects reactions and sync blocks on a mounted machine' do
      reacting = Class.new(Sourced::Machine) do
        reaction(MachineMountsTest::PaymentStarted) { |_context, _evt| nil }
      end
      syncing = Class.new(Sourced::Machine) { sync { |**| nil } }

      expect { machine { checkpoint(MachineMountsTest::OrderPlaced) { region :payment, reacting } } }
        .to raise_error(Sourced::Machine::DefinitionError, /reactions or sync blocks/)
      expect { machine { checkpoint(MachineMountsTest::OrderPlaced) { region :payment, syncing } } }
        .to raise_error(Sourced::Machine::DefinitionError, /reactions or sync blocks/)
    end

    it 'rejects a machine mounted twice, and clashing checkpoints or region names' do
      twice = machine do
        checkpoint(MachineMountsTest::OrderPlaced) do
          region :payment, MachineMountsTest::PaymentMachine
          region :refund, MachineMountsTest::PaymentMachine
        end
      end
      expect { twice.validate! }.to raise_error(Sourced::Machine::DefinitionError, /mounted twice/)

      clash = machine do
        checkpoint(MachineMountsTest::OrderPlaced) { region :payment, MachineMountsTest::PaymentMachine }
        checkpoint MachineMountsTest::PaymentCaptured
      end
      expect { clash.validate! }.to raise_error(Sourced::Machine::DefinitionError, /PaymentCaptured is declared twice/)

      expect do
        machine do
          checkpoint(MachineMountsTest::OrderPlaced) do
            region(:payment) { initial }
            region :payment, MachineMountsTest::PaymentMachine
          end
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /region :payment is declared twice/)
    end

    it 'rejects on_done keys that are not root finals, and unreachable targets' do
      not_final = machine do
        checkpoint(MachineMountsTest::OrderPlaced) do
          region :payment, MachineMountsTest::PaymentMachine, on_done: { MachineMountsTest::PaymentStarted => MachineMountsTest::OrderCancelled }
        end
        final MachineMountsTest::OrderCancelled
      end
      expect { not_final.validate! }.to raise_error(Sourced::Machine::DefinitionError, /PaymentStarted isn't a final checkpoint/)

      unknown = machine do
        checkpoint(MachineMountsTest::OrderPlaced) do
          region :payment, MachineMountsTest::PaymentMachine, on_done: { MachineMountsTest::PaymentExpired => MachineMountsTest::OrderCancelled }
        end
      end
      expect { unknown.validate! }.to raise_error(Sourced::Machine::DefinitionError, /OrderCancelled, which isn't a checkpoint/)
    end

    it 'rejects a block with a machine, and on_done without one' do
      expect do
        machine { checkpoint(MachineMountsTest::OrderPlaced) { region(:payment, MachineMountsTest::PaymentMachine) { initial } } }
      end.to raise_error(Sourced::Machine::DefinitionError, /a machine or a block, not both/)
      expect do
        machine { checkpoint(MachineMountsTest::OrderPlaced) { region(:payment, on_done: { 1 => 2 }) { initial } } }
      end.to raise_error(Sourced::Machine::DefinitionError, /on_done is for a mounted machine/)
    end
  end
end
