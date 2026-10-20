# frozen_string_literal: true

require 'spec_helper'
require 'sequel'
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

  PassCheck = command(:pass_check, :check_id)

  class OrderState
    attr_accessor :card
  end

  # MountedOrder::PaymentMachine's chart, for other owners' payment parts:
  # its context is the owner's.
  def self.payment_chart(part)
    part.class_exec do
      start = const_set(:StartPayment, Command(:start_payment) { attribute :card, String })
      added = const_set(:PaymentInfoAdded, Event(:payment_info_added) { attribute :card, String })
      capture = const_set(:Capture, Command(:capture))
      expire = const_set(:ExpirePayment, Command(:expire_payment))

      evolve(added) { |order, evt| order.card = evt.payload.card }

      initial do
        command start => :authorizing do |_order, cmd|
          event added, card: cmd.payload.card
        end
      end

      state :authorizing do
        entry { |order, _evt| dispatch ChargeCard, card: order.card }
        exit { |_order, _evt| dispatch VoidPayment }
        after(3600) { |_order, _evt| dispatch expire }
        command capture => :captured
        command expire => :expired
      end

      final :captured
      final :expired
    end
  end

  class MountedOrder < Sourced::Machine
    partition_by :order_id
    context { OrderState.new }

    Placed = State(:placed)
    Completed = State(:completed)
    Cancelled = State(:cancelled)

    # The payment region's chart, spelled out (Topology reads its handlers' sources).
    class PaymentMachine < Sourced::Machine::Part
      part_of MountedOrder

      StartPayment = Command(:start_payment) { attribute :card, String }
      PaymentInfoAdded = Event(:payment_info_added) { attribute :card, String }
      Capture = Command(:capture)
      ExpirePayment = Command(:expire_payment)

      Authorizing = State(:authorizing)
      Captured = State(:captured)
      Expired = State(:expired)

      evolve(PaymentInfoAdded) { |order, evt| order.card = evt.payload.card }

      initial do
        command StartPayment => Authorizing do |_order, cmd|
          event PaymentInfoAdded, card: cmd.payload.card
        end
      end

      state Authorizing do
        entry { |order, _evt| dispatch ChargeCard, card: order.card }
        exit { |_order, _evt| dispatch VoidPayment }
        after(3600) { |_order, _evt| dispatch ExpirePayment }
        command Capture => Captured
        command ExpirePayment => Expired
      end

      final Captured
      final Expired
    end

    initial { command PlaceOrder => Placed }

    state Placed do
      command CancelOrder => Cancelled
      region :payment, PaymentMachine, on_final: { PaymentMachine::Expired => Cancelled }
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

    Placed = State(:placed)
    Authorizing = State(:authorizing)
    Completed = State(:completed)
    Cancelled = State(:cancelled)

    initial { command PlaceOrder => Placed }

    state Placed do
      command CancelOrder => Cancelled
      region :payment do
        initial do
          command StartPayment => Authorizing do |_order, cmd|
            event PaymentInfoAdded, card: cmd.payload.card
          end
        end

        state Authorizing do
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

  # Its payment part mounted twice: a deposit while placed, and a balance once shipped.
  class TwicePaidOrder < Sourced::Machine
    partition_by :order_id
    context { OrderState.new }

    Placed = State(:placed)
    Shipped = State(:shipped)

    class PaymentMachine < Sourced::Machine::Part
      part_of TwicePaidOrder
      MachineMountsTest.payment_chart(self)
    end

    initial { command PlaceOrder => Placed }

    state Placed do
      region :deposit, PaymentMachine
      region :balance do
        initial { command Ship => Shipped }
        state Shipped do
          region :payment, PaymentMachine
        end
      end
    end
  end

  # A child machine a part's handler starts.
  class LineMachine < Sourced::Machine
    partition_by :order_id, :line_id

    Added = State(:added)

    started Added
    stopped :cancelled
  end

  class OrderWithLines < Sourced::Machine
    partition_by :order_id

    Placed = State(:placed)

    class LinesRegion < Sourced::Machine::Part
      part_of OrderWithLines

      AddLine = Command(:add_line) { attribute :line_id, String }

      initial do
        command(AddLine, starts: LineMachine) { |_order, cmd| start LineMachine, line_id: cmd.payload.line_id }
      end
    end

    initial { command PlaceOrder => Placed }
    state Placed do
      region :lines, LinesRegion
    end
  end

  # A child machine a part's state starts.
  class CheckMachine < Sourced::Machine
    partition_by :order_id, :check_id

    Running = State(:running)
    Passed = State(:passed)

    started Running do
      command PassCheck => Passed
    end
    final Passed
    stopped :cancelled
  end

  class OrderWithChecks < Sourced::Machine
    partition_by :order_id

    Placed = State(:placed)

    class ChecksRegion < Sourced::Machine::Part
      part_of OrderWithChecks

      StartCheck = Command(:start_check)

      initial { command StartCheck => :checking }
      state :checking do
        start CheckMachine do
          payload { |_order, _state| { check_id: 'c1' } }
          on_done CheckMachine::Passed => :checked
        end
      end
      final :checked
    end

    initial { command PlaceOrder => Placed }
    state Placed do
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

  # +pay+: where the payment region's messages are.
  shared_examples 'a payment region' do
    let(:place) { msg(MM::PlaceOrder) }
    let(:start) { msg(pay::StartPayment, card: 'tok') }

    it 'activates the region with its state' do
      expect(decided(machine, place).machine.value).to eq(
        machine::Placed => { payment: :initial, shipping: :initial }
      )
    end

    it 'decides in the region, and evolves its data into the context' do
      instance = decided(machine, place)
      events = instance.decide(start)

      expect(summary(events)).to eq(
        summary([msg(pay::PaymentInfoAdded, card: 'tok')]) +
        moves(machine, [pay::StartPayment.type, { from: 'placed.payment.initial', to: 'placed.payment.authorizing' }])
      )
      expect(authorizing === events.last).to be(true)
      expect(instance.context.card).to eq('tok')
      expect(instance.machine.in?(:payment, authorizing)).to be(true)
      expect(instance.machine.in?('placed.payment.authorizing')).to be(true)
    end

    it "accepts the region's commands only where it is" do
      expect(decided(machine, place).handles_command?(msg(pay::Capture))).to be(false)
      expect(decided(machine, place, start).handles_command?(msg(pay::Capture))).to be(true)
    end

    it 'runs entry actions and starts timers' do
      instance = decided(machine, place)
      transitioned = instance.decide(start).last
      charge, expire = instance.__react(transitioned)

      expect(charge).to be_a(MM::ChargeCard)
      expect(charge.payload.card).to eq('tok')
      expect(expire).to be_a(pay::ExpirePayment)
      expect(expire.created_at).to eq(transitioned.created_at + 3600)
      expect(expire.metadata[Sourced::Machine::ENTERED_BY]).to eq(transitioned.id)
    end

    it 'exits innermost first when the state is left' do
      instance = decided(machine, place, start)
      cancelled = instance.decide(msg(MM::CancelOrder))

      expect(summary(cancelled)).to eq(moves(machine, [
        'machine_mounts_test.cancel_order',
        { from: 'placed', to: 'cancelled', exited: %w[placed.payment.authorizing placed] }
      ]))
      expect(instance.__react(cancelled.last).map(&:class)).to eq([MM::VoidPayment])
    end

    it 'joins once every region is final, in the deciding decision' do
      events = decided(machine, place, start, msg(pay::Capture)).decide(msg(MM::Ship))

      expect(summary(events)).to eq(moves(
        machine,
        ['machine_mounts_test.ship', { from: 'placed.shipping.initial', to: 'placed.shipping.shipped' }],
        ['done', { from: 'placed', to: 'completed', exited: %w[placed.payment.captured placed.shipping.shipped placed] }]
      ))
    end
  end

  describe 'inline' do
    let(:machine) { MM::InlineOrder }
    let(:pay) { MM }
    let(:authorizing) { MM::InlineOrder::Authorizing }

    it_behaves_like 'a payment region'
  end

  describe 'mounted' do
    let(:machine) { MM::MountedOrder }
    let(:pay) { MM::MountedOrder::PaymentMachine }
    let(:authorizing) { MM::MountedOrder::PaymentMachine::Authorizing }

    it_behaves_like 'a payment region'

    it "places the part's states in the tree" do
      region = machine.tree.root.nodes.fetch(MM::MountedOrder::Placed).regions.find { |r| r.name == :payment }

      expect(region.machine).to eq(pay)
      expect(region.parent.key).to eq(MM::MountedOrder::Placed)
      expect(region.nodes.keys).to eq([:initial, pay::Authorizing, pay::Captured, pay::Expired])
      expect(machine.handled_commands).to include(pay::StartPayment, pay::Capture)
      expect(machine.handled_messages_for_evolve).to include(pay::PaymentInfoAdded, authorizing)
    end

    it "types the part's messages and states under the owner's prefix" do
      expect(pay::StartPayment.type).to eq('machine_mounts_test.mounted_order.payment.start_payment')
      expect(authorizing.type).to eq('machine_mounts_test.mounted_order.runtime.payment.authorizing')
      expect(machine.transition_classes).to include(authorizing, pay::Captured, pay::Expired)
    end
  end

  describe 'on_final' do
    let(:pay) { MM::MountedOrder::PaymentMachine }

    it "maps the part's final onto ours, in the deciding decision" do
      events = decided(MM::MountedOrder, msg(MM::PlaceOrder), msg(pay::StartPayment, card: 'tok')).decide(msg(pay::ExpirePayment))

      expect(summary(events)).to eq(moves(
        MM::MountedOrder,
        [pay::ExpirePayment.type, { from: 'placed.payment.authorizing', to: 'placed.payment.expired', exited: %w[placed.payment.authorizing] }],
        ['on_final', { from: 'placed.payment.expired', to: 'cancelled', exited: %w[placed.payment.expired placed] }]
      ))
    end
  end

  describe 'the same part mounted twice' do
    let(:machine) { MM::TwicePaidOrder }
    let(:pay) { MM::TwicePaidOrder::PaymentMachine }
    let(:authorizing) { pay::Authorizing }

    it 'is one consumer group: it registers with a router' do
      router = Sourced::Router.new(store: Sourced::Store.new(Sequel.sqlite).tap(&:install!), reactors: [machine]).setup!
      expect(router.reactors).to eq([machine])
    end

    it 'places its states at two paths' do
      expect(machine.tree.placements(authorizing).map(&:path))
        .to eq(%w[placed.deposit.authorizing placed.balance.shipped.payment.authorizing])
    end

    it 'keeps an independent position at each path' do
      instance = decided(machine, msg(MM::PlaceOrder), msg(pay::StartPayment, card: 'dep'), msg(MM::Ship))
      expect(instance.machine.value).to eq(
        MM::TwicePaidOrder::Placed => { deposit: authorizing, balance: { MM::TwicePaidOrder::Shipped => { payment: :initial } } }
      )

      events = instance.decide(msg(pay::StartPayment, card: 'bal'))
      expect(events.map(&:class)).to eq([pay::PaymentInfoAdded, authorizing])
      expect(events.last.payload.target).to eq('placed.balance.shipped.payment.authorizing')
      expect(instance.machine.in?('placed.deposit.authorizing')).to be(true)
      expect(instance.machine.in?(:payment, authorizing)).to be(true)
      expect(instance.machine.entered_by(:payment)).to equal(events.last)
      expect(instance.machine.entered_by(:deposit)).not_to equal(events.last)
    end

    it 'routes a command both accept to both regions, one transition each' do
      instance = decided(machine, msg(MM::PlaceOrder), msg(pay::StartPayment, card: 'dep'), msg(MM::Ship), msg(pay::StartPayment, card: 'bal'))
      events = instance.decide(msg(pay::Capture))

      expect(summary(events)).to eq(moves(
        machine,
        [pay::Capture.type, { from: 'placed.deposit.authorizing', to: 'placed.deposit.captured', exited: %w[placed.deposit.authorizing] }],
        [pay::Capture.type, {
          from: 'placed.balance.shipped.payment.authorizing', to: 'placed.balance.shipped.payment.captured',
          exited: %w[placed.balance.shipped.payment.authorizing]
        }]
      ))
      expect(instance.__react(events.first).map(&:class)).to eq([MM::VoidPayment])
    end
  end

  describe "children of a part" do
    let(:lines) { MM::OrderWithLines::LinesRegion }

    it 'are registered with the owner' do
      expect(MM::OrderWithLines.to_sourced_reactors).to eq([MM::OrderWithLines, MM::LineMachine])
      expect(MM::OrderWithChecks.to_sourced_reactors).to eq([MM::OrderWithChecks, MM::CheckMachine])
    end

    it 'starts and tracks them as the owner' do
      machine = MM::OrderWithLines
      instance = decided(machine, msg(MM::PlaceOrder))
      events = instance.decide(msg(lines::AddLine, line_id: 'l1'))

      expect(summary(events)).to eq(summary([MM::LineMachine::Started.new(payload: {
        order_id: 'o1', line_id: 'l1', input: {}, trigger: 'start', source: 'initial', target: 'added', exited: [], entered: %w[added]
      })]))
      expect(instance.machine.children(MM::LineMachine).map(&:keys)).to eq([{ line_id: 'l1' }])
      expect(instance.machine.children(MM::LineMachine).map(&:state)).to eq([MM::LineMachine::Added])
      expect(machine.handled_messages).to include(MM::LineMachine::Started, MM::LineMachine::Finished)
    end
  end

  describe "children a part's state starts" do
    let(:checks) { MM::OrderWithChecks::ChecksRegion }

    it 'starts the child when its state is entered, and maps its final' do
      instance = decided(MM::OrderWithChecks, msg(MM::PlaceOrder))
      events = instance.decide(msg(checks::StartCheck))
      expect(events.map(&:class)).to eq([checks::Checking, MM::CheckMachine::Started])
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
    let(:pay) { MM::MountedOrder::PaymentMachine }

    it 'includes the mounted region in to_h and to_mermaid' do
      placed = machine.to_h[:states]['placed']
      expect(placed[:type]).to eq('parallel')
      expect(placed[:states].keys).to eq(%w[payment shipping])
      expect(placed[:states]['payment'][:states].keys).to include('authorizing', 'captured')
      expect(placed[:states]['payment'][:states]['authorizing'][:machine]).to eq(pay.name)

      expect(machine.to_mermaid).to include('placed_payment_authorizing --> placed_payment_captured : Capture')
    end

    it 'includes the on_final mappings in to_h and to_mermaid' do
      expect(machine.to_h[:states]['placed'][:mounts]).to eq([{
        region: 'payment', src: pay.name, on_final: { 'placed.payment.expired' => 'cancelled' }
      }])
      expect(machine.to_mermaid).to include('placed_payment_expired --> cancelled : on_final')
    end

    it "includes the part's commands and actions in the topology" do
      nodes = Sourced::Topology.build([machine])
      commands = nodes.select { |n| n.type == 'command' }
      automations = nodes.select { |n| n.type == 'automation' }.map(&:name)

      expect(commands.map(&:name)).to include(pay::StartPayment.name, pay::Capture.name)
      expect(commands.find { |n| n.name == pay::StartPayment.name }.produces)
        .to contain_exactly(pay::Authorizing.type, pay::PaymentInfoAdded.type)
      expect(automations).to include('actions(placed.payment.authorizing)')
    end
  end

  describe 'definition checks' do
    def parent
      @parent ||= stub_const('MachineMountsSpecParent', Class.new(Sourced::Machine) do
        partition_by :order_id
        consumer_group 'machine_mounts_spec_parent'
        State(:placed)
        initial { command MachineMountsTest::PlaceOrder => :placed }
      end)
    end

    def part(prefix = 'payment', &block)
      owner = parent
      Class.new(Sourced::Machine::Part) do
        part_of owner
        part_prefix prefix
        class_exec(&block)
      end
    end

    def payment = @payment ||= part { MachineMountsTest.payment_chart(self) }

    # The parent, with +block+ evaluated in its class body.
    def machine(&block) = parent.tap { |klass| klass.class_eval(&block) }

    it 'rejects a part that targets a state outside itself' do
      placed = parent::Placed
      outward = part('outward') { initial { command MachineMountsTest::PlaceOrder => placed } }
      parent.class_eval { state(placed) { region :payment, outward } }

      expect { parent.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /another machine's state that isn't below it/)
    end

    it 'rejects mounting a machine, pointing to parts and children' do
      mounted = stub_const('MachineMountsTest::Mounted', Class.new(Sourced::Machine) do
        reaction(MachineMountsTest::Ship) { |_context, _evt| nil }
      end)

      expect { machine { state(:placed) { region :payment, mounted } } }
        .to raise_error(
          Sourced::Machine::DefinitionError,
          /region :payment: can't mount MachineMountsTest::Mounted, a Sourced::Machine: .*Sourced::Machine::Part \(part_of MachineMountsSpecParent\).*child \(start, starts:\)/
        )
      expect { machine { state(:other, mounted) } }
        .to raise_error(Sourced::Machine::DefinitionError, /can't mount MachineMountsTest::Mounted, a Sourced::Machine/)
    end

    it "rejects another owner's part" do
      expect { machine { state(:placed) { region :payment, MachineMountsTest::MountedOrder::PaymentMachine } } }
        .to raise_error(Sourced::Machine::DefinitionError, /part of MachineMountsTest::MountedOrder, so it mounts only in/)
    end

    it "rejects a part's states, and clashing region names" do
      pay = payment
      expect do
        machine do
          state(:placed) { region :payment, pay }
          state pay::Captured
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /'s state, not MachineMountsSpecParent's/)

      expect do
        machine do
          state(:shipped) do
            region(:payment) { initial }
            region :payment, pay
          end
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /region :payment is declared twice/)
    end

    it "mounts a part whose handlers start children twice, sharing the child" do
      lines = part('lines') do
        initial do
          command(MachineMountsTest::Ship, starts: MachineMountsTest::LineMachine) do |_order, _cmd|
            start MachineMountsTest::LineMachine, line_id: 'l1'
          end
        end
      end
      twice = machine do
        state(:placed) do
          region :lines, lines
          region :more_lines, lines
        end
      end

      expect { twice.validate! }.not_to raise_error
      expect(twice.to_sourced_reactors).to eq([twice, MachineMountsTest::LineMachine])
    end

    it 'accepts a part mounted twice whose state starts a child: one start per placement, rendered at each' do
      checks = part('checks') do
        initial { command MachineMountsTest::Ship => :checking }
        state :checking do
          start(MachineMountsTest::CheckMachine) do
            payload { |_order, state| { check_id: state.payload.target } }
            on_done MachineMountsTest::CheckMachine::Passed => :checked
          end
        end
        final :checked
      end
      twice = machine do
        state(:placed) do
          region :checks, checks
          region :more_checks, checks
        end
      end

      expect(twice.to_sourced_reactors).to eq([twice, MachineMountsTest::CheckMachine])
      expect(twice.tree.children_specs.map { |spec| spec.node.path }).to eq(%w[placed.checks.checking placed.more_checks.checking])

      chart = twice.to_h
      expect(chart[:children].map { |child| child[:state] }).to eq(%w[placed.checks.checking placed.more_checks.checking])
      %w[checks more_checks].each do |region|
        expect(chart[:states]['placed'][:states][region][:states]['checking'][:invoke])
          .to eq([{ id: MachineMountsTest::CheckMachine.group_id, src: MachineMountsTest::CheckMachine.name,
                    on_done: { 'passed' => "placed.#{region}.checked" } }])
        expect(twice.to_mermaid).to include("placed_#{region}_checking --> placed_#{region}_checked : CheckMachine passed")
      end

      automations = Sourced::Topology.build([twice]).select { |n| n.type == 'automation' }.map(&:id)
      group = MachineMountsTest::CheckMachine.group_id
      %w[checks more_checks].each do |region|
        expect(automations.grep(/\Aplaced\.#{region}\.checking-(start|stop|on-done)-#{group}-/).size).to eq(3)
      end
    end

    it 'rejects on_final keys that are not root finals, and unreachable targets' do
      pay = payment
      not_final = machine do
        state(:placed) do
          region :payment, pay, on_final: { pay::Authorizing => :cancelled }
        end
        final :cancelled
      end
      expect { not_final.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Authorizing isn't a final state/)
    end

    it 'rejects on_final targets out of reach' do
      pay = payment
      elsewhere = machine do
        state(:placed) do
          region :payment, pay, on_final: { pay::Expired => :refunded }
        end
        state(:returned) { region(:refund) { final :refunded } }
      end
      expect { elsewhere.validate! }.to raise_error(Sourced::Machine::DefinitionError, /can only reach/)
    end

    it 'rejects on_final targets never placed' do
      pay = payment
      unplaced = machine do
        state(:placed) do
          region :payment, pay, on_final: { pay::Expired => :cancelled }
        end
      end
      expect { unplaced.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Cancelled declared but never placed/)
    end

    it 'rejects a block with a part, and on_final without one' do
      pay = payment
      expect do
        machine { state(:placed) { region(:payment, pay) { initial } } }
      end.to raise_error(Sourced::Machine::DefinitionError, /a part or a block, not both/)
      expect do
        machine { state(:shipped) { region(:payment, on_final: { 1 => 2 }) { initial } } }
      end.to raise_error(Sourced::Machine::DefinitionError, /on_final is for a mounted part/)
    end
  end
end
