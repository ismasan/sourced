# frozen_string_literal: true

require 'spec_helper'
require 'sequel'
require_relative 'support/machine_helpers'

module MachineTest
  OpenCart = Sourced::Command.define('machine_test.open_cart') do
    attribute :cart_id, String
    attribute :currency, String
    attribute :channel, Sourced::Types::String.default('web'.freeze)
  end
  CartOpened = Sourced::Event.define('machine_test.cart_opened') do
    attribute :cart_id, String
    attribute :currency, String
  end
  AddItem = Sourced::Command.define('machine_test.add_item') do
    attribute :cart_id, String
    attribute :sku, String
    attribute :qty, Integer
    attribute :unit_price, Integer
  end
  ItemAdded = Sourced::Event.define('machine_test.item_added') do
    attribute :cart_id, String
    attribute :sku, String
    attribute :qty, Integer
    attribute :unit_price, Integer
  end
  Checkout = Sourced::Command.define('machine_test.checkout') do
    attribute :cart_id, String
  end
  CheckedOut = Sourced::Event.define('machine_test.checked_out') do
    attribute :cart_id, String
    attribute :total, Integer
  end
  CheckoutRejected = Sourced::Event.define('machine_test.checkout_rejected') do
    attribute :cart_id, String
    attribute :reason, String
  end
  Reopen = Sourced::Command.define('machine_test.reopen') do
    attribute :cart_id, String
  end
  Pay = Sourced::Command.define('machine_test.pay') do
    attribute :cart_id, String
  end
  SendReceipt = Sourced::Command.define('machine_test.send_receipt') do
    attribute :cart_id, String
    attribute :total, Integer
  end
  ReleaseStock = Sourced::Command.define('machine_test.release_stock') do
    attribute :cart_id, String
    attribute :skus, [String]
  end

  Ping = Sourced::Command.define('machine_test.ping') do
    attribute :cart_id, String
    attribute :note, String
  end
  NoteTaken = Sourced::Event.define('machine_test.note_taken') do
    attribute :cart_id, String
    attribute :note, String
  end
  Notify = Sourced::Command.define('machine_test.notify') do
    attribute :cart_id, String
    attribute :note, Sourced::Types::String.nullable
  end

  Expire = Sourced::Command.define('machine_test.expire') do
    attribute :cart_id, String
  end

  # Not a valid command for a cart machine: it lacks cart_id
  Unkeyed = Sourced::Command.define('machine_test.unkeyed') do
    attribute :order_id, String
  end

  class CartState
    attr_reader :items
    attr_accessor :currency

    def initialize
      @items = {}
    end

    def add(sku, qty, unit_price) = @items[sku] = { qty:, unit_price: }
    def empty? = @items.empty?
    def total = @items.values.sum { |i| i[:qty] * i[:unit_price] }
  end

  class CartMachine < Sourced::Machine
    partition_by :cart_id

    Open     = State(:open)
    Rejected = State(:rejected)
    Ordered  = State(:ordered)
    Paid     = State(:paid)

    context { CartState.new }
    evolve(CartOpened) { |cart, evt| cart.currency = evt.payload.currency }
    evolve(ItemAdded) { |cart, evt| cart.add(evt.payload.sku, evt.payload.qty, evt.payload.unit_price) }

    initial do
      command OpenCart => Open do |_cart, cmd|
        event CartOpened, currency: cmd.payload.currency
      end
    end

    state Open do
      exit { |cart, _transition| dispatch ReleaseStock, skus: cart.items.keys }

      command AddItem do |_cart, cmd|
        event ItemAdded, sku: cmd.payload.sku, qty: cmd.payload.qty, unit_price: cmd.payload.unit_price
      end

      command Checkout => Ordered | Rejected do |cart, _cmd|
        if cart.empty?
          event CheckoutRejected, reason: 'empty'
          to Rejected
        else
          event CheckedOut, total: cart.total
          to Ordered
        end
      end
    end

    state Rejected do
      command Reopen => Open
    end

    state Ordered do
      entry { |cart, _transition| dispatch SendReceipt, total: cart.total }
      command Pay => Paid
    end

    final Paid
  end

  class TimerMachine < Sourced::Machine
    partition_by :cart_id

    Open    = State(:open)
    Ordered = State(:ordered)
    Expired = State(:expired)

    initial do
      command OpenCart => Open
    end

    state Open do
      after(3600) { |_cart, _transition| dispatch Expire }
      command Expire => Expired
      command Checkout => Ordered
      command Reopen => Open # re-entry
    end

    state Ordered do
      command Expire => Expired # accepted here too: a stale timer must still be refused
    end

    final Expired
  end

  # Entry actions see the data events of the decision that entered the state.
  class PingMachine < Sourced::Machine
    partition_by :cart_id

    context { { note: nil } }
    evolve(NoteTaken) { |s, evt| s[:note] = evt.payload.note }

    initial do
      command Ping => :pinged do |_s, cmd|
        event NoteTaken, note: cmd.payload.note
      end
    end

    state :pinged do
      entry { |s, _transition| dispatch Notify, note: s[:note] }
    end
  end

RSpec.describe Sourced::Machine do
  include Sourced::Testing::RSpec
  include MachineHelpers

  # A throwaway machine over the cart messages, for checking one feature at a
  # time. Named before partition_by, so it gets its Transitioned.
  def machine(&block)
    klass = Class.new(Sourced::Machine)
    @throwaways = (@throwaways || 0) + 1
    stub_const("MachineTest::Throwaway#{@throwaways}", klass)
    klass.class_eval do
      partition_by :cart_id
      class_eval(&block)
    end
    klass
  end

  let(:cart_opened) { CartOpened.new(payload: { cart_id: 'c1', currency: 'EUR' }) }
  let(:opened) { moved(CartMachine, 'machine_test.open_cart', from: 'initial', to: 'open', cart_id: 'c1') }
  let(:item_added) { ItemAdded.new(payload: { cart_id: 'c1', sku: 'A1', qty: 2, unit_price: 50 }) }
  let(:ordered) { moved(CartMachine, 'machine_test.checkout', from: 'open', to: 'ordered', exited: %w[open], cart_id: 'c1') }

  describe 'routing' do
    it 'claims every accepted command and its transitions' do
      expect(CartMachine.handled_commands).to contain_exactly(OpenCart, AddItem, Checkout, Reopen, Pay)
      expect(CartMachine.handled_messages).to include(CartMachine::Open, CartMachine::Rejected, CartMachine::Ordered, CartMachine::Paid)
      expect(CartMachine.handled_messages).not_to include(CartOpened, ItemAdded, CheckedOut)
    end

    it 'evolves its transitions and evolve types' do
      expect(CartMachine.handled_messages_for_evolve).to contain_exactly(
        CartMachine::Open, CartMachine::Rejected, CartMachine::Ordered, CartMachine::Paid, CartOpened, ItemAdded
      )
    end
  end

  describe 'state classes' do
    it 'are events typed under the runtime segment, with the partition keys and the transition attributes' do
      expect(CartMachine::Open).to be < Sourced::Machine::State
      expect(CartMachine::Open.type).to eq('machine_test.cart_machine.runtime.open')
      expect(CartMachine::Open.payload_attribute_names)
        .to contain_exactly(:cart_id, :trigger, :source, :target, :exited, :entered)
      expect(Sourced::Message.registry['machine_test.cart_machine.runtime.open']).to equal(CartMachine::Open)
    end

    it 'are their own handles' do
      expect(CartMachine::Open).to have_attributes(
        state: CartMachine::Open, machine: CartMachine, state_name: :open
      )
      expect(CartMachine.transition_classes)
        .to eq([CartMachine::Open, CartMachine::Rejected, CartMachine::Ordered, CartMachine::Paid])
    end

    it 'build unions with |, nil last' do
      targets = CartMachine::Open | CartMachine::Paid | nil
      expect(targets.members).to eq([CartMachine::Open, CartMachine::Paid, nil])
    end

    it 'sets a constant for a state declared by Symbol' do
      m = machine do
        initial { command Ping => :awaiting_payment }
        state :awaiting_payment
      end
      expect(m::AwaitingPayment).to equal(m.declared_states[:awaiting_payment])
      expect(m::AwaitingPayment.type).to eq("#{m.type_prefix}.runtime.awaiting_payment")
    end

    it 'take the type prefix from consumer_group, so the class can be renamed' do
      m = Class.new(Sourced::Machine) do
        partition_by :cart_id
        consumer_group 'renamed.carts'
        State(:open)
      end
      expect(m.declared_states[:open].type).to eq('renamed.carts.runtime.open')
    end

    it 'require partition_by and consumer_group before them' do
      expect do
        Class.new(Sourced::Machine) do
          consumer_group 'machine_test.late_keys'
          State(:open)
          partition_by :cart_id
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /declare partition_by before states and mounts/)
      expect do
        machine do
          State(:open)
          consumer_group 'machine_test.late_group'
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /declare consumer_group before states and mounts/)
    end

    it "can't be declared once the machine is validated" do
      m = machine do
        initial { command Ping => :pinged }
        state :pinged
      end
      m.validate!
      expect { m.State(:late) }.to raise_error(Sourced::Machine::DefinitionError, /declared after the machine was validated/)
    end

    it "rejects context, evolve, before_evolve and reaction once the machine is validated, which it wouldn't read" do
      m = machine do
        initial { command Ping => :pinged }
        state :pinged
      end
      m.validate!

      late = Sourced::Event.define('machine_test.late') { attribute :cart_id, String }
      [
        -> { m.context { {} } },
        -> { m.evolve(late) { |_s, _e| nil } },
        -> { m.evolve(Class.new(Sourced::Projection)) },
        -> { m.before_evolve { |_s, _e| nil } },
        -> { m.reaction(late) { |_s, _e| nil } }
      ].each do |declare|
        expect(&declare).to raise_error(Sourced::Machine::DefinitionError, /declared after the machine was validated/)
      end
      expect(m.handled_messages_for_evolve).not_to include(late)
    end

    it 'locks a part once its owner is validated' do
      parent = machine { initial { command Ping => :pinged } }
      part = Class.new(Sourced::Machine::Part) do
        part_of parent
        part_prefix 'locked_region'
        started :waiting
      end
      parent.state(:pinged) { region :sub, part }
      parent.validate!

      late = Sourced::Event.define('machine_test.late_mounted') { attribute :cart_id, String }
      expect { part.evolve(late) { |_s, _e| nil } }
        .to raise_error(Sourced::Machine::DefinitionError, /evolve declared after .* was validated/)
    end

    it 'reject names with dots' do
      expect { machine { State(:'a.b') } }.to raise_error(Sourced::Machine::DefinitionError, /can't contain dots/)
    end

    it "reject a type another message already has" do
      Sourced::Event.define('machine_test.taken.runtime.open') { attribute :cart_id, String }
      expect do
        Class.new(Sourced::Machine) do
          partition_by :cart_id
          consumer_group 'machine_test.taken'
          State(:open)
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /"machine_test.taken.runtime.open" is already/)
    end

    it 'are redefined by a machine with the same group_id, as when reloading code' do
      define = -> { Class.new(Sourced::Machine) { partition_by :cart_id; consumer_group 'machine_test.reloaded'; State(:open) } }
      first = define.().declared_states[:open]
      second = define.().declared_states[:open]
      expect(Sourced::Message.registry['machine_test.reloaded.runtime.open']).to equal(second)
      expect(second).not_to equal(first)
    end

    it 'keeps its transitions out of its projection, so adopting one keeps them' do
      items = Class.new(Sourced::Projection) do
        context { CartState.new }
        evolve(ItemAdded) { |cart, evt| cart.add(evt.payload.sku, evt.payload.qty, evt.payload.unit_price) }
      end
      m = machine do
        initial { command OpenCart => :open }
        final :open
        evolve items
      end

      expect(m.projection_class.handled_messages).to eq([ItemAdded])
      expect(m.handled_messages_for_evolve).to contain_exactly(ItemAdded, m::Open)
    end
  end

  describe 'command forms' do
    it 'moves to a single declared state, after the data events its block appends' do
      with_reactor(CartMachine, cart_id: 'c1')
        .when(OpenCart, cart_id: 'c1', currency: 'EUR', channel: 'app')
        .then(cart_opened, opened)
    end

    it 'moves to a single declared state without a block' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(ordered)
        .when(Pay, cart_id: 'c1')
        .then(moved(CartMachine, 'machine_test.pay', from: 'ordered', to: 'paid', exited: %w[ordered], cart_id: 'c1'))
    end

    it 'appends data events without moving the machine (no arrow)' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(AddItem, cart_id: 'c1', sku: 'A1', qty: 2, unit_price: 50)
        .then(ItemAdded, cart_id: 'c1', sku: 'A1', qty: 2, unit_price: 50)
    end

    it 'moves to the state the handler chooses from a union' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(item_added)
        .when(Checkout, cart_id: 'c1')
        .then(CheckedOut.new(payload: { cart_id: 'c1', total: 100 }), ordered)

      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(Checkout, cart_id: 'c1')
        .then(
          CheckoutRejected.new(payload: { cart_id: 'c1', reason: 'empty' }),
          moved(CartMachine, 'machine_test.checkout', from: 'open', to: 'rejected', exited: %w[open], cart_id: 'c1')
        )
    end

    it 'moves back to a state it has left' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(moved(CartMachine, 'machine_test.checkout', from: 'open', to: 'rejected', exited: %w[open], cart_id: 'c1'))
        .when(Reopen, cart_id: 'c1')
        .then(moved(CartMachine, 'machine_test.reopen', from: 'rejected', to: 'open', exited: %w[rejected], cart_id: 'c1'))
    end

    it 'may stay when the union includes nil' do
      m = machine do
        pinged = State(:pinged)
        initial do
          command Ping => pinged | nil do |_s, cmd|
            event NoteTaken, note: cmd.payload.note
            to :pinged if cmd.payload.note == 'go'
          end
        end
        state :pinged
      end

      with_reactor(m, cart_id: 'c1').when(Ping, cart_id: 'c1', note: 'wait')
        .then(NoteTaken, cart_id: 'c1', note: 'wait')
      with_reactor(m, cart_id: 'c1').when(Ping, cart_id: 'c1', note: 'go')
        .then(
          NoteTaken.new(payload: { cart_id: 'c1', note: 'go' }),
          moved(m, 'machine_test.ping', from: 'initial', to: 'pinged', cart_id: 'c1')
        )
    end

    it 'takes targets as an array of symbols' do
      m = machine do
        initial do
          command(Ping => [:pinged, nil]) { |_s, cmd| to :pinged if cmd.payload.note == 'go' }
        end
        state :pinged
      end

      with_reactor(m, cart_id: 'c1').when(Ping, cart_id: 'c1', note: 'wait').then([])
      with_reactor(m, cart_id: 'c1').when(Ping, cart_id: 'c1', note: 'go')
        .then(moved(m, 'machine_test.ping', from: 'initial', to: 'pinged', cart_id: 'c1'))
    end

    it "raises when a union handler doesn't choose a target" do
      m = machine do
        pinged = State(:pinged)
        paid = State(:paid)
        initial do
          command(Ping => pinged | paid) { |_s, _cmd| nil }
        end
        state :pinged
        final :paid
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::TransitionError, /must move to one of .*::Pinged \| .*::Paid/)
    end

    it 'raises when a handler moves to a state it did not declare' do
      m = machine do
        initial do
          command(Ping) { |_s, _cmd| to :pinged }
        end
        state :pinged
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::TransitionError, /moved to .*::Pinged, not one of nil/)
    end

    it 'raises when a handler moves the machine twice' do
      m = machine do
        initial do
          command(Ping => %i[pinged paid]) do |_s, _cmd|
            to :pinged
            to :paid
          end
        end
        state :pinged
        final :paid
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::TransitionError, /moved the machine twice/)
    end

    it 'raises when a handler appends a state as an event' do
      m = machine do
        pinged = State(:pinged)
        initial do
          command(Ping => pinged) { |_s, _cmd| event pinged }
        end
        state :pinged
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::Error, /is a state; move to it with to/)
    end

    it 'raises when a handler appends a union of states as an event' do
      m = machine do
        pinged = State(:pinged)
        ponged = State(:ponged)
        initial do
          command(Ping => pinged | ponged) { |_s, _cmd| event pinged | ponged }
        end
        state :pinged
        state :ponged
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::Error, /::Pinged \| .*::Ponged are states; move to one of them with to/)
    end

    it 'raises when a data event is invalid' do
      m = machine do
        initial do
          command(Notify => :noted) { |_s, cmd| event NoteTaken, note: cmd.payload.note }
        end
        state :noted
      end
      cmd = Notify.new(payload: { cart_id: 'c1', note: nil })

      expect { m.new(cart_id: 'c1').decide(cmd) }
        .to raise_error(Sourced::Machine::InvalidEventError, /invalid .*NoteTaken/)
    end
  end

  describe 'partition keys' do
    it 'are filled in on appended events and dispatched messages' do
      with_reactor(CartMachine, cart_id: 'c1')
        .when(OpenCart, cart_id: 'c1', currency: 'EUR')
        .then(cart_opened, opened)

      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(ordered)
        .then(ReleaseStock.new(payload: { cart_id: 'c1', skus: [] }), SendReceipt.new(payload: { cart_id: 'c1', total: 0 }))
    end
  end

  describe '#handles_command?' do
    it 'is true only for commands the current state accepts' do
      fresh = CartMachine.new(cart_id: 'c1')
      expect(fresh.handles_command?(OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }))).to be(true)
      expect(fresh.handles_command?(Checkout.new(payload: { cart_id: 'c1' }))).to be(false)

      fresh.evolve([opened])
      expect(fresh.handles_command?(Checkout.new(payload: { cart_id: 'c1' }))).to be(true)
      expect(fresh.handles_command?(OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }))).to be(false)
    end

    it 'acks commands the current state does not accept, with no events' do
      cmd = Pay.new(payload: { cart_id: 'c1' })

      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(cmd)
        .then { |result| expect(result.pairs).to eq([[Sourced::Actions::OK, cmd]]) }
    end
  end

  describe 'runtime state' do
    it 'exposes the current state through a read-only view' do
      instance = CartMachine.new(cart_id: 'c1')
      expect(instance.machine.value).to eq(:initial)
      expect(instance.machine.entered_by).to be_nil

      instance.evolve([cart_opened, opened, item_added])
      expect(instance.machine.value).to eq(CartMachine::Open)
      expect(instance.machine.in?(CartMachine::Open)).to be(true)
      expect(instance.machine.in?('open')).to be(true)
      expect(instance.machine.in?(:root, CartMachine::Open)).to be(true)
      expect(instance.machine.entered_by).to eq(opened)
      expect(instance.machine.final?).to be(false)
      expect(instance.context.total).to eq(100)

      instance.evolve([ordered, moved(CartMachine, 'machine_test.pay', from: 'ordered', to: 'paid', exited: %w[ordered], cart_id: 'c1')])
      expect(instance.machine.in?(CartMachine::Paid)).to be(true)
      expect(instance.machine.in?(CartMachine::Open)).to be(false)
      expect(instance.machine.final?).to be(true)
      expect(instance.machine).not_to respond_to(:value=)
    end

    it 'keeps it apart from the domain context' do
      instance = CartMachine.new(cart_id: 'c1')
      instance.evolve([cart_opened, opened])

      expect(instance.context).to be_a(CartState)
      expect(instance.context.currency).to eq('EUR')
    end

    it "raises on a transition to a path that isn't in the chart" do
      instance = CartMachine.new(cart_id: 'c1')
      stray = CartMachine::Open.new(payload: {
        cart_id: 'c1', trigger: 'machine_test.open_cart', source: 'initial', target: 'nowhere', exited: [], entered: %w[nowhere]
      })

      expect { instance.evolve([stray]) }.to raise_error(Sourced::Machine::Error, /no state at "nowhere"/)
    end
  end

  describe 'entry and exit actions' do
    it 'runs exit then entry actions when the transition is claimed' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(item_added)
        .when(ordered)
        .then(
          ReleaseStock.new(payload: { cart_id: 'c1', skus: ['A1'] }), # Open's exit
          SendReceipt.new(payload: { cart_id: 'c1', total: 100 })     # Ordered's entry
        )
    end

    it 'runs the exit actions of the state that was left' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(item_added)
        .when(moved(CartMachine, 'machine_test.checkout', from: 'open', to: 'rejected', exited: %w[open], cart_id: 'c1'))
        .then(ReleaseStock, cart_id: 'c1', skus: ['A1'])
    end

    it 'gives actions the context and the transition' do
      seen = []
      m = machine do
        initial { command Ping => :pinged }
        state(:pinged) { entry { |s, transition| seen << [s, transition] } }
      end
      transition = moved(m, 'machine_test.ping', from: 'initial', to: 'pinged', cart_id: 'c1')

      with_reactor(m, cart_id: 'c1').when(transition).then([])
      expect(seen.map(&:last)).to eq([transition])
    end

    it 'runs exit then entry actions when a state is re-entered' do
      m = machine do
        initial { command Ping => :pinged }
        state :pinged do
          exit { |_s, _transition| dispatch Notify, note: 'exit' }
          entry { |_s, _transition| dispatch Notify, note: 'entry' }
          command Ping => :pinged
        end
      end

      with_reactor(m, cart_id: 'c1')
        .when(Ping, cart_id: 'c1', note: 'x')
        .then(moved(m, 'machine_test.ping', from: 'initial', to: 'pinged', cart_id: 'c1'))

      re_entry = moved(m, 'machine_test.ping', from: 'pinged', to: 'pinged', exited: %w[pinged], cart_id: 'c1')
      with_reactor(m, cart_id: 'c1')
        .given(moved(m, 'machine_test.ping', from: 'initial', to: 'pinged', cart_id: 'c1'))
        .when(Ping, cart_id: 'c1', note: 'x')
        .then(re_entry)

      with_reactor(m, cart_id: 'c1')
        .given(moved(m, 'machine_test.ping', from: 'initial', to: 'pinged', cart_id: 'c1'))
        .when(re_entry)
        .then(Notify.new(payload: { cart_id: 'c1', note: 'exit' }), Notify.new(payload: { cart_id: 'c1', note: 'entry' }))
    end

    it 'skips them while replaying' do
      history = Sourced::ReadResult.new(messages: [opened], guard: Sourced::ConsistencyGuard.new(query: Plumb::BLANK_ARRAY, last_position: 0))

      pairs = CartMachine.handle_batch({ cart_id: 'c1' }, [ordered], history:, replaying: true)
      expect(pairs).to eq([[Sourced::Actions::OK, ordered]])
    end
  end

  describe 'after timers' do
    let(:opened) { moved(TimerMachine, 'machine_test.open_cart', from: 'initial', to: 'open', cart_id: 'c1') }
    let(:ordered) { moved(TimerMachine, 'machine_test.checkout', from: 'open', to: 'ordered', exited: %w[open], cart_id: 'c1') }

    # A command an `after` block dispatched in reaction to +transition+, correlated by the runner.
    def timer_command(transition)
      transition.correlate(Expire.new(payload: { cart_id: 'c1' }).with_metadata(entered_by: transition.id))
    end

    it 'dispatches future-dated commands that carry the entering transition id' do
      with_reactor(TimerMachine, cart_id: 'c1')
        .when(opened)
        .then do |result|
          expire, = result.messages
          expect(expire).to be_a(Expire)
          expect(expire.created_at).to be_within(0.001).of(opened.created_at + 3600)
          expect(expire.metadata[:entered_by]).to eq(opened.id)
          expect(expire.causation_id).to eq(opened.id)
        end
    end

    it 'takes the delay as a duration string' do
      timed = machine do
        initial { command OpenCart => :open }
        state(:open) { after('1h30m') { |_cart, _transition| dispatch Expire } }
      end
      transition = moved(timed, 'machine_test.open_cart', from: 'initial', to: 'open', cart_id: 'c1')
      expire, = timed.new(cart_id: 'c1').tap { |m| m.evolve([transition]) }.__react(transition)

      expect(expire.created_at).to be_within(0.001).of(transition.created_at + 5400)
      expect(expire.metadata[:entered_by]).to eq(transition.id)
    end

    it 'accepts a timer command while its state entry is current' do
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened])

      expect(instance.handles_command?(timer_command(opened))).to be(true)
    end

    it 'refuses a timer command once the machine has moved on, even where the command is accepted' do
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened, ordered])

      expect(instance.handles_command?(timer_command(opened))).to be(false)
      expect(instance.handles_command?(Expire.new(payload: { cart_id: 'c1' }))).to be(true)
    end

    it 'refuses a timer command from an earlier entry of the same state' do
      reentered = moved(TimerMachine, 'machine_test.reopen', from: 'open', to: 'open', exited: %w[open], cart_id: 'c1')
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened, reentered])

      expect(instance.handles_command?(timer_command(opened))).to be(false)
      expect(instance.handles_command?(timer_command(reentered))).to be(true)
    end

    it "doesn't treat a command that inherited the metadata as a timer" do
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened, ordered])
      # Metadata is copied along correlation chains: caused by ordered, naming opened.
      inherited = ordered.correlate(Expire.new(payload: { cart_id: 'c1' }).with_metadata(entered_by: opened.id))

      expect(instance.handles_command?(inherited)).to be(true)
    end

    it "doesn't start timers for a state the machine has already left" do
      claimed = Sourced::PositionedMessage.new(opened, 1)
      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'cart_id:c1', partition_value: { 'cart_id' => 'c1' },
        messages: [claimed], replaying: false, guard: Sourced::ConsistencyGuard.new(query: Plumb::BLANK_ARRAY, last_position: 2)
      )
      history = Sourced::ReadResult.new(
        messages: [claimed, Sourced::PositionedMessage.new(ordered, 2)],
        guard: Sourced::ConsistencyGuard.new(query: Plumb::BLANK_ARRAY, last_position: 2)
      )

      expect(TimerMachine.handle_claim(claim, history:)).to eq([[[], claimed]])
    end

    it 'rejects timers on the initial position, and without a delay or block' do
      expect { machine { initial { after(60) { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /no timers/)
      expect { machine { state(:open) { after('soon') { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /seconds or a duration string/)
      expect { machine { state(:open) { after(:soon) { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /seconds or a duration string/)
      expect { machine { state(:open) { after(60) } } }.to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
    end

    describe 'on a store' do
      let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
      let(:router) { Sourced::Router.new(store:, reactors: [TimerMachine]).setup! }

      def deliver_timers(after:)
        Timecop.travel(Time.now + after) do
          promoted = store.update_schedule!
          router.drain
          promoted
        end
      end

      def value = TimerMachine.load(store:, cart_id: 'c1').value
      def targets = store.read_all(limit: 50).messages.filter_map { |m| m.payload.target if TimerMachine.transition_classes.include?(m.class) }

      before do
        store.append(OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }))
        router.drain
      end

      it 'fires the timer when it falls due' do
        expect(deliver_timers(after: 3599)).to eq(0)
        expect(value).to eq(TimerMachine::Open)

        expect(deliver_timers(after: 3601)).to eq(1)
        expect(value).to eq(TimerMachine::Expired)
        expect(targets).to eq(%w[open expired])
      end

      it 'ignores the timer once the machine has moved on' do
        store.append(Checkout.new(payload: { cart_id: 'c1' }))
        router.drain

        expect(deliver_timers(after: 3601)).to eq(1)
        expect(value).to eq(TimerMachine::Ordered)
        expect(store.read_all(limit: 50).messages.map(&:type)).to include('machine_test.expire')
        expect(targets).to eq(%w[open ordered])
      end

      it "doesn't schedule a timer when the state was left before it was claimed" do
        fresh = Sourced::Store.new(Sequel.sqlite).tap(&:install!)
        fresh_router = Sourced::Router.new(store: fresh, reactors: [TimerMachine]).setup!
        fresh.append([
          OpenCart.new(payload: { cart_id: 'c2', currency: 'EUR' }),
          Checkout.new(payload: { cart_id: 'c2' })
        ])
        fresh_router.drain

        Timecop.travel(Time.now + 3601) do
          expect(fresh.update_schedule!).to eq(0)
        end
      end
    end
  end

  describe 'definition checks' do
    it 'fixes what the runtime reads per message once validated' do
      m = machine do
        initial { command Ping => :pinged }
        state :pinged
      end
      expect(m.handled_commands).not_to be_frozen

      m.validate!
      expect(m.handled_commands).to be_frozen.and eq([Ping])
      expect(m.handled_messages_for_evolve).to be_frozen.and include(m::Pinged)
      expect(m.transition_classes).to be_frozen
    end

    it 'requires partition_by' do
      m = Class.new(Sourced::Machine) do
        consumer_group 'machine_test.unpartitioned'
        initial { command MachineTest::OpenCart => :open }
        state :open
      end
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /needs partition_by/)
    end

    it 'requires a name or a consumer_group' do
      expect do
        Class.new(Sourced::Machine) do
          partition_by :cart_id
          State(:open)
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /a machine needs a name or a consumer_group/)
    end

    it 'accepts commands without the partition keys: they may be tags' do
      m = machine do
        initial { command Unkeyed => :open }
        state :open
      end
      expect { m.validate! }.not_to raise_error
    end

    it 'requires declared states to be placed' do
      m = machine { initial { command OpenCart => :open } }
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /::Open declared but never placed/)
    end

    it "rejects another machine's states" do
      m = machine { initial { command OpenCart => CartMachine::Open } }
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /targets MachineTest::CartMachine::Open, which isn't a state/)
    end

    it 'rejects commands in final states' do
      m = machine { final(:paid) { command Pay => :paid } }
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /final state paid can't accept commands/)
    end

    it 'requires a block for unions and for data-only commands' do
      expect { machine { a = State(:a); b = State(:b); initial { command Checkout => a | b } } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
      expect { machine { a = State(:a); initial { command Checkout => a | nil } } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
      expect { machine { initial { command Checkout } } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
    end

    it 'rejects placing a state or accepting a command twice' do
      expect { machine { state :open; state :open } }
        .to raise_error(Sourced::Machine::DefinitionError, /placed twice/)
      expect { machine { state(:open) { command(Pay => :paid); command(Pay => :paid) } } }
        .to raise_error(Sourced::Machine::DefinitionError, /accepts MachineTest::Pay twice/)
    end

    it 'rejects a state named initial' do
      expect { machine { State(:initial) } }
        .to raise_error(Sourced::Machine::DefinitionError, /can't be named :initial/)
    end

    it 'rejects entry actions on the initial position' do
      expect { machine { initial { entry { nil } } } }
        .to raise_error(Sourced::Machine::DefinitionError, /no entry actions/)
    end

    it 'runs when the machine is registered with a router' do
      m = machine { initial { command OpenCart => :open } }
      router = Sourced::Router.new(store: Sourced::Store.new(Sequel.sqlite))

      expect { router.register(m) }.to raise_error(Sourced::Machine::DefinitionError)
    end
  end

  describe 'running on a store' do
    let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
    let(:router) { Sourced::Router.new(store:, reactors: [CartMachine]).setup! }

    def types = store.read_all(limit: 50).messages.map { |m| m.type.delete_prefix('machine_test.') }

    it 'decides commands and runs entry and exit actions in workers' do
      store.append(OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }))
      store.append(AddItem.new(payload: { cart_id: 'c1', sku: 'A1', qty: 1, unit_price: 30 }))
      store.append(Checkout.new(payload: { cart_id: 'c1' }))
      router.drain

      expect(types).to eq(%w[
        open_cart add_item checkout
        cart_opened cart_machine.runtime.open item_added checked_out cart_machine.runtime.ordered
        release_stock send_receipt
      ])
      snapshot = CartMachine.load(store:, cart_id: 'c1')
      expect(snapshot.value).to eq(CartMachine::Ordered)
      expect(snapshot.context.total).to eq(30)
    end

    it 'acks commands the current state does not accept' do
      store.append(Pay.new(payload: { cart_id: 'c1' }))
      router.drain

      expect(types).to eq(%w[pay])
      expect(CartMachine.load(store:, cart_id: 'c1').value).to eq(:initial)
    end

    it 'gives entry actions the context after the whole entering decision' do
      ping_router = Sourced::Router.new(store:, reactors: [PingMachine]).setup!

      store.append(Ping.new(payload: { cart_id: 'c1', note: 'hello' }))
      ping_router.drain

      notify = store.read_all(limit: 50).messages.find { Notify === it }
      expect(notify.payload.note).to eq('hello')
    end

    describe 'Sourced.handle!' do
      it 'decides a command the current state accepts' do
        _cmd, reactor, events = Sourced.handle!(CartMachine, OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }), store:)

        expect(events.map(&:class)).to eq([CartOpened, CartMachine::Open])
        expect(events.last.payload.target).to eq('open')
        expect(reactor.machine.value).to eq(CartMachine::Open)
      end

      it "raises CommandNotHandledError for a command the current state doesn't accept" do
        expect { Sourced.handle!(CartMachine, Pay.new(payload: { cart_id: 'c1' }), store:) }
          .to raise_error(Sourced::CommandNotHandledError, /CartMachine doesn't handle MachineTest::Pay/)
        expect(types).to be_empty
      end
    end
  end
end
end
