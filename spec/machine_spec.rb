# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

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
  CartPaid = Sourced::Event.define('machine_test.cart_paid') do
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

  # Data events for a machine that emits data after its checkpoint
  Ping = Sourced::Command.define('machine_test.ping') do
    attribute :cart_id, String
    attribute :note, String
  end
  Pinged = Sourced::Event.define('machine_test.pinged') do
    attribute :cart_id, String
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
  CartExpired = Sourced::Event.define('machine_test.cart_expired') do
    attribute :cart_id, String
  end

  # Not a valid checkpoint for a cart machine: it lacks cart_id
  Unkeyed = Sourced::Event.define('machine_test.unkeyed') do
    attribute :order_id, String
  end
  # Requires an attribute OpenCart doesn't have
  CartLabelled = Sourced::Event.define('machine_test.cart_labelled') do
    attribute :cart_id, String
    attribute :label, String
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

    state { CartState.new }
    evolve(CartOpened) { |cart, evt| cart.currency = evt.payload.currency }
    evolve(ItemAdded) { |cart, evt| cart.add(evt.payload.sku, evt.payload.qty, evt.payload.unit_price) }

    initial do
      command OpenCart => CartOpened
    end

    checkpoint CartOpened do
      exit { |cart, _evt| dispatch ReleaseStock, skus: cart.items.keys }

      command AddItem do |_cart, cmd|
        event ItemAdded, sku: cmd.payload.sku, qty: cmd.payload.qty, unit_price: cmd.payload.unit_price
      end

      command Checkout => CheckedOut | CheckoutRejected do |cart, _cmd|
        cart.empty? ? event(CheckoutRejected, reason: 'empty') : event(CheckedOut, total: cart.total)
      end
    end

    checkpoint CheckoutRejected do
      command Reopen => CartOpened do |cart, _cmd|
        event CartOpened, currency: cart.currency
      end
    end

    checkpoint CheckedOut do
      entry { |_cart, evt| dispatch SendReceipt, total: evt.payload.total }
      command Pay => CartPaid
    end

    final CartPaid
  end

  class TimerMachine < Sourced::Machine
    partition_by :cart_id

    initial do
      command OpenCart => CartOpened
    end

    checkpoint CartOpened do
      after(3600) { |_cart, _evt| dispatch Expire }
      command Expire => CartExpired
      command(Checkout => CheckedOut) { |_cart, _cmd| event CheckedOut, total: 0 }
      command(Reopen => CartOpened) { |_cart, _cmd| event CartOpened, currency: 'EUR' }  # re-entry
    end

    checkpoint CheckedOut do
      command Expire => CartExpired  # accepted here too: a stale timer must still be refused
    end

    final CartExpired
  end

RSpec.describe Sourced::Machine do
  include Sourced::Testing::RSpec

  # A throwaway machine over the cart messages, for checking one feature at a time.
  def machine(&block)
    Class.new(Sourced::Machine) do
      partition_by :cart_id
      class_eval(&block)
    end
  end

  let(:opened) { CartOpened.new(payload: { cart_id: 'c1', currency: 'EUR' }) }
  let(:item_added) { ItemAdded.new(payload: { cart_id: 'c1', sku: 'A1', qty: 2, unit_price: 50 }) }

  describe 'routing' do
    it 'claims every accepted command and every checkpoint' do
      expect(CartMachine.handled_commands).to contain_exactly(OpenCart, AddItem, Checkout, Reopen, Pay)
      expect(CartMachine.handled_messages).to include(CartOpened, CheckedOut, CheckoutRejected, CartPaid)
      expect(CartMachine.handled_messages).not_to include(ItemAdded)
    end

    it 'evolves checkpoints and evolve types' do
      expect(CartMachine.handled_messages_for_evolve).to contain_exactly(
        CartOpened, ItemAdded, CheckedOut, CheckoutRejected, CartPaid
      )
    end

    it "keeps its checkpoints out of its projection, so adopting one keeps them" do
      items = Class.new(Sourced::Projection) do
        state { CartState.new }
        evolve(ItemAdded) { |cart, evt| cart.add(evt.payload.sku, evt.payload.qty, evt.payload.unit_price) }
      end
      machine = Class.new(Sourced::Machine) do
        partition_by :cart_id
        initial { command OpenCart => CartOpened }
        final CartOpened
        evolve items
      end

      expect(machine.projection_class.handled_messages).to eq([ItemAdded])
      expect(machine.handled_messages_for_evolve).to contain_exactly(ItemAdded, CartOpened)
    end
  end

  describe 'command forms' do
    it 'appends a single declared checkpoint, built from the command payload' do
      with_reactor(CartMachine, cart_id: 'c1')
        .when(OpenCart, cart_id: 'c1', currency: 'EUR', channel: 'app')
        .then(CartOpened, cart_id: 'c1', currency: 'EUR')
    end

    it 'emits data events without moving the machine (no arrow)' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(AddItem, cart_id: 'c1', sku: 'A1', qty: 2, unit_price: 50)
        .then(ItemAdded, cart_id: 'c1', sku: 'A1', qty: 2, unit_price: 50)
    end

    it 'takes the checkpoint the handler emits from a union' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(item_added)
        .when(Checkout, cart_id: 'c1')
        .then(CheckedOut, cart_id: 'c1', total: 100)

      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(Checkout, cart_id: 'c1')
        .then(CheckoutRejected, cart_id: 'c1', reason: 'empty')
    end

    it "doesn't append a single checkpoint the handler emitted itself" do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(CheckoutRejected, cart_id: 'c1', reason: 'empty')
        .when(Reopen, cart_id: 'c1')
        .then(CartOpened, cart_id: 'c1', currency: 'EUR')
    end

    it 'appends a single checkpoint after the data events the handler emits' do
      m = machine do
        initial do
          command Ping => Pinged do |_s, cmd|
            event NoteTaken, note: cmd.payload.note
          end
        end
        checkpoint Pinged
      end

      with_reactor(m, cart_id: 'c1')
        .when(Ping, cart_id: 'c1', note: 'hi')
        .then(NoteTaken.new(payload: { cart_id: 'c1', note: 'hi' }), Pinged.new(payload: { cart_id: 'c1' }))
    end

    it 'may emit no checkpoint when the union includes nil' do
      m = machine do
        initial do
          command Ping => Pinged | nil do |_s, cmd|
            event NoteTaken, note: cmd.payload.note
            event Pinged if cmd.payload.note == 'go'
          end
        end
        checkpoint Pinged
      end

      with_reactor(m, cart_id: 'c1').when(Ping, cart_id: 'c1', note: 'wait')
        .then(NoteTaken, cart_id: 'c1', note: 'wait')
      with_reactor(m, cart_id: 'c1').when(Ping, cart_id: 'c1', note: 'go')
        .then(NoteTaken.new(payload: { cart_id: 'c1', note: 'go' }), Pinged.new(payload: { cart_id: 'c1' }))
    end

    it 'raises when a union handler emits no checkpoint' do
      m = machine do
        initial do
          command(Ping => Pinged | CartPaid) { |_s, _cmd| nil }
        end
        checkpoint Pinged
        final CartPaid
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::CheckpointError, /must emit one of .*Pinged \| .*CartPaid/)
    end

    it 'raises when a handler emits a checkpoint it did not declare' do
      m = machine do
        initial do
          command(Ping) { |_s, _cmd| event Pinged }
        end
        checkpoint Pinged
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::CheckpointError, /emitted .*Pinged, not one of nil/)
    end

    it 'raises when a handler emits more than one checkpoint' do
      m = machine do
        initial do
          command(Ping => Pinged | CartPaid) do |_s, _cmd|
            event Pinged
            event CartPaid
          end
        end
        checkpoint Pinged
        final CartPaid
      end

      expect { m.new(cart_id: 'c1').decide(Ping.new(payload: { cart_id: 'c1', note: 'x' })) }
        .to raise_error(Sourced::Machine::CheckpointError, /a decision emits one checkpoint/)
    end

    it 'raises when an auto-appended checkpoint is invalid' do
      # Notify declares note, so the definition passes; a nil note doesn't fit NoteTaken's String.
      m = machine do
        initial do
          command Notify => NoteTaken
        end
        checkpoint NoteTaken
      end
      cmd = Notify.new(payload: { cart_id: 'c1', note: nil })

      expect { m.new(cart_id: 'c1').decide(cmd) }
        .to raise_error(Sourced::Machine::InvalidEventError, /invalid .*NoteTaken/)
    end
  end

  describe 'partition keys' do
    it 'are filled in on emitted events and dispatched messages' do
      with_reactor(CartMachine, cart_id: 'c1')
        .when(CheckedOut, cart_id: 'c1', total: 30)
        .then(SendReceipt, cart_id: 'c1', total: 30)
    end
  end

  describe '#handles_command?' do
    it 'is true only for commands the current checkpoint accepts' do
      fresh = CartMachine.new(cart_id: 'c1')
      expect(fresh.handles_command?(OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }))).to be(true)
      expect(fresh.handles_command?(Checkout.new(payload: { cart_id: 'c1' }))).to be(false)

      fresh.evolve([opened])
      expect(fresh.handles_command?(Checkout.new(payload: { cart_id: 'c1' }))).to be(true)
      expect(fresh.handles_command?(OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }))).to be(false)
    end

    it 'acks commands the current checkpoint does not accept, with no events' do
      cmd = Pay.new(payload: { cart_id: 'c1' })

      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(cmd)
        .then { |result| expect(result.pairs).to eq([[Sourced::Actions::OK, cmd]]) }
    end
  end

  describe 'runtime state' do
    it 'exposes the current checkpoint through a read-only view' do
      instance = CartMachine.new(cart_id: 'c1')
      expect(instance.machine.value).to eq(:initial)
      expect(instance.machine.in?(:initial)).to be(true)

      instance.evolve([opened, item_added])
      expect(instance.machine.value).to eq(CartOpened)
      expect(instance.machine.entered_by).to eq(opened)
      expect(instance.machine.final?).to be(false)
      expect(instance.state.total).to eq(100)

      instance.evolve([CheckedOut.new(payload: { cart_id: 'c1', total: 100 }), CartPaid.new(payload: { cart_id: 'c1' })])
      expect(instance.machine.in?(CartPaid)).to be(true)
      expect(instance.machine.final?).to be(true)
      expect(instance.machine).not_to respond_to(:value=)
    end

    it 'keeps it apart from the domain state' do
      instance = CartMachine.new(cart_id: 'c1')
      instance.evolve([opened])

      expect(instance.state).to be_a(CartState)
      expect(instance.state.currency).to eq('EUR')
    end
  end

  describe 'entry and exit actions' do
    it 'runs entry actions when the checkpoint is claimed' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .when(CheckedOut, cart_id: 'c1', total: 100)
        .then(
          ReleaseStock.new(payload: { cart_id: 'c1', skus: [] }),   # CartOpened's exit
          SendReceipt.new(payload: { cart_id: 'c1', total: 100 })   # CheckedOut's entry
        )
    end

    it 'runs the exit actions of the checkpoint that was replaced' do
      with_reactor(CartMachine, cart_id: 'c1')
        .given(opened)
        .and(item_added)
        .when(CheckoutRejected, cart_id: 'c1', reason: 'empty')
        .then(ReleaseStock, cart_id: 'c1', skus: ['A1'])
    end

    it 'runs exit then entry actions when a checkpoint is re-entered' do
      m = machine do
        initial { command Ping => Pinged }
        checkpoint Pinged do
          exit { |_s, _evt| dispatch Notify, note: 'exit' }
          entry { |_s, _evt| dispatch Notify, note: 'entry' }
          command Ping => Pinged
        end
      end

      with_reactor(m, cart_id: 'c1')
        .given(Pinged, cart_id: 'c1')
        .when(Pinged, cart_id: 'c1')
        .then(Notify.new(payload: { cart_id: 'c1', note: 'exit' }), Notify.new(payload: { cart_id: 'c1', note: 'entry' }))
    end

    it 'skips them while replaying' do
      checked_out = CheckedOut.new(payload: { cart_id: 'c1', total: 100 })
      history = Sourced::ReadResult.new(messages: [opened], guard: Sourced::ConsistencyGuard.new(conditions: [], last_position: 0))

      pairs = CartMachine.handle_batch({ cart_id: 'c1' }, [checked_out], history:, replaying: true)
      expect(pairs).to eq([[Sourced::Actions::OK, checked_out]])
    end
  end

  describe 'after timers' do
    let(:checked_out) { CheckedOut.new(payload: { cart_id: 'c1', total: 0 }) }

    # A command as an `after` block of +checkpoint+ dispatches it, correlated by the runner.
    def timer_command(checkpoint)
      checkpoint.correlate(Expire.new(payload: { cart_id: 'c1' }).with_metadata(entered_by: checkpoint.id))
    end

    it 'dispatches future-dated commands that carry the entering event id' do
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

    it 'accepts a timer command while its checkpoint entry is current' do
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened])

      expect(instance.handles_command?(timer_command(opened))).to be(true)
    end

    it 'refuses a timer command once the machine has moved on, even where the command is accepted' do
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened, checked_out])

      expect(instance.handles_command?(timer_command(opened))).to be(false)
      expect(instance.handles_command?(Expire.new(payload: { cart_id: 'c1' }))).to be(true)
    end

    it 'refuses a timer command from an earlier entry of the same checkpoint' do
      reentered = CartOpened.new(payload: { cart_id: 'c1', currency: 'EUR' })
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened, reentered])

      expect(instance.handles_command?(timer_command(opened))).to be(false)
      expect(instance.handles_command?(timer_command(reentered))).to be(true)
    end

    it "doesn't treat a command that inherited the metadata as a timer" do
      instance = TimerMachine.new(cart_id: 'c1')
      instance.evolve([opened, checked_out])
      # Metadata is copied along correlation chains: caused by checked_out, naming opened.
      inherited = checked_out.correlate(Expire.new(payload: { cart_id: 'c1' }).with_metadata(entered_by: opened.id))

      expect(instance.handles_command?(inherited)).to be(true)
    end

    it "doesn't start timers for a checkpoint the machine has already left" do
      claimed = Sourced::PositionedMessage.new(opened, 1)
      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'cart_id:c1', partition_value: { 'cart_id' => 'c1' },
        messages: [claimed], replaying: false, guard: Sourced::ConsistencyGuard.new(conditions: [], last_position: 2)
      )
      history = Sourced::ReadResult.new(
        messages: [claimed, Sourced::PositionedMessage.new(checked_out, 2)],
        guard: Sourced::ConsistencyGuard.new(conditions: [], last_position: 2)
      )

      expect(TimerMachine.handle_claim(claim, history:)).to eq([[[], claimed]])
    end

    it 'rejects timers on the initial position, and without a delay or block' do
      expect { machine { initial { after(60) { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /no timers/)
      expect { machine { checkpoint(CartOpened) { after('1h') { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /delay in seconds/)
      expect { machine { checkpoint(CartOpened) { after(60) } } }.to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
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

      before do
        store.append(OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }))
        router.drain
      end

      it 'fires the timer when it falls due' do
        expect(deliver_timers(after: 3599)).to eq(0)
        expect(value).to eq(CartOpened)

        expect(deliver_timers(after: 3601)).to eq(1)
        expect(value).to eq(CartExpired)
      end

      it 'ignores the timer once the machine has moved on' do
        store.append(Checkout.new(payload: { cart_id: 'c1' }))
        router.drain

        expect(deliver_timers(after: 3601)).to eq(1)
        expect(value).to eq(CheckedOut)
        types = store.read_all(limit: 50).messages.map(&:type)
        expect(types).to include('machine_test.expire')
        expect(types).not_to include('machine_test.cart_expired')
      end

      it "doesn't schedule a timer when the checkpoint was left before it was claimed" do
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
    it 'requires partition_by' do
      m = Class.new(Sourced::Machine) { initial { command MachineTest::OpenCart => MachineTest::CartOpened }; checkpoint MachineTest::CartOpened }
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /needs partition_by/)
    end

    it 'requires checkpoints and commands to declare the partition keys' do
      expect { machine { checkpoint Unkeyed }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /checkpoint MachineTest::Unkeyed must declare cart_id/)
    end

    it 'requires targets to be checkpoints' do
      m = machine { initial { command OpenCart => CartOpened } }
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /targets MachineTest::CartOpened, which isn't a checkpoint/)
    end

    it 'rejects commands in final checkpoints' do
      m = machine { final(CartPaid) { command Pay => CartPaid } }
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /final checkpoint MachineTest::CartPaid can't accept commands/)
    end

    it 'requires a blockless checkpoint to be buildable from its command' do
      m = machine do
        initial { command OpenCart => CartLabelled }
        checkpoint CartLabelled
      end
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /CartLabelled requires label, which MachineTest::OpenCart doesn't declare/)
    end

    it 'accepts a blockless checkpoint whose other attributes may be missing' do
      m = machine do
        initial { command Reopen => CartPaid }
        final CartPaid
      end
      expect(m.validate!).to be(true)
    end

    it 'requires a block for unions and for data-only commands' do
      expect { machine { initial { command Checkout => CheckedOut | CheckoutRejected } } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
      expect { machine { initial { command Checkout } } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
    end

    it 'rejects declaring a checkpoint or a command twice' do
      expect { machine { checkpoint CartOpened; checkpoint CartOpened } }
        .to raise_error(Sourced::Machine::DefinitionError, /declared twice/)
      expect { machine { checkpoint(CartOpened) { command(Pay => CartPaid); command(Pay => CartPaid) } } }
        .to raise_error(Sourced::Machine::DefinitionError, /accepts MachineTest::Pay twice/)
    end

    it 'rejects entry actions on the initial position' do
      expect { machine { initial { entry { nil } } } }
        .to raise_error(Sourced::Machine::DefinitionError, /no entry actions/)
    end

    it 'runs when the machine is registered with a router' do
      m = machine { initial { command OpenCart => CartOpened } }
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
        cart_opened item_added checked_out
        release_stock send_receipt
      ])
      snapshot = CartMachine.load(store:, cart_id: 'c1')
      expect(snapshot.value).to eq(CheckedOut)
      expect(snapshot.state.total).to eq(30)
    end

    it 'acks commands the current checkpoint does not accept' do
      store.append(Pay.new(payload: { cart_id: 'c1' }))
      router.drain

      expect(types).to eq(%w[pay])
      expect(CartMachine.load(store:, cart_id: 'c1').value).to eq(:initial)
    end

    it 'gives entry actions the state after the whole entering decision' do
      m = Class.new(Sourced::Machine) do
        def self.name = 'MachineTest::PingMachine'
        partition_by :cart_id
        state { { note: nil } }
        evolve(NoteTaken) { |s, evt| s[:note] = evt.payload.note }

        initial do
          command Ping => Pinged do |_s, cmd|
            event Pinged
            event NoteTaken, note: cmd.payload.note  # after the checkpoint
          end
        end
        checkpoint Pinged do
          entry { |s, _evt| dispatch Notify, note: s[:note] }
        end
      end
      ping_router = Sourced::Router.new(store:, reactors: [m]).setup!

      store.append(Ping.new(payload: { cart_id: 'c1', note: 'hello' }))
      ping_router.drain

      notify = store.read_all(limit: 50).messages.find { |msg| msg.is_a?(Notify) }
      expect(notify.payload.note).to eq('hello')
    end

    describe 'Sourced.handle!' do
      it 'decides a command the current checkpoint accepts' do
        _cmd, reactor, events = Sourced.handle!(CartMachine, OpenCart.new(payload: { cart_id: 'c1', currency: 'EUR' }), store:)

        expect(events.map(&:class)).to eq([CartOpened])
        expect(reactor.machine.value).to eq(CartOpened)
      end

      it "raises CommandNotHandledError for a command the current checkpoint doesn't accept" do
        expect { Sourced.handle!(CartMachine, Pay.new(payload: { cart_id: 'c1' }), store:) }
          .to raise_error(Sourced::CommandNotHandledError, /CartMachine doesn't handle MachineTest::Pay/)
        expect(types).to be_empty
      end
    end
  end
end
end
