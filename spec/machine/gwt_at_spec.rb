# frozen_string_literal: true

require 'spec_helper'
require 'sourced/testing/rspec'

module GWTAtTest
  Notify = Sourced::Command.define('gwt_at_test.notify') { attribute :order_id, String }

  class Order < Sourced::Machine
    partition_by :order_id

    Place = Command(:place)
    Cancel = Command(:cancel)
    Review = Command(:review)
    Noted = Event(:noted) { attribute :note, String }

    Placed = State(:placed)
    Completed = State(:completed)
    Cancelled = State(:cancelled)

    context { { notes: [] } }

    evolve(Noted) { |context, evt| context[:notes] << evt.payload.note }

    class PaymentMachine < Sourced::Machine::Part
      part_of Order

      Authorize = Command(:authorize)
      Capture = Command(:capture)

      Authorizing = State(:authorizing)
      Captured = State(:captured)

      initial { command Authorize => Authorizing }

      state Authorizing do
        entry { |_context, evt| dispatch Notify, order_id: evt.payload.order_id }
        command Capture => Captured
      end

      final Captured
    end

    class ShippingMachine < Sourced::Machine::Part
      part_of Order

      Ship = Command(:ship)
      Shipped = State(:shipped)

      initial { command Ship => Shipped }
      final Shipped
    end

    # A compound state's part, starting at a compound state of its own.
    class ReviewingMachine < Sourced::Machine::Part
      part_of Order

      Approve = Command(:approve)

      Approved = State(:approved)

      class DraftingMachine < Sourced::Machine::Part
        part_of ReviewingMachine

        Blank = State(:blank)

        started(Blank) {}
      end

      started DraftingMachine do
        command Approve => Approved
      end

      final Approved
    end

    initial do
      command Place => Placed
      command Review => ReviewingMachine
    end

    state Placed do
      command Cancel => Cancelled
      region :payment, PaymentMachine
      region :shipping, ShippingMachine
      done Completed
    end

    state ReviewingMachine

    final Completed
    final Cancelled
  end

  # One part in two regions: one handle at two paths.
  class Tour < Sourced::Machine
    partition_by :tour_id

    Book = Command(:book)
    Booking = State(:booking)

    class SupplierMachine < Sourced::Machine::Part
      part_of Tour

      RequestHold = Command(:request_hold)
      ConfirmHold = Command(:confirm_hold)

      Holding = State(:holding)
      Held = State(:held)

      initial { command RequestHold => Holding }
      state(Holding) { command ConfirmHold => Held }
      final Held
    end

    initial { command Book => Booking }

    state Booking do
      region :primary, SupplierMachine
      region :backup, SupplierMachine
    end
  end
end

RSpec.describe 'GWT at' do
  include Sourced::Testing::RSpec

  let(:order) { GWTAtTest::Order }
  let(:payment) { order::PaymentMachine }
  let(:shipping) { order::ShippingMachine }
  let(:review) { order::ReviewingMachine }
  let(:tour) { GWTAtTest::Tour }
  let(:supplier) { tour::SupplierMachine }


  def summary(messages) = messages.map { |msg| { class: msg.class, payload: msg.payload.to_h } }

  it "appends a machine's own state, from where the history leaves it" do
    gwt = with_reactor(order, order_id: 'o1').at(order::Placed)

    expect(summary(gwt.history)).to eq([
      { class: order::Placed,
        payload: { order_id: 'o1', trigger: 'at', source: 'initial', target: 'placed', exited: [], entered: ['placed'] } }
    ])

    gwt.when(order::Cancel, order_id: 'o1')
       .then(order::Cancelled, order_id: 'o1', trigger: order::Cancel.type, source: 'placed', target: 'cancelled',
                               exited: ['placed'], entered: ['cancelled'])
  end

  it "positions a part's state in a region, entering its ancestors" do
    gwt = with_reactor(order, order_id: 'o1').at(payment::Authorizing)

    expect(gwt.history.map { |msg| msg.payload.to_h.slice(:source, :target, :exited, :entered) }).to eq([
      { source: 'initial', target: 'placed.payment.authorizing', exited: [], entered: ['placed', 'placed.payment.authorizing'] }
    ])

    gwt.when(payment::Capture, order_id: 'o1')
       .then(payment::Captured, order_id: 'o1', trigger: payment::Capture.type, source: 'placed.payment.authorizing',
                                target: 'placed.payment.captured', exited: ['placed.payment.authorizing'],
                                entered: ['placed.payment.captured'])
  end

  it 'moves within the active region when the machine is already in its state' do
    gwt = with_reactor(order, order_id: 'o1').at(order::Placed).at(payment::Captured)

    expect(gwt.history.last.payload.to_h.slice(:source, :exited, :entered)).to eq(
      source: 'placed.payment.initial', exited: [], entered: ['placed.payment.captured']
    )
  end

  it "enters a compound state's default sub-states" do
    gwt = with_reactor(order, order_id: 'o1').at(order::ReviewingMachine)

    expect(gwt.history.last.payload.entered).to eq(['reviewing', 'reviewing.drafting', 'reviewing.drafting.blank'])

    gwt.when(review::Approve, order_id: 'o1')
       .then(review::Approved, order_id: 'o1', trigger: review::Approve.type, source: 'reviewing.drafting',
                               target: 'reviewing.approved', exited: ['reviewing.drafting.blank', 'reviewing.drafting'],
                               entered: ['reviewing.approved'])
  end

  it 'positions nested compound states, leaving what the history entered' do
    gwt = with_reactor(order, order_id: 'o1').at(order::Placed).at(review::Approved)

    expect(gwt.history.last.payload.to_h.slice(:source, :exited, :entered)).to eq(
      source: 'placed', exited: ['placed'], entered: ['reviewing', 'reviewing.approved']
    )
  end

  it 'positions parallel regions with two ats, interleaved with given' do
    with_reactor(order, order_id: 'o1')
      .at(payment::Authorizing)
      .given(order::Noted, order_id: 'o1', note: 'gift')
      .at(shipping::Shipped)
      .when(order::Cancel, order_id: 'o1')
      .then(order::Cancelled, order_id: 'o1', trigger: order::Cancel.type, source: 'placed', target: 'cancelled',
                              exited: ['placed.payment.authorizing', 'placed.shipping.shipped', 'placed'],
                              entered: ['cancelled'])
      .then { |result| expect(result.context[:notes]).to eq(['gift']) }
  end

  it 'positions finals without taking eventless steps' do
    gwt = with_reactor(order, order_id: 'o1').at(payment::Captured).at(shipping::Shipped)

    expect(gwt.history.map(&:class)).to eq([payment::Captured, shipping::Shipped])
  end

  it "doesn't run entry actions: the state event is history" do
    with_reactor(order, order_id: 'o1')
      .at(payment::Authorizing)
      .then { |result| expect(result.pairs).to be_empty }

    entered = with_reactor(order, order_id: 'o1').at(payment::Authorizing).history.last
    with_reactor(order, order_id: 'o1')
      .when(entered)
      .then(GWTAtTest::Notify, order_id: 'o1')
  end

  describe 'a part placed twice' do
    it 'raises for its handle, naming the paths' do
      expect { with_reactor(tour, tour_id: 't1').at(supplier::Holding) }
        .to raise_error(ArgumentError, /Holding is placed at booking\.primary\.holding, booking\.backup\.holding; pass at the path of one/)
    end

    it 'takes the path of one placement' do
      with_reactor(tour, tour_id: 't1')
        .at('booking.primary.holding')
        .when(supplier::ConfirmHold, tour_id: 't1')
        .then(supplier::Held, tour_id: 't1', trigger: supplier::ConfirmHold.type, source: 'booking.primary.holding',
                              target: 'booking.primary.held', exited: ['booking.primary.holding'],
                              entered: ['booking.primary.held'])
    end
  end

  describe 'misuse' do
    it 'raises on a reactor that is not a machine' do
      decider = Class.new(Sourced::Decider) do
        partition_by :device_id
        consumer_group 'gwt_at_decider'
      end

      expect { with_reactor(decider, device_id: 'd1').at(order::Placed) }
        .to raise_error(ArgumentError, /isn't a Sourced::Machine/)
    end

    it 'raises for a state not placed in the machine, or something else' do
      gwt = with_reactor(order, order_id: 'o1')

      expect { gwt.at(supplier::Holding) }.to raise_error(ArgumentError, /Holding isn't placed in it/)
      expect { gwt.at('placed.nowhere') }.to raise_error(ArgumentError, /no state at "placed.nowhere"/)
      expect { gwt.at('placed.payment.initial') }.to raise_error(ArgumentError, /is an initial position, not a state/)
      expect { gwt.at(order::Cancel) }.to raise_error(NoMethodError, /to_state_handle/)
    end

    it 'raises after when, and after then' do
      expect { with_reactor(order, order_id: 'o1').when(order::Place, order_id: 'o1').at(order::Placed) }
        .to raise_error(ArgumentError, /before when, not after/)

      gwt = with_reactor(order, order_id: 'o1').at(order::Placed).then([])
      expect { gwt.at(order::Cancelled) }.to raise_error(/already asserted/)
    end
  end
end
