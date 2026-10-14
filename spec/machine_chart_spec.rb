# frozen_string_literal: true

require 'spec_helper'
require 'json'

module MachineChartTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_chart_test.#{name}") { attrs.each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_chart_test.#{name}") { attrs.each { |a| attribute a, String } }

  Open = command(:open, :trip_id)
  Opened = event(:opened, :trip_id)
  Note = command(:note, :trip_id)
  Noted = event(:noted, :trip_id)
  Quote = command(:quote, :trip_id)
  Quoted = event(:quoted, :trip_id)
  Rejected = event(:rejected, :trip_id)
  Book = command(:book, :trip_id)
  Booked = event(:booked, :trip_id)
  Reopen = command(:reopen, :trip_id)
  Remind = command(:remind, :trip_id)
  Pay = command(:pay, :trip_id)
  AwaitingPayment = event(:awaiting_payment, :trip_id)
  Paid = event(:paid, :trip_id)
  PaymentFailed = event(:payment_failed, :trip_id)
  Ship = command(:ship, :trip_id)
  Shipped = event(:shipped, :trip_id)
  Cancel = command(:cancel, :trip_id)
  Cancelled = event(:cancelled, :trip_id)
  Done = event(:done, :trip_id)
  AddLeg = command(:add_leg, :trip_id, :leg_id)
  LegAdded = event(:leg_added, :trip_id, :leg_id)
  Confirm = command(:confirm, :trip_id, :leg_id)
  LegConfirmed = event(:leg_confirmed, :trip_id, :leg_id)
  LegDropped = event(:leg_dropped, :trip_id, :leg_id)
  LegsDone = event(:legs_done, :trip_id)
  ChargeRequested = event(:charge_requested, :trip_id, :charge_id)
  Capture = command(:capture, :trip_id, :charge_id)
  Captured = event(:captured, :trip_id, :charge_id)
  Declined = event(:declined, :trip_id, :charge_id)
  Voided = event(:voided, :trip_id, :charge_id)

  class LegMachine < Sourced::Machine
    partition_by :trip_id, :leg_id

    start LegAdded do
      command Confirm => LegConfirmed
    end
    final LegConfirmed
    on_stop LegDropped
  end

  class ChargeMachine < Sourced::Machine
    partition_by :trip_id, :charge_id

    start ChargeRequested do
      command Capture => Captured
    end
    final Captured
    final Declined
    on_stop Voided
  end

  class TripMachine < Sourced::Machine
    partition_by :trip_id

    spawns LegMachine, as: :legs, stop_on: Cancelled

    initial do
      command Open => Opened
    end

    checkpoint Opened do
      entry { |_s, _evt| dispatch Remind }
      after(60) { |_s, _evt| dispatch Remind }
      command(Note) { |_s, _cmd| event Noted }
      command(AddLeg) { |_s, cmd| spawn :legs, leg_id: cmd.payload.leg_id }
      command Quote => Quoted | Rejected | nil do |_s, _cmd|
        event Quoted
      end
      children_done(:legs => LegsDone) { |_s| event LegsDone }
    end

    checkpoint Quoted do
      command Book => Booked
    end

    checkpoint Booked do
      region :payment do
        initial { command Pay => AwaitingPayment }
        checkpoint AwaitingPayment do
          invoke ChargeMachine, as: :charge, keys: ->(_s) { { charge_id: 'c1' } },
                                on_done: { Captured => Paid, Declined => PaymentFailed }
        end
        checkpoint PaymentFailed do
          command Cancel => Cancelled
        end
        final Paid
      end
      region :shipping do
        initial { command Ship => Shipped }
        final Shipped
      end
      command Reopen => Opened
      done Done
    end

    final LegsDone
    final Rejected
    final Done
    final Cancelled
  end

  RSpec.describe 'Sourced::Machine charts' do
    def t(name) = "machine_chart_test.#{name}"
    def evolved(*classes) = TripMachine.new(trip_id: 't1').tap { |m| m.evolve(classes.map { |k| message(k) }) }
    def message(klass, **extra) = klass.new(payload: { trip_id: 't1', **extra }.slice(*klass.payload_attribute_names))

    # What a JSON round trip should give back: the same values, with String keys.
    def stringify(value)
      case value
      when Hash then value.to_h { |k, v| [k.to_s, stringify(v)] }
      when Array then value.map { |v| stringify(v) }
      else value
      end
    end

    # The hash with every status value blanked out, for comparing shapes.
    def shape(value)
      case value
      when Hash then value.to_h { |k, v| [k, k == :status ? :status : shape(v)] }
      when Array then value.map { |v| shape(v) }
      else :value
      end
    end

    describe '.to_h' do
      let(:chart) { TripMachine.to_h }

      it 'describes the machine and its root region' do
        expect(chart).to include(
          id: 'root', type: 'compound', initial: 'root/initial', machine: 'MachineChartTest::TripMachine',
          partition_by: ['trip_id'], start: nil, stop: nil, status: nil
        )
        expect(chart[:states].keys).to eq(%w[initial Opened Quoted Booked LegsDone Rejected Done Cancelled])
      end

      it 'describes checkpoints and their transitions' do
        expect(chart[:states]['initial'][:on]).to eq(
          t(:open) => { event: t(:open), label: 'Open', targets: [t(:opened)], optional: false, auto: true }
        )

        opened = chart[:states]['Opened']
        expect(opened).to include(
          id: t(:opened), key: 'Opened', type: 'atomic', message_type: t(:opened), class: 'MachineChartTest::Opened',
          entry: 1, exit: 0, after: [{ delay: 60 }], on_done: nil, invoke: [], mounts: [], states: {}, status: nil,
          children_done: [{ as: 'legs', targets: [t(:legs_done)] }]
        )
        expect(opened[:on][t(:note)]).to include(targets: [], optional: true, auto: false)
        expect(opened[:on][t(:quote)]).to include(targets: [t(:quoted), t(:rejected)], optional: true, auto: false)
        expect(chart[:states]['Done'][:type]).to eq('final')
      end

      it 'describes regions, joins and invoked children' do
        booked = chart[:states]['Booked']
        expect(booked).to include(type: 'parallel', on_done: t(:done))
        payment = booked[:states]['payment']
        expect(payment).to include(id: 'payment', type: 'compound', initial: 'payment/initial')
        expect(payment[:states].keys).to eq(%w[initial AwaitingPayment PaymentFailed Paid])
        expect(payment[:states]['AwaitingPayment'][:invoke]).to eq([
          { id: 'charge', src: 'MachineChartTest::ChargeMachine', on_done: { t(:captured) => t(:paid), t(:declined) => t(:payment_failed) } }
        ])
        # A transition to an ancestor region's checkpoint is listed where it's taken
        expect(payment[:states]['PaymentFailed'][:on][t(:cancel)][:targets]).to eq([t(:cancelled)])
      end

      it 'describes children' do
        expect(chart[:children]).to eq([
          { as: 'legs', machine: 'MachineChartTest::LegMachine', kind: 'spawn', checkpoint: nil, stop_on: [t(:cancelled)], status: nil },
          { as: 'charge', machine: 'MachineChartTest::ChargeMachine', kind: 'invoke', checkpoint: t(:awaiting_payment), stop_on: [], status: nil }
        ])
      end

      it "starts a child machine at its start checkpoint, with its Stop" do
        chart = LegMachine.to_h
        expect(chart).to include(initial: t(:leg_added), start: t(:leg_added),
                                 stop: { event: 'machine_chart_test.leg_machine.stop', label: 'Stop', targets: [t(:leg_dropped)] })
        expect(chart[:states].keys).to eq(%w[LegAdded LegConfirmed LegDropped])
      end

      it 'is JSON-serializable' do
        expect(JSON.parse(JSON.generate(chart))).to eq(stringify(chart))
      end
    end

    describe '#to_h' do
      it 'has the same shape as the definition, with statuses' do
        trip = evolved(Opened, Quoted, Booked, AwaitingPayment)
        expect(shape(trip.to_h)).to eq(shape(TripMachine.to_h))
      end

      it "fills in the machine's, regions' and checkpoints' status" do
        trip = evolved(Opened, Quoted, Booked, AwaitingPayment)
        chart = trip.to_h
        booked = chart[:states]['Booked']
        payment = booked[:states]['payment']

        expect(chart[:status]).to eq(
          active: true, current: t(:booked), done: false,
          value: { t(:booked) => { 'payment' => t(:awaiting_payment), 'shipping' => 'shipping/initial' } }
        )
        expect(chart[:states]['initial'][:status]).to eq(active: false, visited: true, entered_by: nil)
        expect(chart[:states]['Opened'][:status]).to eq(active: false, visited: true, entered_by: nil)
        expect(chart[:states]['Done'][:status]).to eq(active: false, visited: false, entered_by: nil)

        entered = trip.machine.entered_by
        expect(booked[:status]).to eq(
          active: true, visited: true,
          entered_by: { id: entered.id, type: t(:booked), at: entered.created_at.utc.iso8601(6) }
        )
        expect(payment[:status]).to eq(active: true, current: t(:awaiting_payment), done: false)
        expect(payment[:states]['initial'][:status]).to include(active: false, visited: true)
        expect(booked[:states]['shipping'][:states]['initial'][:status]).to include(active: true, visited: true)
      end

      it "clears a region's visited checkpoints when its parent checkpoint is re-entered" do
        trip = evolved(Opened, Quoted, Booked, AwaitingPayment, PaymentFailed)
        payment = trip.to_h[:states]['Booked'][:states]['payment']
        expect(payment[:states]['AwaitingPayment'][:status]).to include(visited: true)

        trip.evolve([message(Opened), message(Quoted), message(Booked)])
        payment = trip.to_h[:states]['Booked'][:states]['payment']
        expect(payment[:states]['AwaitingPayment'][:status]).to include(active: false, visited: false)
        expect(payment[:states]['PaymentFailed'][:status]).to include(active: false, visited: false)
        expect(payment[:status]).to eq(active: true, current: 'payment/initial', done: false)
      end

      it 'keeps where an inactive region was left' do
        trip = evolved(Opened, Quoted, Booked, AwaitingPayment, Cancelled)
        payment = trip.to_h[:states]['Booked'][:states]['payment']
        expect(payment[:status]).to eq(active: false, current: t(:awaiting_payment), done: false)
        expect(trip.to_h[:status]).to include(done: true, value: t(:cancelled))
      end

      it 'reflects what a decision just emitted' do
        trip = evolved(Opened)
        trip.decide(message(Quote))                     # the block emits the checkpoint
        expect(trip.to_h[:status][:current]).to eq(t(:quoted))

        trip.decide(message(Book))                      # blockless: auto-appended
        expect(trip.to_h[:status][:current]).to eq(t(:booked))
      end

      it "has no current position for a child machine that hasn't started" do
        expect(LegMachine.new(trip_id: 't1', leg_id: 'l1').to_h[:status]).to eq(active: true, current: nil, done: false, value: nil)
      end

      it "lists children's instances" do
        trip = evolved(Opened)
        trip.evolve([message(LegAdded, leg_id: 'l1'), message(LegAdded, leg_id: 'l2'), message(LegConfirmed, leg_id: 'l1')])
        legs = trip.to_h[:children].find { |c| c[:as] == 'legs' }
        expect(legs[:status]).to eq(instances: [
          { keys: { 'leg_id' => 'l1' }, state: t(:leg_confirmed), final: true },
          { keys: { 'leg_id' => 'l2' }, state: t(:leg_added), final: false }
        ])
      end

      it 'is JSON-serializable' do
        trip = evolved(Opened, Quoted, Booked, AwaitingPayment)
        trip.evolve([message(ChargeRequested, charge_id: 'c1')])
        expect(JSON.parse(JSON.generate(trip.to_h))).to eq(stringify(trip.to_h))
      end
    end
  end
end
