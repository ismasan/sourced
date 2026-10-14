# frozen_string_literal: true

require 'spec_helper'
require 'json'

module MachineChartTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_chart_test.#{name}") { attrs.each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_chart_test.#{name}") { attrs.each { |a| attribute a, String } }

  Open = command(:open, :trip_id)
  Note = command(:note, :trip_id)
  Noted = event(:noted, :trip_id)
  Quote = command(:quote, :trip_id)
  Book = command(:book, :trip_id)
  Reopen = command(:reopen, :trip_id)
  Remind = command(:remind, :trip_id)
  Pay = command(:pay, :trip_id)
  Ship = command(:ship, :trip_id)
  Cancel = command(:cancel, :trip_id)
  AddLeg = command(:add_leg, :trip_id, :leg_id)
  Confirm = command(:confirm, :trip_id, :leg_id)
  Capture = command(:capture, :trip_id, :charge_id)
  Submit = command(:submit, :trip_id)
  Accept = command(:accept, :trip_id)
  Approve = command(:approve, :trip_id)
  Unapprove = command(:unapprove, :trip_id)
  Send = command(:send, :trip_id)

  class LegMachine < Sourced::Machine
    partition_by :trip_id, :leg_id

    Added = Checkpoint(:added)
    Confirmed = Checkpoint(:confirmed)
    Dropped = Checkpoint(:dropped)

    start Added do
      command Confirm => Confirmed
    end
    final Confirmed
    on_stop Dropped
  end

  class ChargeMachine < Sourced::Machine
    partition_by :trip_id, :charge_id

    Requested = Checkpoint(:requested)
    Captured = Checkpoint(:captured)
    Declined = Checkpoint(:declined)
    Voided = Checkpoint(:voided)

    start Requested do
      command Capture => Captured
    end
    final Captured
    final Declined
    on_stop Voided
  end

  class TripMachine < Sourced::Machine
    partition_by :trip_id

    Opened = Checkpoint(:opened)
    Quoted = Checkpoint(:quoted)
    Rejected = Checkpoint(:rejected)
    Booked = Checkpoint(:booked)
    AwaitingPayment = Checkpoint(:awaiting_payment)
    PaymentFailed = Checkpoint(:payment_failed)
    Paid = Checkpoint(:paid)
    Shipped = Checkpoint(:shipped)
    LegsDone = Checkpoint(:legs_done)
    Done = Checkpoint(:done)
    Cancelled = Checkpoint(:cancelled)

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
        to Quoted
      end
      children_done :legs => LegsDone
    end

    checkpoint Quoted do
      command Book => Booked
    end

    checkpoint Booked do
      region :payment do
        initial { command Pay => AwaitingPayment }
        checkpoint AwaitingPayment do
          invoke ChargeMachine, as: :charge, keys: ->(_s) { { charge_id: 'c1' } },
                                on_done: { ChargeMachine::Captured => Paid, ChargeMachine::Declined => PaymentFailed }
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

  # Mounted twice, as two regions: one handle per checkpoint, at two paths.
  class ProviderMachine < Sourced::Machine
    Submitting = Checkpoint(:submitting)
    Accepted = Checkpoint(:accepted)

    initial { command Submit => Submitting }
    checkpoint Submitting do
      command Accept => Accepted
    end
    final Accepted
  end

  class DraftMachine < Sourced::Machine
    Draft = Checkpoint(:draft)
    Approved = Checkpoint(:approved)

    start Draft do
      command Approve => Approved
    end
    checkpoint Approved do
      command Unapprove => Draft
    end
  end

  # Starts at a compound checkpoint of its own.
  class UnsettledMachine < Sourced::Machine
    Open = Checkpoint(:open)
    Sent = Checkpoint(:sent)

    start Open, DraftMachine do
      command Send => Sent
    end
    final Sent
  end

  class PayrunMachine < Sourced::Machine
    partition_by :trip_id

    Unsettled = Checkpoint(:unsettled)
    Checking = Checkpoint(:checking)
    Settled = Checkpoint(:settled)

    initial { command Open => Unsettled }
    checkpoint Unsettled, UnsettledMachine, on_done: { UnsettledMachine::Sent => Checking }
    checkpoint Checking do
      region :check, ProviderMachine
      region :atlas, ProviderMachine, on_done: { ProviderMachine::Accepted => Settled }
    end
    final Settled
  end

  RSpec.describe 'Sourced::Machine charts' do
    def t(name) = "machine_chart_test.#{name}"
    def message(klass, **extra) = klass.new(payload: { trip_id: 't1', **extra })

    def trip(*commands)
      TripMachine.new(trip_id: 't1').tap do |m|
        commands.each { |cmd| m.decide(cmd) }
      end
    end

    def at_awaiting_payment = trip(message(Open), message(Quote), message(Book), message(Pay))

    def finish_charge(trip, final)
      trip.evolve([ChargeMachine::Finished.new(payload: { trip_id: 't1', charge_id: 'c1', final: })])
      trip.__decide_eventless
      trip
    end

    # What a JSON round trip should give back: the same values, with String keys.
    def stringify(value)
      case value
      in Hash then value.to_h { |k, v| [k.to_s, stringify(v)] }
      in Array then value.map { |v| stringify(v) }
      else value
      end
    end

    # The hash with every status value blanked out, for comparing shapes.
    def shape(value)
      case value
      in Hash then value.to_h { |k, v| [k, k == :status ? :status : shape(v)] }
      in Array then value.map { |v| shape(v) }
      else :value
      end
    end

    describe '.to_h' do
      let(:chart) { TripMachine.to_h }

      it 'describes the machine and its root region' do
        expect(chart).to include(
          id: 'root', type: 'compound', initial: 'initial', machine: 'MachineChartTest::TripMachine',
          partition_by: ['trip_id'], start: nil, stop: nil, status: nil
        )
        expect(chart[:states].keys).to eq(%w[initial opened quoted booked legs_done rejected done cancelled])
      end

      it 'describes checkpoints and their transitions' do
        expect(chart[:states]['initial']).to include(id: 'initial', key: 'initial', type: 'initial')
        expect(chart[:states]['initial'][:on]).to eq(
          t(:open) => { event: t(:open), label: 'Open', targets: ['opened'], optional: false, auto: true }
        )

        opened = chart[:states]['opened']
        expect(opened).to include(
          id: 'opened', key: 'opened', type: 'atomic', machine: 'MachineChartTest::TripMachine',
          entry: 1, exit: 0, after: [{ delay: 60 }], on_done: nil, invoke: [], mounts: [], states: {}, status: nil,
          children_done: [{ as: 'legs', targets: ['legs_done'] }]
        )
        expect(opened).not_to include(:message_type, :class)
        expect(opened[:on][t(:note)]).to include(targets: [], optional: true, auto: false)
        expect(opened[:on][t(:quote)]).to include(targets: %w[quoted rejected], optional: true, auto: false)
        expect(chart[:states]['done'][:type]).to eq('final')
      end

      it 'describes regions, joins and invoked children' do
        booked = chart[:states]['booked']
        expect(booked).to include(id: 'booked', type: 'parallel', on_done: 'done')
        payment = booked[:states]['payment']
        expect(payment).to include(id: 'booked.payment', key: 'payment', type: 'compound', initial: 'booked.payment.initial')
        expect(payment[:states].keys).to eq(%w[initial awaiting_payment payment_failed paid])
        expect(payment[:states]['awaiting_payment'][:id]).to eq('booked.payment.awaiting_payment')
        expect(payment[:states]['awaiting_payment'][:invoke]).to eq([
          { id: 'charge', src: 'MachineChartTest::ChargeMachine',
            on_done: { 'captured' => 'booked.payment.paid', 'declined' => 'booked.payment.payment_failed' } }
        ])
        # A transition to an ancestor region's checkpoint is listed where it's taken
        expect(payment[:states]['payment_failed'][:on][t(:cancel)][:targets]).to eq(['cancelled'])
      end

      it 'describes children' do
        expect(chart[:children]).to eq([
          { as: 'legs', machine: 'MachineChartTest::LegMachine', kind: 'spawn', checkpoint: nil, stop_on: ['cancelled'], status: nil },
          { as: 'charge', machine: 'MachineChartTest::ChargeMachine', kind: 'invoke',
            checkpoint: 'booked.payment.awaiting_payment', stop_on: [], status: nil }
        ])
      end

      it 'starts a child machine at its start checkpoint, with its Stop' do
        chart = LegMachine.to_h
        expect(chart).to include(initial: 'added', start: 'added',
                                 stop: { event: 'machine_chart_test.leg_machine.stop', label: 'Stop', targets: ['dropped'] })
        expect(chart[:states].keys).to eq(%w[added confirmed dropped])
      end

      it 'places a machine mounted twice at two paths' do
        checking = PayrunMachine.to_h[:states]['checking']
        expect(checking).to include(type: 'parallel', mounts: [
          { region: 'check', src: 'MachineChartTest::ProviderMachine', on_done: {} },
          { region: 'atlas', src: 'MachineChartTest::ProviderMachine', on_done: { 'checking.atlas.accepted' => 'settled' } }
        ])
        check, atlas = checking[:states].values_at('check', 'atlas')
        expect(check).to include(id: 'checking.check', initial: 'checking.check.initial')
        expect(atlas).to include(id: 'checking.atlas', initial: 'checking.atlas.initial')
        expect(check[:states].keys).to eq(%w[initial submitting accepted])
        expect(atlas[:states].keys).to eq(%w[initial submitting accepted])
        expect(check[:states]['submitting']).to include(id: 'checking.check.submitting', machine: 'MachineChartTest::ProviderMachine')
        expect(check[:states]['submitting'][:on][t(:accept)][:targets]).to eq(['checking.check.accepted'])
        expect(atlas[:states]['submitting'][:on][t(:accept)][:targets]).to eq(['checking.atlas.accepted'])
      end

      it 'nests a compound start in a compound checkpoint' do
        unsettled = PayrunMachine.to_h[:states]['unsettled']
        expect(unsettled).to include(id: 'unsettled', type: 'compound', initial: 'unsettled.open',
                                     mounts: [{ region: 'unsettled', src: 'MachineChartTest::UnsettledMachine',
                                                on_done: { 'unsettled.sent' => 'checking' } }])
        expect(unsettled[:states].keys).to eq(%w[open sent])

        open = unsettled[:states]['open']
        expect(open).to include(id: 'unsettled.open', type: 'compound', initial: 'unsettled.open.draft',
                                machine: 'MachineChartTest::UnsettledMachine')
        expect(open[:on][t(:send)][:targets]).to eq(['unsettled.sent'])
        expect(open[:states].keys).to eq(%w[draft approved])
        expect(open[:states]['approved']).to include(id: 'unsettled.open.approved', machine: 'MachineChartTest::DraftMachine')
        expect(open[:states]['approved'][:on][t(:unapprove)][:targets]).to eq(['unsettled.open.draft'])
      end

      it 'is JSON-serializable' do
        expect(JSON.parse(JSON.generate(chart))).to eq(stringify(chart))
        expect(JSON.parse(JSON.generate(PayrunMachine.to_h))).to eq(stringify(PayrunMachine.to_h))
      end
    end

    describe '#to_h' do
      it 'has the same shape as the definition, with statuses' do
        expect(shape(at_awaiting_payment.to_h)).to eq(shape(TripMachine.to_h))
      end

      it "fills in the machine's, regions' and checkpoints' status" do
        trip = at_awaiting_payment
        chart = trip.to_h
        booked = chart[:states]['booked']
        payment = booked[:states]['payment']

        expect(chart[:status]).to eq(
          active: true, current: 'booked', done: false,
          value: { 'booked' => { 'payment' => 'booked.payment.awaiting_payment', 'shipping' => 'booked.shipping.initial' } }
        )
        expect(chart[:states]['initial'][:status]).to eq(active: false, visited: true, entered_by: nil)
        expect(chart[:states]['opened'][:status]).to eq(active: false, visited: true, entered_by: nil)
        expect(chart[:states]['done'][:status]).to eq(active: false, visited: false, entered_by: nil)

        entered = trip.machine.entered_by
        expect(entered.payload).to have_attributes(trigger: t(:book), target: 'booked')
        expect(booked[:status]).to eq(
          active: true, visited: true,
          entered_by: { id: entered.id, type: t('trip_machine.transitioned'), at: entered.created_at.utc.iso8601(6) }
        )
        expect(payment[:status]).to eq(active: true, current: 'booked.payment.awaiting_payment', done: false)
        expect(payment[:states]['initial'][:status]).to include(active: false, visited: true)
        expect(booked[:states]['shipping'][:states]['initial'][:status]).to include(active: true, visited: true)
      end

      it "clears a region's visited checkpoints when its parent checkpoint is re-entered" do
        trip = finish_charge(at_awaiting_payment, 'declined')
        payment = trip.to_h[:states]['booked'][:states]['payment']
        expect(payment[:states]['awaiting_payment'][:status]).to include(visited: true)
        expect(payment[:status]).to include(current: 'booked.payment.payment_failed')

        [Reopen, Quote, Book].each { |klass| trip.decide(message(klass)) }
        payment = trip.to_h[:states]['booked'][:states]['payment']
        expect(payment[:states]['awaiting_payment'][:status]).to include(active: false, visited: false)
        expect(payment[:states]['payment_failed'][:status]).to include(active: false, visited: false)
        expect(payment[:status]).to eq(active: true, current: 'booked.payment.initial', done: false)
      end

      it 'keeps where an inactive region was left' do
        trip = finish_charge(at_awaiting_payment, 'declined')
        trip.decide(message(Cancel))
        payment = trip.to_h[:states]['booked'][:states]['payment']
        expect(payment[:status]).to eq(active: false, current: 'booked.payment.payment_failed', done: false)
        expect(trip.to_h[:status]).to include(done: true, value: 'cancelled')
      end

      it 'reflects what a decision just appended' do
        trip = trip(message(Open))
        trip.decide(message(Quote))                     # the block chooses with to
        expect(trip.to_h[:status][:current]).to eq('quoted')

        trip.decide(message(Book))                      # blockless: its only target
        expect(trip.to_h[:status][:current]).to eq('booked')
      end

      it "has no current position for a child machine that hasn't started" do
        expect(LegMachine.new(trip_id: 't1', leg_id: 'l1').to_h[:status]).to eq(active: true, current: nil, done: false, value: nil)
      end

      it "lists children's instances" do
        trip = trip(message(Open), message(AddLeg, leg_id: 'l1'), message(AddLeg, leg_id: 'l2'))
        trip.evolve([LegMachine::Finished.new(payload: { trip_id: 't1', leg_id: 'l1', final: 'confirmed' })])
        legs = trip.to_h[:children].find { |c| c[:as] == 'legs' }
        expect(legs[:status]).to eq(instances: [
          { keys: { 'leg_id' => 'l1' }, state: 'confirmed', final: true },
          { keys: { 'leg_id' => 'l2' }, state: 'added', final: false }
        ])
      end

      it "fills in a nested compound start's status" do
        payrun = PayrunMachine.new(trip_id: 't1')
        payrun.decide(message(Open))
        chart = payrun.to_h
        expect(chart[:status]).to include(current: 'unsettled', value: { 'unsettled' => { 'unsettled.open' => 'unsettled.open.draft' } })

        open = chart[:states]['unsettled'][:states]['open']
        expect(open[:status]).to include(active: true, visited: true)
        expect(open[:states]['draft'][:status]).to include(active: true, visited: true)
        expect(open[:states]['approved'][:status]).to include(active: false, visited: false)

        payrun.decide(message(Approve))
        open = payrun.to_h[:states]['unsettled'][:states]['open']
        expect(open[:states]['approved'][:status]).to include(active: true)
        expect(payrun.to_h[:status][:value]).to eq('unsettled' => { 'unsettled.open' => 'unsettled.open.approved' })
      end

      it 'fills in each placement of a machine mounted twice' do
        payrun = PayrunMachine.new(trip_id: 't1')
        [Open, Send, Submit].each { |klass| payrun.decide(message(klass)) }
        expect(payrun.to_h[:status][:value]).to eq(
          'checking' => { 'check' => 'checking.check.submitting', 'atlas' => 'checking.atlas.submitting' }
        )

        payrun.decide(message(Accept))
        expect(payrun.to_h[:status]).to include(current: 'settled', done: true)
        checking = payrun.to_h[:states]['checking']
        expect(checking[:states]['check'][:status]).to eq(active: false, current: 'checking.check.accepted', done: true)
        expect(checking[:states]['atlas'][:status]).to eq(active: false, current: 'checking.atlas.accepted', done: true)
      end

      it 'is JSON-serializable' do
        trip = at_awaiting_payment
        expect(JSON.parse(JSON.generate(trip.to_h))).to eq(stringify(trip.to_h))
      end
    end

    describe '.to_mermaid' do
      it 'draws a machine mounted twice once per region, and a nested compound start' do
        expect(PayrunMachine.to_mermaid).to eq(<<~MERMAID.chomp)
          stateDiagram-v2
            state "unsettled" as unsettled {
              state "open" as unsettled_open {
                state "draft" as unsettled_open_draft
                state "approved" as unsettled_open_approved
                [*] --> unsettled_open_draft
                unsettled_open_draft --> unsettled_open_approved : Approve
                unsettled_open_approved --> unsettled_open_draft : Unapprove
              }
              state "sent" as unsettled_sent
              [*] --> unsettled_open
              unsettled_open --> unsettled_sent : Send
              unsettled_sent --> [*]
            }
            state "checking" as checking {
              state "submitting" as checking_check_submitting
              state "accepted" as checking_check_accepted
              [*] --> checking_check_submitting : Submit
              checking_check_submitting --> checking_check_accepted : Accept
              checking_check_accepted --> [*]
              --
              state "submitting" as checking_atlas_submitting
              state "accepted" as checking_atlas_accepted
              [*] --> checking_atlas_submitting : Submit
              checking_atlas_submitting --> checking_atlas_accepted : Accept
              checking_atlas_accepted --> [*]
            }
            state "settled" as settled
            [*] --> unsettled : Open
            unsettled_sent --> checking : on_done
            checking_atlas_accepted --> settled : on_done
            settled --> [*]
        MERMAID
      end
    end
  end
end
