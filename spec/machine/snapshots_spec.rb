# frozen_string_literal: true

require 'spec_helper'
require 'sequel'
require 'date'
require_relative 'acceptance/support/holiday_booking'

module MachineSnapshotSpec
  Open = Sourced::Command.define('machine_snapshot_spec.open') { attribute :door_id, String }
  Close = Sourced::Command.define('machine_snapshot_spec.close') { attribute :door_id, String }
  Lock = Sourced::Command.define('machine_snapshot_spec.lock') { attribute :door_id, String }
  Turn = Sourced::Command.define('machine_snapshot_spec.turn') do
    attribute :door_id, String
    attribute :knob_id, String
  end
  Twist = Sourced::Command.define('machine_snapshot_spec.twist') do
    attribute :door_id, String
    attribute :knob_id, String
  end

  Inspect = Sourced::Command.define('machine_snapshot_spec.inspect') { attribute :door_id, String }
  Release = Sourced::Command.define('machine_snapshot_spec.release') do
    attribute :door_id, String
    attribute :knob_id, String
  end

  # A child that snapshots too: its entering event is its own Started.
  class Knob < Sourced::Machine
    partition_by :door_id, :knob_id
    snapshot every: 1, version: 1
    context { {} }

    started(:turning) { command Twist => :twisted }
    stopped :done
    state(:twisted) { command Release => :done }
  end

  # A child keyed by its parent's keys alone.
  class Audit < Sourced::Machine
    partition_by :door_id
    started :auditing
    stopped :dropped
  end

  class Entrance < Sourced::Machine
    partition_by :door_id
    snapshot every: 1, version: 1
    context { {} }

    initial { command Open => :opened }
    state :opened do
      command Turn, starts: Knob
      command Inspect, starts: Audit
      command Close => :closed
    end
    state(:closed) { command Open => :opened }
  end

  include MachineAcceptanceHoliday

  RSpec.describe 'Machine snapshots' do
    let(:db) { Sequel.sqlite }
    let(:store) { Sourced::Store.new(db).tap(&:install!) }

    # A doors machine (the same group and message types for every variant),
    # anonymous so variants can define the same state types.
    def door(group = 'machine_snapshot_spec.door', &block)
      Class.new(Sourced::Machine) do
        partition_by :door_id
        consumer_group group
        snapshot every: 1, version: 1
        context { {} }
        class_eval(&block)
      end
    end

    def knob(name, final: :done, prefix: 'machine_snapshot_spec.knob_a')
      klass = Class.new(Sourced::Machine)
      stub_const("MachineSnapshotSpec::#{name}", klass)
      klass.class_eval do
        partition_by :door_id, :knob_id
        consumer_group prefix
        started :turning
        stopped final
      end
      klass
    end

    def door_with(*children) = door do
      initial { command Open => :opened }
      state(:opened) { children.zip([Turn, Twist]) { |child, cmd| command cmd, starts: child } }
    end

    def codec(machine) = (@codecs ||= {})[machine] ||= Plumb::Codec::JSON.for(machine.snapshot_config.type)
    def encode(machine, state) = JSON.parse(JSON.generate(codec(machine).last.parse(state)))
    def decode(machine, json) = codec(machine).first.parse(json)

    # +machine+ after +history+, restored from a snapshot taken after its
    # first +at+ messages (through JSON, as the store keeps it).
    def restored(machine, values, history, at:)
      taken = machine.new(values).tap { |instance| instance.evolve(history.first(at)) }
      decoded = decode(machine, encode(machine, taken.snapshot_state))
      machine.snapshot_instance(values, Sourced::Snapshot.new(position: 0, first: decoded))
             .tap { |instance| instance.evolve(history.drop(at)) }
    end

    # What a machine instance is, made comparable: its stored state with
    # regions, history and children in a stable order and entering events
    # resolved, its status and what it accepts.
    def observed(instance)
      state = instance.snapshot_state
      runtime = state[:runtime]
      positions = runtime[:positions].map do |pos|
        pos.merge(entered_by: pos[:entered_by] && runtime[:events].fetch(pos[:entered_by]))
      end
      {
        positions: positions.sort_by { |pos| pos[:region] },
        history: runtime[:history].sort_by { |entry| entry[:region] },
        children: runtime[:children].sort_by { |entry| entry[:child] },
        context: state[:context],
        chart: instance.to_h,
        value: instance.machine.value,
        child_refs: instance.machine.children,
        commands: instance.available_commands,
        # Read off the instance, not its dump: what handlers are given.
        entered_by: instance.machine.entered_by&.then { |event| [event.id, event.respond_to?(:position) && event.position] }
      }
    end

    def outcome
      yield
    rescue StandardError => e
      [:raised, e.class]
    end

    # What an instance does next: whether it takes each probe command and what
    # it decides for it (each on a fresh instance), and what it decides
    # without a command after +next_message+. Events by class and payload:
    # ids and times differ.
    def behaviour(build, probes, next_message)
      allow(SecureRandom).to receive(:alphanumeric).and_return('fixed') # HolidayBooking's payment reference
      events = ->(list) { list.map { |evt| [evt.class, evt.payload.to_h] } }
      decisions = probes.map do |cmd|
        instance = build.call
        [cmd.class, instance.handles_command?(cmd), outcome { events.(instance.decide(cmd)) }]
      end
      eventless = next_message && outcome do
        instance = build.call
        instance.evolve([next_message])
        events.(instance.__decide_eventless)
      end
      { decisions:, eventless: }
    end

    # At every point of the partition's stored history, an instance restored
    # from a snapshot taken there (through JSON) and one replayed to there are
    # the same: what they hold, what they accept and decide (for each command
    # the scenario stored, timers' included), and after evolving the rest of
    # the history.
    def expect_round_trips(machine, values)
      history = store.read(machine.history_selector.bind(**values)).messages
      expect(history.size).to be >= 3
      probes = store.read_all(limit: 1000).messages.select do |msg|
        machine.handled_commands.include?(msg.class) && values.all? { |k, v| msg.payload.to_h[k] == v }
      end
      expect(probes).not_to be_empty
      full = observed(machine.new(values).tap { |instance| instance.evolve(history) })

      (0..history.size).each do |at|
        label = "snapshot after #{at} of #{history.size} messages"
        replay = -> { machine.new(values).tap { |instance| instance.evolve(history.first(at)) } }
        json = encode(machine, replay.call.snapshot_state)
        restore = -> { machine.snapshot_instance(values, Sourced::Snapshot.new(position: 0, first: decode(machine, json))) }

        expect(observed(restore.call)).to eq(observed(replay.call)), label
        expect(behaviour(restore, probes, history[at])).to eq(behaviour(replay, probes, history[at])), label
        expect(observed(restored(machine, values, history, at:))).to eq(full), label
      end
    end

    describe 'restoring' do
      def msg(klass, **attrs) = klass.new(payload: { booking_id: 'b1', **attrs })

      def deliver_timers(router, after:)
        Timecop.travel(Time.now + after) do
          store.update_schedule!
          router.drain
        end
      end

      it 'restores a machine with parallel regions, done joins and a timer as a full replay would' do
        router = Sourced::Router.new(store:, reactors: [HolidayBooking]).setup!
        run = ->(*cmds) { store.append(cmds) && router.drain }
        run.(msg(StartBooking, destination: 'lisbon', depart_on: Date.new(2026, 11, 1), nights: 3, travellers: 2))
        run.(msg(ChooseHotel, hotel: 'h1'), msg(ChooseRoom, room: 'sea'), msg(ChooseHotel, hotel: 'h2'))
        run.(msg(ChooseRoom, room: 'garden'), msg(ChooseBoard, board: 'half'), msg(ChooseFlight, flight: 'TP1'))
        run.(msg(ToggleExtra, extra: 'car'), msg(ConfirmFlight), msg(FinishExtras))
        run.(msg(Pay, card_number: '4111 1111 1111 0000'))
        run.(msg(Pay, card_number: '4111 1111 1111 1111'))
        deliver_timers(router, after: HolidayBooking::HOLD_SECONDS + 1)

        expect(HolidayBooking.load(store:, booking_id: 'b1').value).to eq(HolidayBooking::BookingConfirmed)
        expect_round_trips(HolidayBooking, { booking_id: 'b1' })
      end

      it 'restores a machine with parts, history states, children and timers as a full replay would' do
        router = Sourced::Router.new(store:, reactors: [TourBooking, VisaApplication, AirportTransfer, DepositGateway]).setup!
        run = ->(*cmds) { store.append(cmds) && router.drain }
        run.(msg(RequestTour, budget: 1000, deposit: 200, visas_for: %w[t1 t2]), msg(AddStop, city: 'Porto', cost: 400))
        run.(msg(PriceItinerary))
        run.(msg(ReviseItinerary))
        run.(msg(PriceItinerary))
        run.(msg(PauseTour))
        run.(msg(ResumeTour))
        deliver_timers(router, after: REMINDER_SECONDS + 1)
        run.(msg(AcceptQuote))
        run.(msg(HoldSeats))
        run.(msg(SeatsTicketed, pnr: 'ABC123'))
        run.(msg(RequestVisas))
        run.(GrantVisa.new(payload: { booking_id: 'b1', traveller_id: 't1' }))
        run.(RefuseVisa.new(payload: { booking_id: 'b1', traveller_id: 't2' }))

        loaded = TourBooking.load(store:, booking_id: 'b1')
        expect(loaded.value).to eq(TourBooking::VisaRefused)
        expect(loaded.children[VisaApplication].size).to eq(2)
        expect_round_trips(TourBooking, { booking_id: 'b1' })
      end

      it 'restores a machine through a state-started child to its end' do
        router = Sourced::Router.new(store:, reactors: [TourBooking, VisaApplication, AirportTransfer, DepositGateway]).setup!
        run = ->(*cmds) { store.append(cmds) && router.drain }
        run.(msg(RequestTour, budget: 1000, deposit: 200, visas_for: []), msg(AddStop, city: 'Porto', cost: 400))
        run.(msg(PriceItinerary))
        run.(msg(AcceptQuote))
        run.(msg(HoldSeats))
        run.(msg(SeatsTicketed, pnr: 'ABC123'))
        run.(ArrangeTransfer.new(payload: { booking_id: 'b1', transfer_id: 'airport' }))

        expect(TourBooking.load(store:, booking_id: 'b1').value).to eq(TourBooking::Completed)
        expect_round_trips(TourBooking, { booking_id: 'b1' })
      end

      it 'restores children keyed by the parent alone, and a child that snapshots itself' do
        router = Sourced::Router.new(store:, reactors: [Entrance]).setup!
        run = ->(cmd, **attrs) { store.append(cmd.new(payload: { door_id: 'd1', **attrs })) && router.drain }
        run.(Open)
        run.(Turn, knob_id: 'k1')
        run.(Inspect)
        run.(Twist, knob_id: 'k1')
        run.(Release, knob_id: 'k1')
        run.(Turn, knob_id: 'k2')
        run.(Close)

        expect(Entrance.load(store:, door_id: 'd1').children[Knob].map(&:final?)).to eq([true, false])
        expect(db[:sourced_snapshots].count).to eq(2) # Entrance's, and Knob k1's
        expect_round_trips(Entrance, { door_id: 'd1' })
        expect_round_trips(Knob, { door_id: 'd1', knob_id: 'k1' })
      end
    end

    describe 'on the Router' do
      it 'stores the runtime with the context, and reads only the history after it' do
        router = Sourced::Router.new(store:, reactors: [TourBooking, VisaApplication, AirportTransfer, DepositGateway]).setup!
        run = ->(cmd) { store.append(cmd) && router.drain }
        run.(RequestTour.new(payload: { booking_id: 'b1', budget: 1000, deposit: 200, visas_for: [] }))
        run.(AddStop.new(payload: { booking_id: 'b1', city: 'Porto', cost: 400 }))
        run.(AddStop.new(payload: { booking_id: 'b1', city: 'Faro', cost: 100 }))
        row = db[:sourced_snapshots].first
        expect(row).not_to be_nil

        allow(store).to receive(:read).and_call_original
        store.append(PriceItinerary.new(payload: { booking_id: 'b1' }))
        router.drain

        expect(store).to have_received(:read).with(anything, after_position: row[:position]).at_least(:once)
        expect(TourBooking.load(store:, booking_id: 'b1').value)
          .to eq(PlanningMachine::State => { ItineraryMachine::State => ItineraryMachine::Quoted })
      end
    end

    describe 'a changed definition' do
      let(:v1) do
        door do
          initial { command Open => :opened }
          state(:opened) { command Close => :closed }
          state(:closed) { command Open => :opened }
        end
      end

      def stored(machine, *commands)
        instance = machine.new(door_id: 'd1')
        commands.each { |cmd| instance.evolve(instance.decide(cmd.new(payload: { door_id: 'd1' }))) }
        encode(machine, instance.snapshot_state)
      end

      it 'decodes against the same definition' do
        expect { decode(v1, stored(v1, Open, Close)) }.not_to raise_error
      end

      {
        'a renamed state' => -> {
          door do
            initial { command Open => :ajar }
            state(:ajar) { command Close => :closed }
            state(:closed) { command Open => :ajar }
          end
        },
        'an added state' => -> {
          door do
            initial { command Open => :opened }
            state(:opened) { command Close => :closed }
            state(:closed) { command Open => :opened; command Lock => :locked }
            state :locked
          end
        },
        'an added region' => -> {
          door do
            initial { command Open => :opened }
            state :opened do
              command Close => :closed
              region(:latch) do
                initial { command Lock => :locked }
                state :locked
              end
            end
            state(:closed) { command Open => :opened }
          end
        }
      }.each do |change, variant|
        it "ignores a snapshot stored before #{change}" do
          json = stored(v1, Open, Close)

          expect { decode(instance_exec(&variant), json) }.to raise_error(Plumb::ParseError)
        end
      end

      it 'ignores a snapshot stored before a child was added, or a child state renamed' do
        knob_a = knob('KnobA')
        json = stored(door_with(knob_a), Open)

        expect { decode(door_with(knob_a, knob('KnobB', prefix: 'machine_snapshot_spec.knob_b')), json) }.to raise_error(Plumb::ParseError)
        renamed = knob('KnobRenamed', final: :finished, prefix: 'machine_snapshot_spec.knob_renamed')
        expect { decode(door_with(renamed), json) }.to raise_error(Plumb::ParseError)
      end

      it 'keeps a snapshot when children are declared in another order' do
        knob_a = knob('KnobA')
        knob_b = knob('KnobB', prefix: 'machine_snapshot_spec.knob_b')

        json = stored(door_with(knob_a, knob_b), Open)

        expect { decode(door_with(knob_b, knob_a), json) }.not_to raise_error
      end

      it 'reads the whole history, and stores a new snapshot, when the stored one is from another definition' do
        # Message types of their own: other examples' variants share the default group's.
        v1 = door('machine_snapshot_spec.router_door') do
          initial { command Open => :opened }
          state(:opened) { command Close => :closed }
          state(:closed) { command Open => :opened }
        end
        # Defined after the suite compiled the shared codec: a codec of its own.
        store.message_codec = Sourced::Store::MessageCodec.new.compile!
        Sourced::Router.new(store:, reactors: [v1]).setup!.tap do |router|
          [Open, Close, Open].each { |cmd| store.append(cmd.new(payload: { door_id: 'd1' })) && router.drain }
        end
        expect(db[:sourced_snapshots].count).to eq(1)

        v2 = door('machine_snapshot_spec.router_door') do
          initial { command Open => :opened }
          state(:opened) { command Close => :closed; command Lock => :locked }
          state(:closed) { command Open => :opened }
          state :locked
        end
        store.message_codec = Sourced::Store::MessageCodec.new.compile!
        router = Sourced::Router.new(store:, reactors: [v2]).setup!
        store.reclaim_consumer_groups # v2 claims Lock too: the group runs under its rule from here
        allow(store).to receive(:read).and_call_original
        store.append(Lock.new(payload: { door_id: 'd1' }))
        router.drain

        expect(store).to have_received(:read).with(anything, after_position: nil).at_least(:once)
        row = db[:sourced_snapshots].first
        expect(store.read_snapshot(v2.group_id, offset_id: row[:offset_id])).not_to be_nil # stored by v2
        expect(store.stats.groups.map { |g| g[:status] }.uniq).to eq(['active'])
        expect(v2.load(store:, door_id: 'd1').value).to eq(v2::Locked)
      end
    end

    describe 'definition checks' do
      it 'refuses a machine that reads its history through a selector' do
        machine = Class.new(Sourced::Machine) do
          partition_by :door_id
          consumer_group 'machine_snapshot_spec.selector'
          evolve Sourced::Selector[Open]
          snapshot every: 1, version: 1
          initial { command Open => :opened }
          state :opened
        end

        expect { machine.validate! }.to raise_error(Sourced::DefinitionError, /inline projector/)
      end

      it 'locks snapshot once the machine is validated' do
        machine = Class.new(Sourced::Machine) do
          partition_by :door_id
          consumer_group 'machine_snapshot_spec.locked'
          initial { command Open => :opened }
          state :opened
        end
        machine.validate!

        expect { machine.snapshot(every: 1, version: 1) }.to raise_error(Sourced::Machine::DefinitionError, /snapshot declared after/)
      end

      it 'keeps the definition check for machines that do not snapshot' do
        machine = Class.new(Sourced::Machine) do
          partition_by :door_id
          consumer_group 'machine_snapshot_spec.plain'
          initial { command Open => :opened }
          state :opened
        end

        expect(machine.validate!).to be(true)
        expect(machine.snapshot_config).to be_nil
      end
    end
  end
end
