# frozen_string_literal: true

require 'spec_helper'
require 'sequel'
require 'date'
require_relative 'support/holiday_booking'

# Acceptance: sidereal's holiday booking example. Parallel regions (one nested
# two deep), `done` joins at two levels, a compound's self-transition that
# restarts its regions, data-only commands, an optional transition (`| nil`)
# and a checkout timer. Then a guided tour (see the support file's header):
# nested compound starts, history, `always`, a machine mounted twice, `on`,
# spawned and invoked children, and timers left behind.
module MachineAcceptanceHoliday
  Booking = HolidayBooking

  RSpec.describe 'Acceptance: holiday booking' do
    def msg(klass, **attrs) = klass.new(payload: { booking_id: 'b1', **attrs })
    def start = msg(StartBooking, destination: 'lisbon', depart_on: Date.new(2026, 11, 1), nights: 3, travellers: 2)
    def booking(*commands) = Booking.new(booking_id: 'b1').tap { |b| commands.each { |cmd| b.decide(cmd) } }

    def moves(events) = events.select do |e|
      Booking.transition_classes.include?(e.class)
    end.map { |e| [e.class, e.payload.entered] }

    # Everything up to checkout, in one of the orders a traveller could pick.
    def picks
      [
        msg(ChooseHotel, hotel: 'h1'), msg(ChooseFlight, flight: 'TP1'), msg(ChooseRoom, room: 'sea'),
        msg(ToggleExtra, extra: 'car'), msg(ChooseBoard, board: 'half'), msg(ConfirmFlight), msg(FinishExtras)
      ]
    end

    it 'runs the three regions in any order, and moves to checkout once all are final' do
      trip = booking(start)
      expect(trip.machine.value).to eq(Booking::BookingStarted => { flight: :initial, hotel: :initial,
                                                                    extras: :initial })

      *rest, finish = picks
      rest.each { |cmd| trip.decide(cmd) }
      expect(trip.machine.value).to eq(
        Booking::BookingStarted => { flight: Booking::FlightConfirmed, hotel: Booking::HotelConfirmed,
                                     extras: :initial }
      )

      events = trip.decide(finish)
      expect(moves(events)).to eq([
                                    [Booking::ExtrasChosen, %w[booking_started.extras.extras_chosen]],
                                    [Booking::ItineraryReady, %w[itinerary_ready]]
                                  ])
      expect(events.last.payload.exited).to eq(%w[
                                                 booking_started.flight.flight_confirmed booking_started.hotel.hotel_confirmed booking_started.extras.extras_chosen booking_started
                                               ])
      expect(trip.context).to have_attributes(flight: 'TP1', hotel: 'h1', room: 'sea', board: 'half', extras: %w[car])
      expect(trip.context.hold_until).to be_within(1).of(events.last.created_at + Booking::HOLD_SECONDS)
    end

    it "joins the hotel's room and board regions with their own done, in the deciding command" do
      events = booking(start, msg(ChooseHotel, hotel: 'h1'),
                       msg(ChooseRoom, room: 'sea')).decide(msg(ChooseBoard, board: 'half'))

      expect(moves(events)).to eq([
                                    [Booking::BoardChosen, %w[booking_started.hotel.hotel_chosen.board.board_chosen]],
                                    [Booking::HotelConfirmed, %w[booking_started.hotel.hotel_confirmed]]
                                  ])
    end

    it 'restarts room and board when another hotel is chosen' do
      trip = booking(start, msg(ChooseHotel, hotel: 'h1'), msg(ChooseRoom, room: 'sea'))
      events = trip.decide(msg(ChooseHotel, hotel: 'h2'))

      expect(events.last.payload.exited).to eq(%w[booking_started.hotel.hotel_chosen.room.room_chosen
                                                  booking_started.hotel.hotel_chosen])
      expect(events.last.payload.entered).to eq(%w[booking_started.hotel.hotel_chosen])
      expect(trip.machine.value).to eq(
        Booking::BookingStarted => { flight: :initial,
                                     hotel: { Booking::HotelChosen => { room: :initial, board: :initial } }, extras: :initial }
      )
      expect(trip.context).to have_attributes(hotel: 'h2', room: nil)
    end

    it 'toggles extras as data, without moving' do
      trip = booking(start)
      expect(moves(trip.decide(msg(ToggleExtra, extra: 'car')))).to eq([])
      trip.decide(msg(ToggleExtra, extra: 'car'))

      expect(trip.context.extras).to eq([])
      expect(trip.machine.in?(:extras, :initial)).to be(true)
    end

    it 'stays at checkout when a card is declined, and confirms with another' do
      trip = booking(start, *picks)

      declined = trip.decide(msg(Pay, card_number: '4111 1111 1111 0000'))
      expect(declined.map(&:class)).to eq([PaymentDeclined])
      expect(trip.machine.value).to eq(Booking::ItineraryReady)

      paid = trip.decide(msg(Pay, card_number: '4111 1111 1111 1111'))
      expect(paid.map(&:class)).to eq([PaymentCaptured, Booking::BookingConfirmed])
      expect(trip.machine).to be_final
      expect(trip.context.reference).to match(/\ASR-[A-Z0-9]{6}\z/)
    end

    it 'cancels from any region configuration' do
      trip = booking(start, msg(ChooseHotel, hotel: 'h1'), msg(ChooseFlight, flight: 'TP1'))
      events = trip.decide(msg(CancelBooking))

      expect(events.map(&:class)).to eq([Booking::BookingCancelled])
      expect(events.last.payload.exited).to eq(%w[booking_started.flight.flight_chosen booking_started.hotel.hotel_chosen
                                                  booking_started])
    end

    it 'refuses commands no active checkpoint accepts' do
      expect(booking(start).handles_command?(msg(Pay, card_number: '4111111111111111'))).to be(false)
      expect(booking(start).handles_command?(msg(ChooseRoom, room: 'sea'))).to be(false)
    end

    it 'renders' do
      expect(Booking.to_h[:states].keys).to include('initial', 'booking_started', 'itinerary_ready',
                                                    'booking_confirmed')
      expect(Booking.to_mermaid).to include('booking_started --> itinerary_ready : done')
      nodes = Sourced::Topology.build([Booking])
      pay = nodes.find { |n| n.type == 'command' && n.id == Pay.type }
      expect(pay.produces).to include(Booking::BookingConfirmed.type, PaymentCaptured.type, PaymentDeclined.type)
    end

    describe 'on a store' do
      let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
      let(:router) { Sourced::Router.new(store:, reactors: [Booking]).setup! }

      def run(*commands)
        store.append(commands)
        router.drain
      end

      def snapshot = Booking.load(store:, booking_id: 'b1')

      def deliver_timers(after:)
        Timecop.travel(Time.now + after) do
          promoted = store.update_schedule!
          router.drain
          promoted
        end
      end

      before { run(start, *picks) }

      it 'holds the itinerary, and expires it when the hold runs out' do
        expect(snapshot.value).to eq(Booking::ItineraryReady)
        expect(deliver_timers(after: Booking::HOLD_SECONDS - 5)).to eq(0)

        run(msg(Pay, card_number: '4111 1111 1111 0000'))
        expect(snapshot.value).to eq(Booking::ItineraryReady)

        expect(deliver_timers(after: Booking::HOLD_SECONDS + 1)).to eq(1)
        expect(snapshot.value).to eq(Booking::HoldExpired)
        expect(store.read_all(limit: 200).messages.map(&:class)).to include(ExpireHold, Booking::HoldExpired)
      end

      it 'ignores the hold timer once paid' do
        run(msg(Pay, card_number: '4111 1111 1111 1111'))

        expect(deliver_timers(after: Booking::HOLD_SECONDS + 1)).to eq(1)
        expect(snapshot.value).to eq(Booking::BookingConfirmed)
        expect(store.read_all(limit: 200).messages.map(&:class)).not_to include(Booking::HoldExpired)
      end
    end
  end

  RSpec.describe 'Acceptance: guided tour' do
    def msg(klass, **attrs) = klass.new(payload: { booking_id: 'b1', **attrs })
    def request(visas_for: []) = msg(RequestTour, budget: 1000, deposit: 200, visas_for:)
    def tour(*commands) = TourBooking.new(booking_id: 'b1').tap { |t| commands.each { |cmd| t.decide(cmd) } }
    def moves(events) = events.select { |e| Sourced::Machine::Checkpoint === e }.map { |e| [e.payload.trigger, e.payload.entered] }
    def quoted = [request, msg(AddStop, city: 'Porto', cost: 400), msg(PriceItinerary)]
    def over_budget = [request, msg(AddStop, city: 'Porto', cost: 700), msg(AddStop, city: 'Faro', cost: 600), msg(PriceItinerary)]

    it 'enters planning, its itinerary and outlining with one transition, running entry actions outermost first' do
      trip = TourBooking.new(booking_id: 'b1')
      events = trip.decide(request)

      expect(moves(events)).to eq([['acceptance_holidays.request_tour', %w[planning planning.itinerary planning.itinerary.outlining]]])
      expect(trip.machine.value).to eq(TourBooking::Planning => { PlanningMachine::Itinerary => ItineraryMachine::Outlining })
      expect(trip.__react(events.last).map(&:class)).to eq([StartPlannerChat, ShareItinerary])
    end

    it 'prices through a transient checkpoint, by its guards' do
      expect(moves(tour(request).decide(msg(PriceItinerary))).last).to eq(['always', %w[planning.itinerary.outlining]])
      expect(moves(tour(*quoted[0..1]).decide(msg(PriceItinerary))).last).to eq(['always', %w[planning.itinerary.quoted]])
      expect(tour(*over_budget).machine.value).to eq(TourBooking::Planning => { PlanningMachine::Itinerary => ItineraryMachine::AwaitingSignOff })
      expect(tour(*over_budget, msg(SignOffBudget)).machine.in?(ItineraryMachine::Quoted)).to be(true)
    end

    it 'revises a quote by leaving only the quote: planning and the itinerary stay' do
      trip = tour(*quoted)
      events = trip.decide(msg(ReviseItinerary))

      expect(events.last.payload.to_h).to include(exited: %w[planning.itinerary.quoted], entered: %w[planning.itinerary.outlining])
      expect(trip.__react(events.last)).to eq([])
    end

    it "moves the itinerary back to outlining with planning's own command" do
      trip = tour(*quoted)
      events = trip.decide(msg(ChangeDates, depart_on: Date.new(2027, 5, 1)))

      expect(events.map(&:class)).to eq([DatesChanged, TourBooking.checkpoint_class(ItineraryMachine::Outlining)])
      expect(events.last.payload.to_h).to include(exited: %w[planning.itinerary.quoted planning.itinerary],
                                                  entered: %w[planning.itinerary planning.itinerary.outlining])
      expect(trip.__react(events.last).map(&:class)).to eq([ShareItinerary])
      expect(trip.context.depart_on).to eq(Date.new(2027, 5, 1))
    end

    it 'resumes a paused tour where it was (deep history), or reopens planning at its start (shallow)' do
      paused = -> { tour(*over_budget, msg(PauseTour)) }
      expect(paused.call.machine.value).to eq(TourBooking::OnHold)

      resumed = paused.call.decide(msg(ResumeTour))
      expect(resumed.last.payload.entered).to eq(%w[planning planning.itinerary planning.itinerary.awaiting_sign_off])

      reopened = paused.call.decide(msg(ReopenPlanning))
      expect(reopened.last.payload.entered).to eq(%w[planning planning.itinerary planning.itinerary.outlining])
    end

    it 'accepts a quote and escalates through two on_done levels to reserving, in the same decision' do
      trip = tour(*quoted)
      events = trip.decide(msg(AcceptQuote))

      expect(moves(events)).to eq([
        ['acceptance_holidays.accept_quote', %w[planning.itinerary.accepted]],
        ['on_done', %w[planning.agreed]],
        ['on_done', %w[reserving]]
      ])
      expect(events.last).to be_a(TourBooking::Reserving)
      expect(trip.machine.value).to eq(TourBooking::Reserving => { outbound: :initial, return: :initial, deposit: :initial })
      expect(trip.__react(events.last).map { |cmd| [cmd.class, cmd.payload.amount] }).to eq([[ChargeDeposit, 200]])
    end

    it 'runs one flight leg machine as both legs, with one class for both' do
      trip = tour(*quoted, msg(AcceptQuote))
      held = trip.decide(msg(HoldSeats))

      expect(held.map(&:class).uniq).to eq([TourBooking.checkpoint_class(FlightLeg::SeatsHeld)])
      expect(held.map { |e| e.payload.target }).to eq(%w[reserving.outbound.seats_held reserving.return.seats_held])

      # Only the outbound leg maps Released: losing the seats makes the tour unavailable.
      released = trip.decide(msg(ReleaseSeats))
      expect(moves(released).last).to eq(['on_done', %w[unavailable]])
      expect(trip.machine.value).to eq(TourBooking::Unavailable)
    end

    it 'renders' do
      chart = TourBooking.to_h
      expect(chart[:states]['on_hold'][:on][ResumeTour.type][:targets]).to eq(['planning.$deep_history'])
      expect(TourBooking.to_mermaid).to include('on_hold --> planning : ResumeTour (H*)', 'reserving --> confirmed : done [no_visas_needed?]')
      nodes = Sourced::Topology.build([TourBooking, VisaApplication, AirportTransfer, DepositGateway])
      accept = nodes.find { |n| n.type == 'command' && n.id == AcceptQuote.type }
      expect(accept.produces).to include(TourBooking::Reserving.type)
    end

    describe 'on a store' do
      let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
      let(:router) { Sourced::Router.new(store:, reactors: [TourBooking, VisaApplication, AirportTransfer, DepositGateway]).setup! }

      def run(*messages)
        store.append(messages)
        router.drain
      end

      def snapshot = TourBooking.load(store:, booking_id: 'b1')
      def types = store.read_all(limit: 500).messages.map(&:class)

      def deliver_timers(after:)
        Timecop.travel(Time.now + after) do
          promoted = store.update_schedule!
          router.drain
          promoted
        end
      end

      def reserve(visas_for: [])
        run(request(visas_for:), msg(AddStop, city: 'Porto', cost: 400))
        run(msg(PriceItinerary))
        run(msg(AcceptQuote))
        run(msg(HoldSeats))
      end

      it "takes the gateway's deposit and the airline's tickets, then arranges the transfer" do
        reserve
        expect(snapshot.value).to eq(TourBooking::Reserving => {
          outbound: FlightLeg::SeatsHeld, return: FlightLeg::SeatsHeld, deposit: TourBooking::DepositPaid
        })

        run(msg(SeatsTicketed, pnr: 'ABC123'))
        expect(snapshot.value).to eq(TourBooking::Confirmed)
        expect(snapshot.children[MachineAcceptanceHoliday::AirportTransfer].map(&:final?)).to eq([false])

        run(ArrangeTransfer.new(payload: { booking_id: 'b1', transfer_id: 'airport' }))
        expect(snapshot.value).to eq(TourBooking::Completed)
        expect(snapshot.context).to have_attributes(reference: 'DP-200', pnr: 'ABC123')
      end

      it 'checks visas, one child per traveller, and joins on their results' do
        reserve(visas_for: %w[t1 t2])
        run(msg(SeatsTicketed, pnr: 'ABC123'))
        expect(snapshot.value).to eq(TourBooking::AwaitingVisas)

        run(msg(RequestVisas))
        expect(snapshot.children[MachineAcceptanceHoliday::VisaApplication].map { |ref| ref.keys[:traveller_id] }).to contain_exactly('t1', 't2')

        run(GrantVisa.new(payload: { booking_id: 'b1', traveller_id: 't1' }))
        expect(snapshot.value).to eq(TourBooking::AwaitingVisas)
        run(RefuseVisa.new(payload: { booking_id: 'b1', traveller_id: 't2' }))
        expect(snapshot.value).to eq(TourBooking::VisaRefused)
      end

      it 'releases both legs when the seat hold runs out, and ignores the other leg\'s timer' do
        reserve
        expect(deliver_timers(after: SEAT_HOLD_SECONDS + 1)).to eq(2)

        expect(snapshot.value).to eq(TourBooking::Unavailable)
        expect(types.count(ReleaseSeats)).to eq(2)
        expect(types.count(TourBooking::Unavailable)).to eq(1)
      end

      it 'reminds the traveller about the current quote only' do
        run(*quoted)
        run(msg(ReviseItinerary))
        run(msg(PriceItinerary))

        expect(deliver_timers(after: REMINDER_SECONDS + 1)).to eq(2)
        expect(snapshot.context.reminders).to eq(1)
        expect(types.count(TravellerReminded)).to eq(1)
      end

      it "doesn't cancel a resumed tour when the pause runs out" do
        run(*quoted)
        run(msg(PauseTour))
        run(msg(ResumeTour))

        # The pause's CancelTour, and the reminders of both entries into the quote.
        expect(deliver_timers(after: PAUSE_SECONDS + 1)).to eq(3)
        expect(types).to include(CancelTour)
        expect(snapshot.context.reminders).to eq(1)
        expect(snapshot.value).to eq(TourBooking::Planning => { PlanningMachine::Itinerary => ItineraryMachine::Quoted })
      end
    end
  end
end
