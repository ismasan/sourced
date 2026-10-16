# frozen_string_literal: true

require 'securerandom'

# The holiday booking from sidereal's machine example, on checkpoint classes.
# Starting a booking enters BookingStarted, which runs three regions in
# parallel: the flight, the hotel (which nests room and board) and the extras.
# Once every region is final, `done` moves to checkout, which holds the
# itinerary for HOLD_SECONDS before it expires.
#
#   initial ─StartBooking─▶ BookingStarted ─────────────────done─▶ ItineraryReady ─Pay─▶ BookingConfirmed
#                           ├ flight: ChooseFlight ▶ FlightChosen ▶ FlightConfirmed      ├─ExpireHold─▶ HoldExpired
#                           ├ hotel:  ChooseHotel ▶ HotelChosen ─done─▶ HotelConfirmed   └─Cancel─▶ BookingCancelled
#                           │                       ├ room:  ChooseRoom ▶ RoomChosen
#                           │                       └ board: ChooseBoard ▶ BoardChosen
#                           └ extras: ToggleExtra (data) … FinishExtras ▶ ExtrasChosen
module MachineAcceptanceHoliday
  def self.command(name, **attrs) = Sourced::Command.define("acceptance_holidays.#{name}") do
    MachineAcceptanceHoliday.keyed(self, attrs)
  end

  def self.event(name, **attrs) = Sourced::Event.define("acceptance_holidays.#{name}") do
    MachineAcceptanceHoliday.keyed(self, attrs)
  end

  def self.keyed(message, attrs)
    message.attribute :booking_id, String
    attrs.each { |name, type| message.attribute name, type }
  end

  StartBooking = command(:start_booking, destination: String, depart_on: Date, nights: Integer, travellers: Integer)
  ChooseFlight = command(:choose_flight, flight: String)
  ConfirmFlight = command(:confirm_flight)
  ChooseHotel = command(:choose_hotel, hotel: String)
  ChooseRoom = command(:choose_room, room: String)
  ChooseBoard = command(:choose_board, board: String)
  ToggleExtra = command(:toggle_extra, extra: String)
  FinishExtras = command(:finish_extras)
  Pay = command(:pay, card_number: String)
  CancelBooking = command(:cancel_booking)
  # Dispatched by the checkout timer.
  ExpireHold = command(:expire_hold)

  TripPlanned = event(:trip_planned, destination: String, depart_on: Date, nights: Integer, travellers: Integer)
  FlightSelected = event(:flight_selected, flight: String)
  HotelSelected = event(:hotel_selected, hotel: String)
  RoomSelected = event(:room_selected, room: String)
  BoardSelected = event(:board_selected, board: String)
  ExtraAdded = event(:extra_added, extra: String)
  ExtraRemoved = event(:extra_removed, extra: String)
  PaymentDeclined = event(:payment_declined, last4: String, reason: String)
  PaymentCaptured = event(:payment_captured, reference: String, last4: String)

  class Trip
    attr_accessor :booking_id, :destination, :depart_on, :nights, :travellers,
                  :flight, :hotel, :room, :board, :hold_until, :declined, :reference
    attr_reader :extras

    def initialize = @extras = []

    def toggle(extra) = extras.include?(extra) ? extras.delete(extra) : extras << extra
  end

  class HolidayBooking < Sourced::Machine
    partition_by :booking_id

    HOLD_SECONDS = 180

    BookingStarted = Checkpoint(:booking_started)
    FlightChosen = Checkpoint(:flight_chosen)
    FlightConfirmed = Checkpoint(:flight_confirmed)
    HotelChosen = Checkpoint(:hotel_chosen)
    RoomChosen = Checkpoint(:room_chosen)
    BoardChosen = Checkpoint(:board_chosen)
    HotelConfirmed = Checkpoint(:hotel_confirmed)
    ExtrasChosen = Checkpoint(:extras_chosen)
    ItineraryReady = Checkpoint(:itinerary_ready)
    BookingConfirmed = Checkpoint(:booking_confirmed)
    BookingCancelled = Checkpoint(:booking_cancelled)
    HoldExpired = Checkpoint(:hold_expired)

    context { Trip.new }

    before_evolve { |trip, evt| trip.booking_id = evt.payload.booking_id }

    evolve TripPlanned do |trip, evt|
      trip.destination = evt.payload.destination
      trip.depart_on = evt.payload.depart_on
      trip.nights = evt.payload.nights
      trip.travellers = evt.payload.travellers
    end

    evolve(FlightSelected) { |trip, evt| trip.flight = evt.payload.flight }

    # Choosing another hotel re-enters HotelChosen, which restarts its room and
    # board regions: the earlier picks were for the other hotel.
    evolve HotelSelected do |trip, evt|
      trip.hotel = evt.payload.hotel
      trip.room = nil
      trip.board = nil
    end

    evolve(RoomSelected) { |trip, evt| trip.room = evt.payload.room }
    evolve(BoardSelected) { |trip, evt| trip.board = evt.payload.board }
    evolve(ExtraAdded) { |trip, evt| trip.toggle(evt.payload.extra) }
    evolve(ExtraRemoved) { |trip, evt| trip.toggle(evt.payload.extra) }
    evolve(PaymentDeclined) { |trip, evt| trip.declined = evt.payload.reason }
    evolve(PaymentCaptured) { |trip, evt| trip.reference = evt.payload.reference }

    # The hold starts when checkout is entered: its own checkpoint event.
    evolve(ItineraryReady) { |trip, evt| trip.hold_until = evt.created_at + HOLD_SECONDS }

    initial do
      command StartBooking => BookingStarted do |_trip, cmd|
        event TripPlanned, cmd.payload.to_h.slice(:destination, :depart_on, :nights, :travellers)
      end
    end

    checkpoint BookingStarted do
      command CancelBooking => BookingCancelled

      region :flight do
        initial do
          command(ChooseFlight => FlightChosen) { |_trip, cmd| event FlightSelected, flight: cmd.payload.flight }
        end

        checkpoint FlightChosen do
          command(ChooseFlight => FlightChosen) { |_trip, cmd| event FlightSelected, flight: cmd.payload.flight }
          command ConfirmFlight => FlightConfirmed
        end

        final FlightConfirmed
      end

      region :hotel do
        initial do
          command(ChooseHotel => HotelChosen) { |_trip, cmd| event HotelSelected, hotel: cmd.payload.hotel }
        end

        checkpoint HotelChosen do
          command(ChooseHotel => HotelChosen) { |_trip, cmd| event HotelSelected, hotel: cmd.payload.hotel }

          region :room do
            initial do
              command(ChooseRoom => RoomChosen) { |_trip, cmd| event RoomSelected, room: cmd.payload.room }
            end
            final RoomChosen
          end

          region :board do
            initial do
              command(ChooseBoard => BoardChosen) { |_trip, cmd| event BoardSelected, board: cmd.payload.board }
            end
            final BoardChosen
          end

          done HotelConfirmed
        end

        final HotelConfirmed
      end

      region :extras do
        initial do
          command ToggleExtra do |trip, cmd|
            extra = cmd.payload.extra
            event(trip.extras.include?(extra) ? ExtraRemoved : ExtraAdded, extra:)
          end

          command FinishExtras => ExtrasChosen
        end

        final ExtrasChosen
      end

      done ItineraryReady
    end

    checkpoint ItineraryReady do
      after(HOLD_SECONDS) { |_trip, _transition| dispatch ExpireHold }

      # A card ending in 0000 is declined, and checkout stays where it is, hold
      # timer included. Any other is charged.
      command Pay => BookingConfirmed | nil do |_trip, cmd|
        card = cmd.payload.card_number.delete(' ')
        if card.end_with?('0000')
          event PaymentDeclined, last4: card[-4..], reason: 'Your card was declined. Try another one.'
        else
          event PaymentCaptured, last4: card[-4..], reference: "SR-#{SecureRandom.alphanumeric(6).upcase}"
          to BookingConfirmed
        end
      end

      command ExpireHold => HoldExpired
      command CancelBooking => BookingCancelled
    end

    final BookingConfirmed
    final BookingCancelled
    final HoldExpired
  end

  # A guided tour, on the same messages helpers: the plan's gaps in travel terms.
  #
  #   initial ─RequestTour─▶ planning ════════════════════════════════════on_done─▶ reserving ─done─▶ confirmed ─on_done─▶ completed
  #                          └ itinerary (PlanningMachine's start)                  ├ outbound: FlightLeg  │  └ invoke AirportTransfer
  #                            └ outlining ─Price─▶ pricing ─always─▶ quoted ─Accept─▶ accepted               ├ return:   FlightLeg  └─▶ awaiting_visas ─children_done─▶ confirmed | visa_refused
  #                                                       └─always─▶ awaiting_sign_off  └ deposit:  on DepositReceived
  #                          planning ─PauseTour─▶ on_hold ─ResumeTour─▶ planning.history(deep: true)
  #
  # - Gap 1, compound starts at any depth: RequestTour enters planning,
  #   planning.itinerary and planning.itinerary.outlining in one transition.
  # - Gap 2, exits below the domain only: ReviseItinerary from quoted re-enters
  #   outlining without leaving planning or the itinerary.
  # - Gap 3, one machine mounted twice: FlightLeg as :outbound and :return. Both
  #   take the same commands and airline events (a booking's seats are held,
  #   ticketed and released together); only :outbound maps Released.
  # - Gap 4, `always` and guarded `done`: pricing is transient, and reserving
  #   joins to confirmed or awaiting_visas.
  # - Gap 5, history: ResumeTour restores the itinerary exactly (deep);
  #   ReopenPlanning restores it at its start (shallow).
  # - Gap 6, a compound's command targeting its sub-state: planning's ChangeDates
  #   moves the itinerary back to outlining.
  # - Gap 7, escalation: AcceptQuote reaches reserving through two on_done
  #   levels, in the deciding command.
  # - External events: a payment gateway's DepositReceived and an airline's
  #   SeatsTicketed move the tour with `on`.
  # - Children: one VisaApplication per traveller who needs one (spawned, joined
  #   with children_done), and an invoked AirportTransfer.
  REMINDER_SECONDS = 2 * 24 * 3600
  SEAT_HOLD_SECONDS = 20 * 60
  PAUSE_SECONDS = 7 * 24 * 3600

  RequestTour = command(:request_tour, budget: Integer, deposit: Integer, visas_for: Sourced::Types::Array[String])
  AddStop = command(:add_stop, city: String, cost: Integer)
  PriceItinerary = command(:price_itinerary)
  SignOffBudget = command(:sign_off_budget)
  ReviseItinerary = command(:revise_itinerary)
  AcceptQuote = command(:accept_quote)
  ChangeDates = command(:change_dates, depart_on: Date)
  PauseTour = command(:pause_tour)
  ResumeTour = command(:resume_tour)
  ReopenPlanning = command(:reopen_planning)
  CancelTour = command(:cancel_tour)
  HoldSeats = command(:hold_seats)
  ReleaseSeats = command(:release_seats)
  RequestVisas = command(:request_visas)
  GrantVisa = command(:grant_visa, traveller_id: String)
  RefuseVisa = command(:refuse_visa, traveller_id: String)
  ArrangeTransfer = command(:arrange_transfer, transfer_id: String)
  # Dispatched by entry actions and timers.
  StartPlannerChat = command(:start_planner_chat)
  ShareItinerary = command(:share_itinerary)
  EmailQuote = command(:email_quote)
  RemindTraveller = command(:remind_traveller)
  ChargeDeposit = command(:charge_deposit, amount: Integer)

  TourRequested = event(:tour_requested, budget: Integer, deposit: Integer, visas_for: Sourced::Types::Array[String])
  StopAdded = event(:stop_added, city: String, cost: Integer)
  DatesChanged = event(:dates_changed, depart_on: Date)
  TravellerReminded = event(:traveller_reminded)
  # Appended by other reactors: a payment gateway and an airline.
  DepositReceived = event(:deposit_received, reference: String)
  SeatsTicketed = event(:seats_ticketed, pnr: String)

  class Tour
    attr_accessor :budget, :deposit, :visas_for, :depart_on, :reference, :pnr, :reminders
    attr_reader :stops

    def initialize
      @stops = []
      @visas_for = []
      @reminders = 0
    end

    def total = stops.sum { |_city, cost| cost }
    def over_budget? = total > budget
    def no_visas_needed? = visas_for.empty?
  end

  # A guard that's neither a Symbol nor a lambda: anything with #call(context).
  module NothingToPrice
    def self.call(tour) = tour.stops.empty?
  end

  # Mounted only: the tour's context and partition. Outlined, priced, then quoted.
  class ItineraryMachine < Sourced::Machine
    Outlining = Checkpoint(:outlining)
    Pricing = Checkpoint(:pricing)
    AwaitingSignOff = Checkpoint(:awaiting_sign_off)
    Quoted = Checkpoint(:quoted)
    Accepted = Checkpoint(:accepted)

    evolve(StopAdded) { |tour, evt| tour.stops << [evt.payload.city, evt.payload.cost] }

    started Outlining do
      command(AddStop) { |_tour, cmd| event StopAdded, city: cmd.payload.city, cost: cmd.payload.cost }
      command PriceItinerary => Pricing
    end

    checkpoint Pricing do
      always Outlining, if: NothingToPrice
      always AwaitingSignOff, if: :over_budget?
      always Quoted
    end

    checkpoint AwaitingSignOff do
      command SignOffBudget => Quoted
      command ReviseItinerary => Outlining
    end

    checkpoint Quoted do
      entry { |_tour, _evt| dispatch EmailQuote }
      after(REMINDER_SECONDS) { |_tour, _evt| dispatch RemindTraveller }
      command(RemindTraveller) { |_tour, _cmd| event TravellerReminded }
      command ReviseItinerary => Outlining
      command AcceptQuote => Accepted
    end

    final Accepted
  end

  # Mounted only: its start is itself a compound checkpoint.
  class PlanningMachine < Sourced::Machine
    Itinerary = Checkpoint(:itinerary)
    Agreed = Checkpoint(:agreed)

    started Itinerary, ItineraryMachine, on_done: { ItineraryMachine::Accepted => Agreed } do
      entry { |_tour, _evt| dispatch ShareItinerary }
    end

    final Agreed
  end

  # Mounted twice by TourBooking, as :outbound and :return.
  class FlightLeg < Sourced::Machine
    SeatsHeld = Checkpoint(:seats_held)
    Ticketed = Checkpoint(:ticketed)
    Released = Checkpoint(:released)

    initial { command HoldSeats => SeatsHeld }

    checkpoint SeatsHeld do
      after(SEAT_HOLD_SECONDS) { |_tour, _evt| dispatch ReleaseSeats }
      on SeatsTicketed => Ticketed
      command ReleaseSeats => Released
    end

    final Ticketed
    final Released
  end

  # A child per traveller who needs a visa, in its own partition.
  class VisaApplication < Sourced::Machine
    partition_by :booking_id, :traveller_id

    Withdrawn = Checkpoint(:withdrawn)
    stopped Withdrawn

    Applied = Checkpoint(:applied)
    Granted = Checkpoint(:granted)
    Refused = Checkpoint(:refused)

    started Applied do
      command GrantVisa => Granted
      command RefuseVisa => Refused
    end

    final Granted
    final Refused
  end

  # Invoked once the tour is confirmed.
  class AirportTransfer < Sourced::Machine
    partition_by :booking_id, :transfer_id

    Dropped = Checkpoint(:dropped)
    stopped Dropped

    Requested = Checkpoint(:requested)
    Arranged = Checkpoint(:arranged)

    started(Requested) { command ArrangeTransfer => Arranged }
    final Arranged
  end

  # The payment gateway: charges the deposit the tour asks for.
  class DepositGateway < Sourced::Decider
    partition_by :booking_id

    command ChargeDeposit do |_context, cmd|
      event DepositReceived, booking_id: cmd.payload.booking_id, reference: "DP-#{cmd.payload.amount}"
    end
  end

  class TourBooking < Sourced::Machine
    partition_by :booking_id

    Planning = Checkpoint(:planning)
    OnHold = Checkpoint(:on_hold)
    Reserving = Checkpoint(:reserving)
    DepositPaid = Checkpoint(:deposit_paid)
    AwaitingVisas = Checkpoint(:awaiting_visas)
    Confirmed = Checkpoint(:confirmed)
    Completed = Checkpoint(:completed)
    Unavailable = Checkpoint(:unavailable)
    VisaRefused = Checkpoint(:visa_refused)
    Cancelled = Checkpoint(:cancelled)

    spawns VisaApplication, as: :visas, stop_on: Cancelled

    context { Tour.new }

    evolve TourRequested do |tour, evt|
      tour.budget = evt.payload.budget
      tour.deposit = evt.payload.deposit
      tour.visas_for = evt.payload.visas_for
    end

    evolve(DatesChanged) { |tour, evt| tour.depart_on = evt.payload.depart_on }
    evolve(TravellerReminded) { |tour, _evt| tour.reminders += 1 }
    evolve(DepositReceived) { |tour, evt| tour.reference = evt.payload.reference }
    evolve(SeatsTicketed) { |tour, evt| tour.pnr = evt.payload.pnr }

    initial do
      command RequestTour => Planning do |_tour, cmd|
        event TourRequested, cmd.payload.to_h.slice(:budget, :deposit, :visas_for)
      end
    end

    checkpoint Planning, PlanningMachine, on_done: { PlanningMachine::Agreed => Reserving } do
      entry { |_tour, _evt| dispatch StartPlannerChat }
      command ChangeDates => ItineraryMachine::Outlining do |_tour, cmd|
        event DatesChanged, depart_on: cmd.payload.depart_on
      end
      command PauseTour => OnHold
      command CancelTour => Cancelled
    end

    checkpoint OnHold do
      after(PAUSE_SECONDS) { |_tour, _evt| dispatch CancelTour }
      command ResumeTour => Planning.history(deep: true)
      command ReopenPlanning => Planning.history
      command CancelTour => Cancelled
    end

    checkpoint Reserving do
      entry { |tour, _evt| dispatch ChargeDeposit, amount: tour.deposit }
      command CancelTour => Cancelled

      region :outbound, FlightLeg, on_done: { FlightLeg::Released => Unavailable }
      region :return, FlightLeg

      region :deposit do
        initial { on DepositReceived => DepositPaid }
        final DepositPaid
      end

      done Confirmed, if: :no_visas_needed?
      done AwaitingVisas
    end

    checkpoint AwaitingVisas do
      command(RequestVisas) { |tour, _cmd| tour.visas_for.each { |id| spawn :visas, traveller_id: id } }

      children_done visas: Confirmed | VisaRefused do |_tour|
        to(machine.children(:visas).all? { |visa| visa.state == VisaApplication::Granted } ? Confirmed : VisaRefused)
      end

      command CancelTour => Cancelled
    end

    checkpoint Confirmed do
      invoke AirportTransfer, as: :transfer, keys: ->(_tour) { { transfer_id: 'airport' } },
                              on_done: { AirportTransfer::Arranged => Completed }
    end

    final Completed
    final Unavailable
    final VisaRefused
    final Cancelled
  end
end
