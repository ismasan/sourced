# frozen_string_literal: true

require 'sourced/spawner'

module Sourced
  # A loop that does some work every +interval+ seconds, in a thread or fiber
  # spawned by {#start}, until {#stop} wakes it and waits for it to finish.
  # It can be started again after stopping, so a component can run it with
  # +start+ and +stop+ hooks.
  #
  # Subclasses implement {#tick}, and {#first_tick} for the work done right
  # after starting (by default, a tick).
  #
  #   class Beat < PeriodicTask
  #     def tick = logger.info('beat')
  #   end
  #
  #   beat = Beat.new(interval: 1)
  #   beat.start(task)  # spawns the loop
  #   beat.stop         # wakes it, and waits for it to finish
  #   beat.run          # or run the loop in the caller, until #stop
  class PeriodicTask
    # The parts of one run. +wake+ is what the loop sleeps on, closed to stop it;
    # +finished+ is closed when the loop returns; +started+ says the loop ran at
    # all, so a stop never waits on a block that was spawned but not run.
    Cycle = Struct.new(:wake, :finished, :started, :stopped)

    attr_reader :interval, :logger

    # @param interval [Numeric] seconds between ticks
    # @param stop_timeout [Numeric] seconds {#stop} waits for a tick in progress
    # @param logger [Object]
    def initialize(interval:, stop_timeout: 30, logger: NULL_LOGGER)
      @interval = interval
      @stop_timeout = stop_timeout
      @logger = logger
      @cycle = nil
    end

    # Spawn the loop into +context+ (see {Spawner}). A no-op while running,
    # or spawned and not stopped since.
    # @return [self]
    def start(context)
      return self if spawned?

      cycle = @cycle = Cycle.new(Thread::Queue.new, Thread::Queue.new, false, false)
      Spawner.into(context) { run_cycle(cycle) }
      self
    end

    # Run the loop in the caller, blocking until {#stop}.
    # @return [void]
    def run
      run_cycle(@cycle = Cycle.new(Thread::Queue.new, Thread::Queue.new, false, false))
    end

    # Wake the loop and wait for it to finish, for up to +timeout+ seconds.
    # Blocks the caller's thread, or yields to the fiber scheduler in a fiber.
    #
    # @return [Boolean] true unless the loop is still running when the timeout expires
    def stop(timeout: @stop_timeout)
      cycle = @cycle
      return true unless cycle

      cycle.stopped = true
      cycle.wake.close
      return true unless cycle.started

      cycle.finished.pop(timeout:)
      cycle.finished.closed?
    end

    # {#stop}, raising if the loop is still running after the timeout
    # @raise [ShutdownTimeoutError]
    def stop!(timeout: @stop_timeout)
      return if stop(timeout:)

      raise ShutdownTimeoutError, "#{name} still running after #{timeout}s"
    end

    # Whether the loop has started and not finished
    def running?
      cycle = @cycle
      !cycle.nil? && cycle.started && !cycle.finished.closed?
    end

    # Whether a loop was spawned and neither stopped nor finished: unlike
    # #running?, true before the spawned loop gets to run
    private def spawned?
      cycle = @cycle
      !cycle.nil? && !cycle.stopped && !cycle.finished.closed?
    end

    private

    # The work done every +interval+ seconds
    def tick = raise(NotImplementedError, "#{self.class} must implement #tick")

    # The work done right after starting
    def first_tick = tick

    def name = self.class.name

    def run_cycle(cycle)
      cycle.started = true
      return if cycle.stopped

      guarded { first_tick }
      until cycle.stopped
        cycle.wake.pop(timeout: interval) # nil on timeout, or right away once closed by #stop
        break if cycle.stopped

        guarded { tick }
      end
      logger.info "#{name}: stopped"
    ensure
      cycle.finished.close
    end

    # A tick that raises is logged, and the loop goes on to the next one:
    # a transient error (ex. a busy database) shouldn't end a poller for
    # the life of the process.
    def guarded
      yield
    rescue StandardError => e
      logger.error "#{name}: #{e.class}: #{e.message}\n  #{Array(e.backtrace).first(5).join("\n  ")}"
    end
  end
end
