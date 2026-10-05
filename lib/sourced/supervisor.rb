# frozen_string_literal: true

module Sourced
  # Top-level entry point for a background worker process. Boots the root of
  # Sourced's configuration tree inside its executor, so the dispatcher spawns
  # workers into the executor's task, then blocks until stopped (INT, TERM or
  # {#stop}) and tears the tree down.
  #
  # @example Standalone
  #   Sourced.register(Courses)
  #   Sourced::Supervisor.start
  #
  # @example Mounted in a host app: boots the host's whole tree
  #   App.mount('sourced', Sourced)
  #   Sourced::Supervisor.start
  class Supervisor
    SIGNALS = %w[INT TERM].freeze

    # @return [void] blocks until stopped
    def self.start(...)
      new(...).start
    end

    # @param config [Sourced::Component] Sourced's configuration, standalone or
    #   mounted in a host. The supervisor boots the root of its tree, and reads
    #   +executor+, +logger+ and +workers.count+ from it.
    def initialize(config: Sourced.config)
      @config = config
      @root = config.root
      @stop_reader, @stop_writer = IO.pipe
    end

    # Boot, block until stopped, then tear down. Whether it returns or raises
    # (ex. a component fails to build), the signal handlers it installed are
    # restored and its pipe is closed.
    # @return [void]
    def start
      previous_handlers = trap_signals
      root.build!
      executor = config['executor']
      logger.info("Sourced::Supervisor: starting #{config['workers.count']} workers with #{executor}")

      executor.start do |task|
        root.start!(task)
        task.spawn { shut_down_when_stopped }
      end
    ensure
      restore_signals(previous_handlers)
      close_pipe
    end

    # Ask the supervisor to tear down. Safe to call from a signal handler:
    # lifecycle methods take a lock, which Ruby doesn't allow in trap context,
    # so this only wakes up the task that tears down.
    # @return [Boolean] false if the supervisor isn't running (it returned, or
    #   failed to start), so there is nothing to stop
    def stop
      @stop_writer.write_nonblock('.')
      true
    rescue IO::WaitWritable
      true # already asked to stop
    rescue IOError
      false
    end

    private

    attr_reader :config, :root

    def logger = config['logger']

    # @return [Hash{String => Object}] the handlers replaced, by signal
    def trap_signals
      SIGNALS.to_h { |signal| [signal, Signal.trap(signal) { stop }] }
    end

    def restore_signals(previous_handlers)
      previous_handlers&.each { |signal, handler| Signal.trap(signal, handler) }
    end

    def close_pipe
      @stop_reader.close unless @stop_reader.closed?
      @stop_writer.close unless @stop_writer.closed?
    end

    # Spawned into the executor next to the workers, so teardown runs where they do
    def shut_down_when_stopped
      @stop_reader.read(1)
      logger.info('Sourced::Supervisor: stopping')
      root.teardown!
      logger.info('Sourced::Supervisor: stopped')
    end
  end
end
