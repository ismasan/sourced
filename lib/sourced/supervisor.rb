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

    # Boot, block until stopped, then tear down.
    # @return [void]
    def start
      root.build!
      executor = config['executor']
      logger.info("Sourced::Supervisor: starting #{config['workers.count']} workers with #{executor}")
      trap_signals

      executor.start do |task|
        root.start!(task)
        task.spawn { shut_down_when_stopped }
      end
    end

    # Ask the supervisor to tear down. Safe to call from a signal handler:
    # lifecycle methods take a lock, which Ruby doesn't allow in trap context,
    # so this only wakes up the task that tears down.
    # @return [void]
    def stop
      @stop_writer.write_nonblock('.')
    rescue IO::WaitWritable, IOError
      nil # already asked to stop
    end

    private

    attr_reader :config, :root

    def logger = config['logger']

    def trap_signals
      SIGNALS.each { |signal| Signal.trap(signal) { stop } }
    end

    # Spawned into the executor next to the workers, so teardown runs where they do
    def shut_down_when_stopped
      @stop_reader.read(1)
      logger.info('Sourced::Supervisor: stopping')
      root.teardown!
      logger.info('Sourced::Supervisor: stopped')
    ensure
      @stop_reader.close
      @stop_writer.close
    end
  end
end
