# frozen_string_literal: true

require_relative 'sourced/version'

require 'securerandom'
require 'sourced/message'

# Sourced is an Event Sourcing / CQRS library for Ruby built around the "Decide, Evolve, React" pattern.
# It provides eventual consistency by default with an actor-like execution model for building 
# event-sourced applications.
#
# @example Basic setup with Sequel backend
#   Sourced.configure do |config|
#     config.backend = Sequel.connect('postgres://localhost/mydb')
#   end
#
# @example Register actors and projectors
#   Sourced.register(MyActor)
#   Sourced.register(MyProjector)
#
# @example Start background workers
#   Sourced::Supervisor.start(count: 10)
#
# @see https://github.com/ismasan/sourced
module Sourced
  # Base error class for all Sourced-specific exceptions
  class Error < StandardError; end
  
  # Raised when concurrent writes to the same stream are detected
  ConcurrentAppendError = Class.new(Error)
  
  # Raised when concurrent acknowledgments of the same event are detected
  ConcurrentAckError = Class.new(Error)
  
  # Raised when an invalid reactor is registered
  InvalidReactorError = Class.new(Error)
  
  BackendError = Class.new(Error)

  # Raised when a batch is partially processed before a message raises.
  # Carries the action_pairs for successfully processed messages,
  # the failed message, and the original exception as #cause.
  class PartialBatchError < Error
    attr_reader :action_pairs, :failed_message

    def initialize(action_pairs, failed_message, cause)
      @action_pairs = action_pairs
      @failed_message = failed_message
      super(cause.message)
      set_backtrace(cause.backtrace)
    end
  end

  class InvalidMessageError < Error
    attr_reader :message

    def initialize(message)
      @message = message
      super <<~ERR
      Invalid message #{message.class} ('#{message.type}')
      Errors: #{message.errors.inspect}
      ERR
    end
  end

  # Generate a new unique stream identifier, optionally with a prefix.
  # Stream IDs define concurrency boundaries - events for the same stream ID
  # are processed sequentially, while different stream IDs can be processed concurrently.
  #
  # @param prefix [String, nil] Optional prefix for the stream ID
  # @return [String] A new UUID-based stream ID
  # @example Generate a simple stream ID
  #   Sourced.new_stream_id #=> "123e4567-e89b-12d3-a456-426614174000"
  # @example Generate a prefixed stream ID
  #   Sourced.new_stream_id("cart") #=> "cart-123e4567-e89b-12d3-a456-426614174000"
  def self.new_stream_id(prefix = nil)
    uuid = SecureRandom.uuid
    prefix ? "#{prefix}-#{uuid}" : uuid
  end

  # Access the global Sourced configuration instance.
  #
  # @return [Configuration] The current configuration instance
  def self.config
    @config ||= Configuration.new
  end

  # Configure Sourced with backend, error handling, and other settings.
  # The configuration is frozen after the block executes to prevent 
  # accidental modification during runtime.
  #
  # @yield [config] Yields the configuration object for setup
  # @yieldparam config [Configuration] The configuration instance to configure
  # @return [Configuration] The frozen configuration
  # @example Basic configuration with Sequel
  #   Sourced.configure do |config|
  #     config.backend = Sequel.connect(ENV['DATABASE_URL'])
  #     config.logger = Logger.new(STDOUT)
  #   end
  # @example Configuration with error handling
  #   Sourced.configure do |config|
  #     config.backend = Sequel.connect(ENV['DATABASE_URL'])
  #     config.error_strategy do |s|
  #       s.retry(times: 3, after: 5)
  #       s.on_stop { |e, msg| Sentry.capture_exception(e) }
  #     end
  #   end
  def self.configure(&)
    yield config if block_given?
    config.setup!
    config.freeze
    config
  end

  # Register an Actor or Projector class to make it available for background processing.
  # Registered reactors can handle commands and react to events asynchronously.
  #
  # @param reactor [Class] Actor or Projector class that implements the reactor interface
  # @return [void]
  # @raise [InvalidReactorError] if the reactor doesn't implement required interface methods
  # @example Register an actor
  #   Sourced.register(CartActor)
  # @example Register a projector
  #   Sourced.register(CartListingsProjector)
  # @see Actor
  # @see Projector
  def self.register(reactor)
    Router.register(reactor)
  end

  # @return [Boolean]
  def self.registered?(reactor)
    Router.registered?(reactor)
  end

  # Append messages (probably commands) to a stream
  # auto-incrementing the sequence number
  # Raises if the message is invalid
  # This is meant for an app's front-end to dispatch commands into the system
  # so it's defensive and raises on error
  # helpers upstream of this can first validate the message
  # and surface errors back to the UI
  # TODO: in future we might want to restrict what messages
  # can be publicly dispatched. Whether that lives here, or
  # somewhere else, I'm not sure.
  #
  # @param message [Message] the mesagge to append
  # @raise [InvalidMessageError] if !message.valid?
  # @return [Message]
  # @example
  #   command = CreateCart.new(stream_id: 'cart-123', payload: {}),
  #   Sourced.dispatch(command)
  def self.dispatch(message)
    raise InvalidMessageError.new(message) unless message.valid?

    appended = config.backend.append_next_to_stream(message.stream_id, [message])
    raise BackendError, "Backend #{config.backend}#append_next_to_stream failed with message #{message.inspect}" unless appended

    message
  end

  class Loader
    def initialize(backend: Sourced.config.backend)
      @backend = backend
    end

    def load(actor, after: nil, upto: nil)
      after ||= actor.seq
      events = @backend.read_stream(actor.id, after:, upto:)
      actor.evolve(events)
      [actor, events]
    end
  end

  # Load or catch up an Actor from its event history
  # @example
  #   actor = MyActor.new(id: '123')
  #   Sourced.load(actor)
  #   actor.seq # Integer
  #
  # Actor must implement:
  #   #id() => String
  #   #seq() => Integer
  #   #evolve(events)
  #
  # It also supports passing a Reactor class (Actor, Evolver)
  # and a stream_id
  # @example
  #   actor, events = Sourced.load(MyActor, 'order-123')
  #   actor, events = Sourced.load(MyActor, 'order-123', after: 20)
  def self.load(*args)
    reactor, options = case args
    in [ReactorInterface => r, String => stream_id, Hash => opts]
      [r.new(id: stream_id), opts]
    in [ReactorInterface => r, String => stream_id]
      [r.new(id: stream_id), {}]
    in [Evolve => r, Hash => opts]
      [r, opts]
    in [Evolve => r]
      [r, {}]
    else
      raise ArgumentError, "expected a Reactor class and stream_id, or a Reactor instance, but got #{args.inspect}"
    end

    backend = options.delete(:backend) || config.backend
    Loader.new(backend:).load(reactor, **options)
  end

  # Load history for a reactor, or a stream id string
  # @example
  #   history = Sourced.history_for('order-123')
  #   history = Sourced.history_for('order-123', upto: 20)
  #   history = Sourced.history_for(order_actor)
  #
  # @param stream_id [String, #id, #stream_id]
  # @option after [nil, Integer] load messages after this sequence number
  # @option upto [nil, Integer] load messsages upto this sequence number
  # @return [Enumerable<Sourced::Message>]
  def self.history_for(stream_id, after: nil, upto: nil)
    stream_id = if stream_id.respond_to?(:stream_id)
      stream_id.stream_id
    elsif stream_id.respond_to?(:id)
      stream_id.id
    else
      stream_id
    end

    config.backend.read_stream(stream_id, after:, upto:)
  end

  # Build the topology graph from all registered async reactors.
  # Returns a flat array of CommandNode, EventNode, and AutomationNode structs.
  # The result is memoized; call Sourced.reset_topology to clear.
  #
  # @return [Array<Topology::CommandNode, Topology::EventNode, Topology::AutomationNode>]
  def self.topology
    @topology ||= Topology.build(Router.instance.async_reactors)
  end

  def self.reset_topology
    @topology = nil
  end

  # Generate a standardized method name for message handlers.
  # Used internally to create consistent handler method names.
  #
  # @param prefix [String] The handler type prefix (e.g., 'command', 'event')
  # @param name [String] The message class name
  # @return [String] The generated method name
  # @api private
  def self.message_method_name(prefix, name)
    "__handle_#{prefix}_#{name.split('::').map(&:downcase).join('_')}"
  end

  # @!group Type Interfaces
  
  # Interface that command handlers (Deciders) must implement.
  # @!attribute [r] handled_commands
  #   @return [Array<Class>] Command classes this decider handles
  # @!attribute [r] handle_command  
  #   @return [Method] Method to handle incoming commands
  # @!attribute [r] on_exception
  #   @return [Method] Method to handle exceptions during command processing
  DeciderInterface = Types::Interface[:handled_commands, :handle_command, :on_exception]
  
  # Interface that event handlers (Reactors) must implement.
  # @!attribute [r] consumer_info
  #   @return [Sourced::Consumer::ConsumerInfo] Consumer group information for this reactor
  # @!attribute [r] handled_messages
  #   @return [Array<Class>] Message classes this reactor handles
  # @!attribute [r] handle
  #   @return [Method] Method to handle incoming events
  # @!attribute [r] on_exception
  #   @return [Method] Method to handle exceptions during event processing
  ReactorInterface = Types::Interface[:handle_batch, :consumer_info, :handled_messages, :on_exception]
end

require 'sourced/consumer'
require 'sourced/actions'
require 'sourced/evolve'
require 'sourced/react'
require 'sourced/sync'
require 'sourced/configuration'
require 'sourced/router'
require 'sourced/message'
require 'sourced/actor'
require 'sourced/handler'
require 'sourced/projector'
require 'sourced/work_queue'
require 'sourced/inline_notifier'
require 'sourced/catchup_poller'
require 'sourced/dispatcher'
require 'sourced/supervisor'
require 'sourced/command_context'
require 'sourced/unit'
require 'sourced/topology'
# require 'sourced/rails/railtie' if defined?(Rails::Railtie)
