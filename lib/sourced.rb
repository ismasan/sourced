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

  # Schedule commands for background processing by registered actors.
  #
  # @param commands [Array<Command>] Array of command instances to schedule
  # @return [void]
  # @example Schedule multiple commands
  #   commands = [
  #     CreateCart.new(stream_id: 'cart-123', payload: {}),
  #     AddItem.new(stream_id: 'cart-123', payload: { product_id: 'p1' })
  #   ]
  #   Sourced.schedule_commands(commands)
  def self.schedule_commands(commands)
    Router.schedule_commands(commands)
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
  #   @return [Hash] Consumer group information for this reactor
  # @!attribute [r] handled_events
  #   @return [Array<Class>] Event classes this reactor handles
  # @!attribute [r] handle_events
  #   @return [Method] Method to handle incoming events
  # @!attribute [r] on_exception
  #   @return [Method] Method to handle exceptions during event processing
  ReactorInterface = Types::Interface[:consumer_info, :handled_events, :handle_events, :on_exception]
end

require 'sourced/consumer'
require 'sourced/evolve'
require 'sourced/react'
require 'sourced/sync'
require 'sourced/configuration'
require 'sourced/router'
require 'sourced/message'
require 'sourced/actor'
require 'sourced/projector'
require 'sourced/supervisor'
require 'sourced/command_context'
# require 'sourced/rails/railtie' if defined?(Rails::Railtie)
