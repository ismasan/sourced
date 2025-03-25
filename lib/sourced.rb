# frozen_string_literal: true

require_relative 'sourced/version'

require 'sourced/message'

module Sourced
  class Error < StandardError; end
  ConcurrentAppendError = Class.new(Error)
  ConcurrentAckError = Class.new(Error)
  InvalidReactorError = Class.new(Error)
  
  def self.config
    @config ||= Configuration.new
  end

  def self.configure(&)
    yield config if block_given?
    config.freeze
    config
  end

  def self.register(reactor)
    Router.register(reactor)
  end

  def self.schedule_commands(commands)
    Router.schedule_commands(commands)
  end

  def self.message_method_name(prefix, name)
    "__handle_#{prefix}_#{name.split('::').map(&:downcase).join('_')}"
  end

  DeciderInterface = Types::Interface[:handled_commands, :handle_command, :on_exception]
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
