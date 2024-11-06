# frozen_string_literal: true

require_relative 'sors/version'

require 'sors/message'

module Sors
  class Error < StandardError; end
  ConcurrentAppendError = Class.new(Error)
  ConcurrentAckError = Class.new(Error)
  
  def self.config
    @config ||= Configuration.new
  end

  def self.configure(&)
    yield config if block_given?
    config.freeze
    config
  end

  def self.message_method_name(prefix, name)
    "__handle_#{prefix}_#{name.split('::').map(&:downcase).join('_')}"
  end

  DeciderInterface = Types::Interface[:handled_commands, :handle_command]
  ReactorInterface = Types::Interface[:consumer_info, :handled_events, :handle_events]
end

require 'sors/consumer'
require 'sors/decide'
require 'sors/evolve'
require 'sors/react'
require 'sors/sync'
require 'sors/configuration'
require 'sors/router'
require 'sors/message'
require 'sors/machine'
require 'sors/aggregate'
require 'sors/supervisor'
require 'sors/rails/railtie' if defined?(Rails::Railtie)
