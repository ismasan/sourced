# frozen_string_literal: true

require_relative 'sors/version'

require 'sors/message'

module Sors
  class Error < StandardError; end
  ConcurrentAppendError = Class.new(Error)
  
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

  ProcessBatch = Message.define('sors.batch.process')
end

require 'sors/decide'
require 'sors/evolve'
require 'sors/react'
require 'sors/react_sync'
require 'sors/configuration'
require 'sors/router'
require 'sors/machine'
require 'sors/supervisor'
require 'sors/rails/railtie' if defined?(Rails::Railtie)
