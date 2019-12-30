# frozen_string_literal: true

require 'securerandom'
require 'sourced/version'
require 'sourced/aggregate_repo'
require 'sourced/mem_event_store'
require 'sourced/configuration'

module Sourced
  def self.uuid
    SecureRandom.uuid
  end

  def self.configuration
    @configuration ||= Configuration.new
  end

  def self.configure(&_block)
    conf = Configuration.new
    yield conf
    @configuration = conf
  end
end

require 'sourced/errors'
require 'sourced/eventable'
require 'sourced/event'
require 'sourced/persistable'
require 'sourced/aggregate'
require 'sourced/subscribers'
require 'sourced/entity_session'
require 'sourced/entity_repo'
