# frozen_string_literal: true

require_relative 'sors/version'

module Sors
  class Error < StandardError; end
  
  def self.config
    @config ||= Configuration.new
  end

  def self.configure(&)
    yield config if block_given?
    config.freeze
    config
  end
end

require 'sors/configuration'
require 'sors/machine'
require 'sors/router'
require 'sors/supervisor'
