# frozen_string_literal: true

require_relative 'sors/version'

module Sors
  class Error < StandardError; end
  # Your code goes here...
end

require 'sors/machine'
require 'sors/router'
require 'sors/supervisor'
