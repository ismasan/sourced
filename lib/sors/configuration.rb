# frozen_string_literal: true

require 'console' #  comes with async gem
require 'sors/backends/test_backend'

module Sors
  class Configuration
    attr_accessor :logger, :backend

    def initialize
      @logger = Console
      @backend = Backends::TestBackend.new
    end
  end
end
