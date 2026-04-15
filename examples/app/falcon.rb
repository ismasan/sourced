#!/usr/bin/env falcon-host
# frozen_string_literal: true

require_relative 'domain'
require_relative 'app'
require 'sourced/falcon'

service "ccc-app" do
  include Sourced::Falcon::Environment
  include Falcon::Environment::Rackup

  # url "http://localhost:9292"
  count 1
end
