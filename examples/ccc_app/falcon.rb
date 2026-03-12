#!/usr/bin/env falcon-host
# frozen_string_literal: true

require_relative 'domain'
require_relative 'app'
require 'sourced/ccc/falcon'

service "ccc-app" do
  include Sourced::CCC::Falcon::Environment
  include Falcon::Environment::Rackup

  # url "http://localhost:9292"
  count 1
end
