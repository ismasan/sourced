# frozen_string_literal: true

require 'sourced'
require 'logger'
require 'timecop'
require 'sourced/testing/rspec'

ENV['ENVIRONMENT'] ||= 'test'

# Helpers for specs that need message types the default codec deliberately
# can't serialize — types belonging to some app's own codec.
module CodecSpecHelpers
  # Build a message class without adding it to the global registry.
  #
  # Sourced.setup! refuses to boot when a registered message type can't be
  # serialized, and this suite plays several apps at once, so an app-specific
  # type must stay out of the registry every other spec's setup! walks.
  #
  # @param type_str [String] message type string
  # @param base [Class<Sourced::Message>]
  # @return [Class<Sourced::Message>]
  def self.unregistered_message(type_str, base: Sourced::Event, &payload_block)
    type_str.freeze unless type_str.frozen?

    Class.new(base) do
      def self.node_name = :data
      define_singleton_method(:type) { type_str }
      attribute :type, Sourced::Message::Types::Static[type_str]

      next unless payload_block

      payload_class = Class.new(Sourced::Message::Payload, &payload_block)
      const_set(:Payload, payload_class)
      attribute :payload, payload_class
      names = payload_class._schema.to_h.keys.map(&:to_sym).freeze
      define_singleton_method(:payload_attribute_names) { names }
    end
  end

  # A stand-in for Sourced::Message::Registry over a fixed set of classes, so a
  # MessageCodec can resolve unregistered types.
  class Registry
    def initialize(classes) = @classes = classes
    def all(&block) = @classes.each(&block)
    def [](type) = @classes.find { |klass| klass.type == type }
  end
end

RSpec.configure do |config|
  config.example_status_persistence_file_path = '.rspec_status'
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.include Sourced::Testing::RSpec

  # Stores share one codec per format, compiled once per process — Store#setup!
  # does it at boot, this does it for specs that build stores directly.
  config.before(:suite) { Sourced::Store::MessageCodec.default.compile! }
end
