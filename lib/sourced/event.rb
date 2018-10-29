require 'securerandom'
require 'time'
require 'parametric/struct'

Parametric.policy :uuid do
  UUID_EXP = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.freeze

  message do
    "it must be a valid UUID"
  end

  validate do |value, key, payload|
    !!(value.to_s =~ UUID_EXP)
  end

  meta_data do
    {type: :string}
  end
end

module Sourced
  EventError = Class.new(StandardError)
  UnknownEventError = Class.new(EventError)

  class InvalidEventError < EventError
    attr_reader :errors
    def initialize(topic, errors)
      @errors = errors
      msg = errors.map {|k, strings|
        "#{k} #{strings.join(', ')}"
      }.join('. ')
      super "Not a valid event '#{topic}': #{msg}"
    end
  end

  class Event
    include Parametric::Struct

    schema do
      field(:topic).type(:string).present
      field(:id).type(:uuid).default(->(*_){ SecureRandom.uuid })
      field(:aggregate_id).declared.type(:uuid)
      field(:date).type(:datetime).default(->(*_){ Time.now.utc })
    end

    def self.registry
      @registry ||= {}
    end

    def self.define(event_name, &block)
      klass = Class.new(self)
      # redefine topic with default value
      klass.schema do
        field(:topic).default(event_name).options([event_name])
      end
      # apply new schema
      klass.schema &block

      registry[event_name] = klass
    end

    def self.topic
      schema.fields[:topic].visit(:default)
    end

    def self.instance(data = {})
      event = new(data)
      raise InvalidEventError.new(event.topic, event.errors) unless event.valid?
      event
    end
  end
end
