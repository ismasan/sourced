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
      field(:id).type(:uuid).default(->(*_){ ::Sourced.uuid })
      field(:aggregate_id).declared.type(:uuid)
      field(:version).type(:integer).default(1)
      field(:date).type(:datetime).default(->(*_){ Time.now.utc })
    end

    def self.registry
      @registry ||= {}
    end

    def self.define(topic, &block)
      klass = Class.new(self)
      # redefine topic with default value
      klass.schema do
        field(:topic).default(topic).options([topic])
      end
      # apply new schema
      klass.schema &block

      registry[topic] = klass
    end

    def self.topic
      schema.fields[:topic].visit(:default)
    end

    def self.instance(data = {})
      event = new(data)
      raise InvalidEventError.new(event.topic, event.errors) unless event.valid?
      event
    end

    def inspect
      %(<#{self.class.name} #{inspect_line}>)
    end

    private
    def inspect_line
      to_h.map { |k, v|
        [k, v].join('=')
      }.join(' ')
    end
  end
end
