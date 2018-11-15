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
  class Event
    include Parametric::Struct

    schema do
      field(:topic).type(:string).present
      field(:id).type(:uuid).default(->(*_){ ::Sourced.uuid })
      field(:aggregate_id).present.type(:uuid)
      field(:parent_id).declared.type(:uuid)
      field(:version).type(:integer).default(1)
      field(:date).type(:datetime).default(->(*_){ Time.now.utc })
    end

    def self.registry
      @registry ||= {}
    end

    def self.define(topic, schema = nil, &block)
      klass = Class.new(self)
      # redefine topic with default value
      klass.schema do
        field(:topic).default(topic).options([topic])
      end
      # apply new schema
      klass.schema = klass.schema.merge(schema.schema) if schema
      # we need to call .schema(&block) to define struct methods
      if block_given?
        klass.schema &block
      else
        klass.schema(&Proc.new{})
      end

      registry[topic] = klass
    end

    def self.topic
      schema.fields[:topic].visit(:default)
    end

    def self.new!(data = {})
      event = new(data)
      raise InvalidEventError.new(event.topic, event.errors) unless event.valid?
      event
    end

    def self.resolve(topic)
      klass = registry[topic]
      raise UnknownEventError, "no event schema registered for '#{topic}'" unless klass
      klass
    end

    def self.topic
      schema.fields[:topic].visit(:default)
    end

    def self.from(data = {})
      data[:topic] = topic unless data[:topic]
      klass = resolve(data[:topic])
      klass.new! data
    end

    def copy(new_attrs = {})
      data = to_h.merge(new_attrs)
      self.class.new!(data)
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
