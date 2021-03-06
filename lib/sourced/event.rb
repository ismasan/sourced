# frozen_string_literal: true

require 'time'
require 'delegate'
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

# Time equality compares fractional seconds,
# but Time#to_s (used by JSON.dump) doesn't preserve fractions,
# so reconstituing an event with JSON.parse produces different times
class ComparableTime < SimpleDelegator
  def self.utc
    new(Time.now.utc)
  end

  def self.wrap(obj)
    case obj
    when String
      new(Time.parse(obj))
    when Time, DateTime
      new(obj.to_time)
    when ComparableTime
      obj
    else
      raise ArgumentError, "#{obj.class} #{obj} cannot be coerce to Time"
    end
  end

  def ==(time)
    time.to_i == time.to_time.to_i
  end
end

Parametric.policy :time do
  coerce do |v, k, c|
    ComparableTime.wrap(v)
  end

  meta_data do
    {type: :time}
  end
end

module Sourced
  class Event
    include Parametric::Struct

    schema do
      field(:topic).type(:string).present
      field(:id).type(:uuid).default(->(*_){ ::Sourced.uuid })
      field(:entity_id).present.type(:string)
      field(:originator_id).declared.type(:uuid)
      field(:seq).type(:integer).default(1)
      field(:date).type(:time).default(->(*_){ ComparableTime.utc })
      # field(:payload).type(:object).default({})
    end

    def self.registry
      @registry ||= {}
    end

    def self.define(topic, schema = nil, &block)
      klass = Class.new(self)
      if schema.nil? && block_given?
        schema = Parametric::Schema.new(&block)
      end
      # redefine topic with default value
      klass.schema do
        field(:topic).default(topic).options([topic])
        field(:payload).type(:object).present.schema(schema) if schema
      end
      # apply new schema
      # klass.schema = klass.schema.merge(schema.schema) if schema
      # we need to call .schema(&block) to define struct methods
      # klass.schema(&Proc.new{})
      # if block_given?
      #   klass.schema &block
      # else
      #   klass.schema(&Proc.new{})
      # end

      ::Sourced::Event.registry[topic] = klass
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
      klass = ::Sourced::Event.registry[topic]
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
      %(<Event #{inspect_line}>)
    end

    def ==(other)
      other.respond_to?(:to_h) && other.to_h == to_h
    end

    private

    def inspect_line
      to_h.map { |k, v|
        [k, v].join('=')
      }.join(' ')
    end
  end
end
