# frozen_string_literal: true

require 'sourced/projector'

module Sourced
  class EntitySession
    def self.projector(pr = nil, &block)
      if pr
        @projector = pr
      elsif block_given?
        @projector = Class.new(Projector, &block)
      end

      @projector
    end

    def self.entity(entity = nil, &block)
      if entity
        @entity = entity.respond_to?(:call) ? entity : ->(_id) { entity }
      elsif block_given?
        @entity = block
      end

      @entity
    end

    def self.load(id, stream)
      entity = build(id)
      seq = 0
      stream.each do |evt|
        seq = evt.seq
        projector.call(evt, entity)
      end

      new(id, entity: entity, projector: projector, seq: seq)
    end

    def self.build(id)
      entity.call(id)
    end

    attr_reader :id, :entity, :events, :seq

    def initialize(id, entity:, projector:, seq: 0)
      @id = id
      @entity = entity
      @projector = projector
      @seq = seq
      @events = []
    end

    def ==(other)
      other.id == id && other.seq == seq
    end

    def inspect
      %(<#{self.class.name}##{id} #{events.size} uncommitted events #{entity} >)
    end

    def apply(event_or_class, attrs = {})
      attrs = attrs.dup
      event = if event_or_class.respond_to?(:new!)
        event_or_class.new!(next_event_attrs.merge(attrs))
      else
        event_or_class
      end
      projector.call(event, entity)
      @seq = event.seq
      events << event
      self
    end

    def clear_events
      @events.slice!(0, @events.size)
    end

    private

    attr_reader :projector

    def next_event_attrs
      {
        aggregate_id: id,
        seq: seq + 1,
      }
    end
  end
end
