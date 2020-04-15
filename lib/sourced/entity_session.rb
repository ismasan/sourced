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
      _entity = entity.call(id)
      seq = 0
      stream.each do |event|
        seq = event.seq
        projector.call(_entity, event)
      end

      new(id, entity: _entity, projector: projector, seq: seq)
    end

    attr_reader :id, :entity, :events, :seq, :last_committed_seq

    def initialize(id, entity:, projector:, seq: 0)
      @id = id
      @entity = entity
      @projector = projector
      @seq = seq
      @last_committed_seq = seq
      @events = []
    end

    def ==(other)
      other.id == id && other.seq == seq
    end

    def inspect
      %(<#{self.class.name}##{id} #{events.size} uncommitted events #{entity} >)
    end

    def apply(event_or_class, attrs = {})
      attrs = next_event_attrs.merge(attrs)
      event = if event_or_class.respond_to?(:new!)
        event_or_class.new!(attrs)
      else
        event_or_class.copy(attrs)
      end
      projector.call(entity, event)
      @seq = event.seq
      events << event
      self
    end

    def commit(&_block)
      # Yield last known committed sequence and new events
      yield @last_committed_seq, @events.slice(0, @events.size), entity

      # Only update @seq and clear events if yield above hasn't raised
      @last_committed_seq = @seq
      @events = []
    end

    private

    attr_reader :projector

    def next_event_attrs
      {
        entity_id: id,
        seq: seq + 1,
      }
    end
  end
end
