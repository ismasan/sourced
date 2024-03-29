# frozen_string_literal: true

require 'sourced/projector'
require 'sourced/metadata_event_decorator'

module Sourced
  class Stage
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
        _entity = projector.call(_entity, event)
      end

      new(id, entity: _entity, projector: projector, seq: seq)
    end

    # An event decorator that tracks and adds next seq number and stream_id to
    # new events
    class SeqTracker
      def initialize(id, seq)
        @id, @seq = id, seq
      end

      def set(new_seq)
        @seq = new_seq
      end

      # The EventAttributeDecorator interface
      def call(attrs)
        attrs.merge(
          stream_id: @id,
          seq: @seq + 1,
        )
      end
    end

    attr_reader :id, :entity, :events, :seq, :last_committed_seq

    def initialize(id, entity:, projector:, seq: 0, last_committed_seq: nil, events: [], event_decorators: [])
      @id = id
      @entity = entity
      @projector = projector
      @seq = seq
      @last_committed_seq = last_committed_seq || seq
      @events = events
      @event_decorators = event_decorators
      if (seq_tracker = @event_decorators.find { |d| d.is_a?(SeqTracker) })
        @seq_tracker = seq_tracker
      else
        @seq_tracker = SeqTracker.new(id, seq)
        @event_decorators << @seq_tracker
      end
    end

    def with_event_decorator(decorator = nil, &block)
      decorator ||= block

      self.class.new(
        id,
        entity: entity,
        projector: projector,
        seq: seq,
        last_committed_seq: last_committed_seq,
        events: events,
        event_decorators: event_decorators + [decorator]
      )
    end

    def with_metadata(metadata = {})
      with_event_decorator(MetadataEventDecorator.new(metadata))
    end

    def following(causation_event)
      with_metadata(causation_id: causation_event.id, correlation_id: causation_event.correlation_id)
    end

    def ==(other)
      other.id == id && other.seq == seq
    end

    def inspect
      %(<#{self.class.name}##{id} #{events.size} uncommitted events #{entity} >)
    end

    def apply(event_or_class, payload = {})
      event = if event_or_class.respond_to?(:copy)
        event_or_class.copy(decorate_event_attrs(event_or_class.to_h))
      else
        event_or_class.new(decorate_event_attrs(payload: payload))
      end
      @entity = projector.call(entity, event)
      @seq = seq_tracker.set(event.seq)
      events << event
      self
    end

    def commit(&_block)
      # Yield last known committed sequence and new events
      output_events = @events.slice(0, @events.size)
      yield @last_committed_seq, output_events, entity

      # Only update @seq and clear events if yield above hasn't raised
      @last_committed_seq = @seq
      @events = []
      output_events
    end

    private

    attr_reader :projector, :seq_tracker, :event_decorators

    def decorate_event_attrs(attrs)
      event_decorators.reduce(attrs) { |hash, dec| dec.call(hash) }
    end
  end
end
