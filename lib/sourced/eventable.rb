# frozen_string_literal: true

module Sourced
  module Eventable
    def self.included(base)
      base.extend ClassMethods
    end

    def apply(event_or_class, attrs = {})
      attrs = attrs.dup
      deps = attrs.delete(:deps) || []

      event = if event_or_class.respond_to?(:new)
        event_or_class.new(next_event_attrs.merge(attrs))
      else
        event_or_class
      end

      handlers = self.class.handlers[event.topic]
      return unless handlers.any?

      self.class.handlers_for_any.each do |record|
        instance_exec(*deps, event, &record.handler)
      end

      handlers.each do |record|
        before_apply(event)
        instance_exec(*deps, event, &record.handler)
      end
    end

    def topics
      self.class.topics
    end

    private
    def next_event_attrs
      {}
    end

    def before_apply(_)

    end

    Record = Struct.new(:handler, :options)
    NOOP = ->(*) {}

    module ClassMethods
      def inherited(subclass)
        handlers.each do |key, list|
          subclass.handlers[key] = list
        end
      end

      def on(event_type, opts = {},  &block)
        block = NOOP unless block_given?

        if event_type == :any
          return handlers_for_any << Record.new(block, opts)
        end

        key = event_type.respond_to?(:topic) ? event_type.topic : event_type.to_s
        handlers[key] << Record.new(block, opts)
      end

      def handlers
        @handlers ||= Hash.new{|h, k| h[k] = [] }
      end

      def handlers_for_any
        @handlers_for_any ||= []
      end

      def topics
        handlers.keys
      end
    end
  end
end
