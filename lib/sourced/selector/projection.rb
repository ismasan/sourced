# frozen_string_literal: true

module Sourced
  class Selector
    # A selector as a projector (see {Evolve}), one subclass per selector
    # ({.for}). Its context is the evolved messages of the selector's types, in
    # order, filtered by type only: the store read is already the partition's.
    # It takes no inline declarations.
    class Projection
      class << self
        # @return [Selector]
        attr_reader :selector

        # @return [Class<Projection>]
        def for(selector) = Class.new(self) { @selector = selector }

        # @return [Set<Class>] the selector's types
        def types = @types ||= selector.types.to_set

        def inherited(subclass)
          super
          subclass.instance_variable_set(:@selector, selector)
        end

        # @return [self]
        def to_projector = self

        # {Sourced.load}'s protocol, as {Projector::Inline}'s: every value is the partition.
        def partition_of(values) = values
        def history_for(partition) = selector.by(**partition)
        def for_partition(_values) = new

        %i[context evolve before_evolve].each do |dsl|
          define_method(dsl) do |*|
            raise ArgumentError, "#{self}.#{dsl}: a selector projection takes no context/evolve/before_evolve " \
                                 'declarations (its context is the messages it selects)'
          end
        end
      end

      # @param messages [Array<Sourced::Message>] a starting context
      def initialize(messages = [])
        @messages = messages.dup
      end

      # @return [Array<Sourced::Message>] the context
      def evolve(messages)
        types = self.class.types
        Array(messages).each { |msg| @messages << msg if types.include?(msg.to_message.class) }
        context
      end

      # @return [Array<Sourced::Message>] the messages evolved so far
      def context = @messages
    end
  end
end
