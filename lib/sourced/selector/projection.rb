# frozen_string_literal: true

module Sourced
  class Selector
    # A selector as a projector: what a reactor reading through a selector
    # (see {Evolve}) evolves. A subclass per selector ({.for}); a projection
    # (an instance) keeps the messages of the selector's types it evolves, in
    # order, and they're its context. It takes the history as read, so it
    # filters by type only: the store read is the partition's, and what a
    # reactor appends is checked against the partition when it's emitted.
    #
    # It answers what a reactor asks of its projector: {.selector}, +new+,
    # {#evolve} and {#context}. It takes no inline declarations.
    class Projection
      class << self
        # @return [Selector]
        attr_reader :selector

        # @param selector [Selector]
        # @return [Class<Projection>]
        def for(selector) = Class.new(self) { @selector = selector }

        def inherited(subclass)
          super
          subclass.instance_variable_set(:@selector, selector)
        end

        # @return [self]
        def to_projector = self

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

      # @param messages [Sourced::Message, Array<Sourced::Message>]
      # @return [Array<Sourced::Message>] the context
      def evolve(messages)
        types = self.class.selector.types
        Array(messages).each { |msg| @messages << msg if types.include?(msg.to_message.class) }
        context
      end

      # @return [Array<Sourced::Message>] the messages evolved so far
      def context = @messages
    end
  end
end
