module Sourced
  class CommandHandler
    include Eventable

    class << self
      def call(cmd, aggr)
        new.call(cmd, aggr)
      end

      def aggregates(klass)
        @aggregate_class = klass
      end

      attr_reader :aggregate_class
    end

    def call(cmd, aggr)
      apply(cmd, deps: [aggr].compact, collect: false)
      [aggr, aggr.clear_events]
    end
  end
end
