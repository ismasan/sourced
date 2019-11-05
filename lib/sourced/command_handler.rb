module Sourced
  module CommandHandler
    def self.included(base)
      base.send :include, Eventable
      base.extend ClassMethods
    end

    module ClassMethods
      def aggregates(klass)
        @aggregate_class = klass
      end

      attr_reader :aggregate_class
    end

    def call(cmd, aggr)
      apply(cmd, deps: [aggr].compact, collect: false)
      [aggr, aggr.clear_events]
    end

    def aggregate_class
      self.class.aggregate_class
    end
  end
end
