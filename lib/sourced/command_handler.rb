module Sourced
  class CommandHandler
    include Eventable

    class << self
      def call(cmd, event_store: MemEventStore.new)
        new(event_store).call(cmd)
      end

      def aggregates(klass)
        @aggregate_class = klass
      end

      attr_reader :aggregate_class
    end

    def initialize(event_store)
      @repository = AggregateRepo.new(event_store: event_store)
    end

    def call(cmd)
      clear_events
      aggr = load_aggregate(cmd.aggregate_id)
      apply(cmd, deps: [aggr].compact, collect: false)
      [aggr, collect_and_clear_events]
    end

    private
    attr_reader :repository

    # collect events
    # 1. added directly to command handler, if any
    # 2. applied to aggregates managed by repository
    def collect_and_clear_events
      clear_events + repository.clear_events
    end

    def load_aggregate(id)
      return nil unless self.class.aggregate_class
      repository.load(id, self.class.aggregate_class)
    end
  end
end
