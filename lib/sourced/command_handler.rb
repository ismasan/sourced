module Sourced
  class CommandHandler
    include Eventable

    class << self
      # by default it will use a repository with a memory-only event store
      # pass your own pre-instantiated repository
      # to share repository state across handlers
      def call(cmd, repository: AggregateRepo.new)
        if !topics.include?(cmd.topic)
          raise UnhandledCommandError, "#{self.name} does not handle command '#{cmd.topic}'"
        end
        repository.clear_events
        new(repository).call(cmd)
      end

      def aggregates(klass)
        @aggregate_class = klass
      end

      attr_reader :aggregate_class
    end

    def initialize(repository)
      @repository = repository
    end

    def call(cmd)
      clear_events
      aggr = load_aggregate(cmd.aggregate_id)
      apply(cmd, deps: [aggr].compact, collect: false)
      [aggr, collect_and_clear_events(cmd)]
    end

    private
    attr_reader :repository

    # collect events, add #parent_id from command
    # 1. added directly to command handler, if any
    # 2. applied to aggregates managed by repository
    def collect_and_clear_events(cmd)
      (clear_events + repository.clear_events).map do |evt|
        evt.copy(copy_cmd_attrs(cmd))
      end
    end

    # these attributes will be copied from originating command
    # on to new events emitted by this handler
    # overwrite this in your sub-classes if you need to copy aditional attributes
    def copy_cmd_attrs(cmd)
      {parent_id: cmd.id}
    end

    def load_aggregate(id)
      return nil unless self.class.aggregate_class
      repository.load(id, self.class.aggregate_class)
    end
  end
end
