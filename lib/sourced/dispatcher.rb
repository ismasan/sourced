module Sourced
  class NullHandler
    def self.call(*_args)
      [nil, []]
    end
  end

  class Dispatcher
    def initialize(repository: nil, event_store:, handler: NullHandler, subscribers: Subscribers.new)
      @repository = repository || AggregateRepo.new(event_store: event_store)
      @event_store = event_store
      @_handler = handler
      @subscribers = subscribers
    end

    def call(cmd, handler: nil)
      hndl = handler || _handler
      validate_handler!(hndl, cmd)
      aggr = load_aggregate(cmd.aggregate_id, hndl.aggregate_class)
      aggr, events = hndl.call(cmd, aggr)
      events = collect_and_decorate_events(events, cmd)
      subscribers.call(event_store.append(events))
      aggr
    end

    private
    attr_reader :repository, :event_store, :_handler, :subscribers

    def validate_handler!(handler, cmd)
      if !handler.topics.include?(cmd.topic)
        raise UnhandledCommandError, "#{handler} does not handle command '#{cmd.topic}'"
      end
    end

    def load_aggregate(id, aggregate_class)
      return nil unless aggregate_class
      repository.load(id, aggregate_class)
    end

    def collect_and_decorate_events(events, cmd)
      out = events.map do |evt|
        evt.copy(copy_cmd_attrs(cmd))
      end

      [cmd] + out
    end

    def copy_cmd_attrs(cmd)
      { parent_id: cmd.id }
    end
  end
end
