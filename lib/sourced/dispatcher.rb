module Sourced
  class NullHandler
    def self.call(*_args)
      [nil, []]
    end

    def self.aggregate_class
      nil
    end

    def self.topics
      []
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
      hndl = prepare_handler(handler || _handler)
      validate_handler!(hndl, cmd)
      aggr = repository.load(cmd.aggregate_id, hndl.aggregate_class)
      aggr = hndl.call(cmd, aggr)
      events = collect_and_decorate_events(aggr.clear_events, cmd)
      subscribers.call repository.persist_events(events)
      aggr
    end

    private
    attr_reader :repository, :event_store, :_handler, :subscribers

    def prepare_handler(handler)
      if handler.respond_to?(:call)
        handler
      elsif handler.respond_to?(:new)
        handler.new
      else
        handler
      end
    end

    def validate_handler!(handler, cmd)
      if !handler.topics.include?(cmd.topic)
        raise UnhandledCommandError, "#{handler} does not handle command '#{cmd.topic}'"
      end
      if !handler.aggregate_class
        raise ArgumentError, "#{handler}#aggregate_class must return an Aggregate class"
      end
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
