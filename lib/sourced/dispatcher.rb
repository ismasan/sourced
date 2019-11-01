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
      aggr, events = hndl.call(cmd, repository: repository)
      subscribers.call(event_store.append(events))
      aggr
    end

    private
    attr_reader :repository, :event_store, :_handler, :subscribers
  end
end
