module Sourced
  class Dispatcher
    def initialize(repository: nil, event_store:, handler:, subscribers: Subscribers.new)
      @repository = repository || AggregateRepo.new(event_store: event_store)
      @event_store = event_store
      @handler = handler
      @subscribers = subscribers
    end

    def call(cmd)
      aggr, events = handler.call(cmd, repository: repository)
      subscribers.call(event_store.append(events))
      aggr
    end

    private
    attr_reader :repository, :event_store, :handler, :subscribers
  end
end
