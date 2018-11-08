module Sourced
  class Subscribers
    def initialize
      @subs = Set.new
    end

    def subscribe(sub)
      @subs.add sub
    end

    def call(events)
      events = Array(events)
      events.each do |evt|
        @subs.each { |sub| sub.apply(evt, collect: false) }
      end
    end
  end
end
