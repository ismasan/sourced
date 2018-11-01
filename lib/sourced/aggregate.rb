module Sourced
  module Aggregate
    def self.included(base)
      base.send :include, Eventable
    end
    include Eventable

    def version
      @version ||= 0
    end

    def load_from(stream)
      stream.each do |evt|
        apply evt, collect: false
      end
      self
    end

    private
    def before_apply(event)
      @version = event.version
    end
  end
end
