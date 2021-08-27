# frozen_string_literal: true

module Sourced
  class Projector
    include Sourced::Eventable

    def self.call(entity, event)
      new.call(entity, event)
    end

    def call(entity, event)
      apply(event, deps: [entity])
      entity
    end
  end
end

