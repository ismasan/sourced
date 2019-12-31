# frozen_string_literal: true

module Sourced
  class Projector
    include Sourced::Eventable

    def self.call(evt, entity)
      new.call(evt, entity)
    end

    def call(evt, entity)
      apply(evt, deps: [entity])
    end
  end
end

