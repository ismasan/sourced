# frozen_string_literal: true

module Sourced
  # And event decorator
  # that copies and adds event metadata
  # to all events applied by a stage
  # Usage:
  #
  #   stage = SomeStage.load(id, stream).with_metadata(year: 2022)
  #   stage.apply(SomeEvent, payload: { foo: 'bar' })
  #   stage.events.first.metadata # includes { foo: 'bar' }
  #
  class MetadataEventDecorator
    def initialize(metadata)
      @metadata = metadata
    end

    def call(attrs)
      meta = attrs[:metadata] || {}
      attrs.merge(metadata: meta.merge(@metadata))
    end
  end
end
