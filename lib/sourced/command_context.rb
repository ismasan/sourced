# frozen_string_literal: true

require 'sourced/types'

module Sourced
  # A command factory to instantiate commands from Hash attributes
  # including extra metadata.
  # @example
  #
  #  ctx = Sourced::CommandContext.new(
  #    stream_id: params[:stream_id],
  #    metadata: {
  #      user_id: session[:user_id]
  #    }
  #  )
  #
  # # params[:command] should be a Hash with { type: String, payload: Hash | nil }
  #
  #  cmd = ctx.build(params[:command])
  #  cmd.stream_id # String
  #  cmd.metadata[:user_id] # == session[:user_id]
  #
  class CommandContext
    # @option stream_id [String]
    # @option metadata [Hash] metadata to add to commands built by this context
    # @option scope [Sourced::Message] Message class to use as command registry
    def initialize(stream_id: nil, metadata: Plumb::BLANK_HASH, scope: Sourced::Command)
      @defaults = {
        stream_id:,
        metadata:
      }.freeze
      @scope = scope
    end

    # @param attrs [Hash] attributes to lookup and buils a scope from.
    # @return [Sourced::Message]
    def build(attrs)
      attrs = defaults.merge(Types::SymbolizedHash.parse(attrs))
      scope.from(attrs)
    end

    private

    attr_reader :defaults, :scope
  end
end
