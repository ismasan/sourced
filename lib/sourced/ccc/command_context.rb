# frozen_string_literal: true

require 'sourced/types'

module Sourced
  module CCC
    class CommandContext
      # @option metadata [Hash] metadata to add to commands built by this context
      # @option scope [CCC::Message] Message class to use as command registry
      def initialize(metadata: Plumb::BLANK_HASH, scope: CCC::Command)
        @defaults = { metadata: }.freeze
        @scope = scope
      end

      # @param args [Array] either [Hash] or [Class, Hash]
      # @return [CCC::Message]
      def build(*args)
        case args
        in [Class => klass, Hash => attrs]
          attrs = defaults.merge(Types::SymbolizedHash.parse(attrs))
          klass.parse(attrs)
        in [Hash => attrs]
          attrs = defaults.merge(Types::SymbolizedHash.parse(attrs))
          scope.from(attrs)
        else
          raise ArgumentError, "Invalid arguments: #{args.inspect}"
        end
      end

      private

      attr_reader :defaults, :scope
    end
  end
end
