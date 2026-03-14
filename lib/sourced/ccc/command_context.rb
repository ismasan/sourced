# frozen_string_literal: true

require 'sourced/types'

module Sourced
  module CCC
    # Builds CCC command instances with shared default metadata.
    #
    # @example Build a command from a type string
    #   ctx = CommandContext.new(metadata: { user_id: 10 })
    #   cmd = ctx.build(type: 'orders.place', payload: { item: 'hat' })
    #   cmd.metadata[:user_id] # => 10
    #
    # @example Scope to a custom command subclass
    #   scope = Class.new(CCC::Command)
    #   MyCmd = scope.define('my.cmd') { attribute :name, String }
    #   ctx = CommandContext.new(scope: scope)
    #   cmd = ctx.build(type: 'my.cmd', payload: { name: 'hello' })
    class CommandContext
      class << self
        # Register a block to run when building specific command type(s).
        # The block receives the app scope and the command, and must return the (possibly modified) command.
        #
        # @param message_classes [Class] one or more command classes to match
        # @yield [app, cmd] transformation block
        # @return [void]
        def on(*message_classes, &block)
          message_classes.each { |klass| message_blocks[klass] = block }
        end

        # Register a block to run for all commands.
        # The block receives the app scope and the command, and must return the (possibly modified) command.
        #
        # @yield [app, cmd] transformation block
        # @return [void]
        def any(&block)
          any_blocks << block
        end

        # @api private
        def message_blocks
          @message_blocks ||= {}
        end

        # @api private
        def any_blocks
          @any_blocks ||= []
        end

        # @api private
        def inherited(subclass)
          super
          message_blocks.each { |k, v| subclass.message_blocks[k] = v }
          any_blocks.each { |blk| subclass.any_blocks << blk }
        end
      end

      # @param metadata [Hash] default metadata merged into every command built by this context
      # @param scope [Class] message class whose registry is used to resolve type strings (default: {CCC::Command})
      # @param app [Object, nil] request-scoped object passed to callback blocks
      #
      # @example
      #   ctx = CommandContext.new(metadata: { user_id: 42 }, app: rack_app)
      def initialize(metadata: Plumb::BLANK_HASH, scope: CCC::Command, app: nil)
        @defaults = { metadata: }.freeze
        @scope = scope
        @app = app
      end

      # Build a command instance, merging in default metadata.
      #
      # @overload build(attrs)
      #   Resolve the command class from the +type+ key in +attrs+ via the scope's registry.
      #   @param attrs [Hash] must include +:type+ and +:payload+ keys
      #   @return [CCC::Message]
      #   @example
      #     ctx.build(type: 'orders.place', payload: { item: 'hat' })
      #
      # @overload build(klass, attrs)
      #   Use the given command class directly.
      #   @param klass [Class] a CCC::Command subclass
      #   @param attrs [Hash] must include +:payload+ key
      #   @return [CCC::Message]
      #   @example
      #     ctx.build(PlaceOrder, payload: { item: 'hat' })
      #
      # @raise [ArgumentError] if arguments don't match either form
      # @raise [Sourced::UnknownMessageError] if the type string is not registered in the scope
      def build(*args)
        cmd = case args
              in [Class => klass, Hash => attrs]
                attrs = defaults.merge(Types::SymbolizedHash.parse(attrs))
                klass.parse(attrs)
              in [Hash => attrs]
                attrs = defaults.merge(Types::SymbolizedHash.parse(attrs))
                scope.from(attrs)
              else
                raise ArgumentError, "Invalid arguments: #{args.inspect}"
              end
        run_pipeline(cmd)
      end

      private

      attr_reader :defaults, :scope, :app

      def run_pipeline(cmd)
        block = self.class.message_blocks[cmd.class]
        cmd = block.call(app, cmd) if block
        self.class.any_blocks.each { |blk| cmd = blk.call(app, cmd) }
        cmd
      end
    end
  end
end
