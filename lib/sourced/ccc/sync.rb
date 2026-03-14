# frozen_string_literal: true

module Sourced
  module CCC
    # Sync mixin for CCC reactors.
    # Registers blocks that run within the store transaction (+sync+)
    # or after the transaction commits (+after_sync+).
    module Sync
      def self.included(base)
        super
        base.extend ClassMethods
      end

      # Build {Actions::Sync} wrappers for all registered +sync+ blocks.
      #
      # @param args [Hash] keyword arguments forwarded to each block
      # @return [Array<Actions::Sync>]
      def sync_actions(**args)
        self.class.sync_blocks.map do |block|
          Actions::Sync.new(proc { instance_exec(**args, &block) })
        end
      end

      # Build {Actions::AfterSync} wrappers for all registered +after_sync+ blocks.
      #
      # @param args [Hash] keyword arguments forwarded to each block
      # @return [Array<Actions::AfterSync>]
      def after_sync_actions(**args)
        self.class.after_sync_blocks.map do |block|
          Actions::AfterSync.new(proc { instance_exec(**args, &block) })
        end
      end

      # Build all sync and after_sync actions together.
      #
      # @param args [Hash] keyword arguments forwarded to each block
      # @return [Array<Actions::Sync, Actions::AfterSync>]
      def collect_actions(**args)
        sync_actions(**args) + after_sync_actions(**args)
      end

      module ClassMethods
        # @api private
        def inherited(subclass)
          super
          sync_blocks.each do |blk|
            subclass.sync_blocks << blk
          end
          after_sync_blocks.each do |blk|
            subclass.after_sync_blocks << blk
          end
        end

        # @return [Array<Proc>] registered sync blocks
        def sync_blocks
          @sync_blocks ||= []
        end

        # Register a block to run inside the store transaction.
        #
        # The block receives the same keyword arguments as the reactor's
        # action-building step (e.g. +state:+, +messages:+, +events:+
        # for deciders, or +state:+, +messages:+, +replaying:+ for projectors).
        #
        # @yield [**args] side-effect block executed within the transaction
        # @return [void]
        def sync(&block)
          sync_blocks << block
        end

        # @return [Array<Proc>] registered after_sync blocks
        def after_sync_blocks
          @after_sync_blocks ||= []
        end

        # Register a block to run after the store transaction commits.
        #
        # Use this for side effects that should only happen on successful
        # commit (e.g. sending emails, HTTP calls, pushing to external queues).
        #
        # The block receives the same keyword arguments as +sync+.
        #
        # @yield [**args] side-effect block executed after transaction commit
        # @return [void]
        def after_sync(&block)
          after_sync_blocks << block
        end
      end
    end
  end
end
