# frozen_string_literal: true

module Sourced
  module CCC
    # Sync mixin for CCC reactors.
    # Registers blocks that run within the store transaction.
    module Sync
      def self.included(base)
        super
        base.extend ClassMethods
      end

      # Build Actions::Sync wrappers for all registered sync blocks.
      def sync_actions(**args)
        self.class.sync_blocks.map do |block|
          Actions::Sync.new(proc { instance_exec(**args, &block) })
        end
      end

      module ClassMethods
        def inherited(subclass)
          super
          sync_blocks.each do |blk|
            subclass.sync_blocks << blk
          end
        end

        def sync_blocks
          @sync_blocks ||= []
        end

        def sync(&block)
          sync_blocks << block
        end
      end
    end
  end
end
