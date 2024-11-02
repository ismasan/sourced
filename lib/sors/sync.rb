# frozen_string_literal: true

module Sors
  module Sync
    def self.included(base)
      super
      base.extend ClassMethods
    end

    def run_sync_blocks(state, command, events)
      self.class.sync_blocks.each do |blk|
        case blk
        when Proc
          if blk.arity == 2 # (command, events)
            instance_exec(command, events, &blk)
          else # (state, command, events)
            instance_exec(state, command, events, &blk)
          end
        else
          blk.call(state, command, events)
        end
      end
    end

    CallableInterface = Sors::Types::Interface[:call]

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

      def sync(callable = nil, &block)
        callable ||= block
        callable = case callable
        when Proc
          raise ArgumentError, 'sync block must accept 2 or 3 arguments' unless (2..3).include?(callable.arity)
          callable
        when ReactorInterface
          # Wrap reactors here
          callable
        when CallableInterface
          callable
        else
          raise ArgumentError, 'sync block must be a Proc, Sors::ReactorInterface or #call interface'
        end

        sync_blocks << callable
      end
    end
  end
end
