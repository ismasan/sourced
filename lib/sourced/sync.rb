# frozen_string_literal: true

module Sourced
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

    CallableInterface = Sourced::Types::Interface[:call]

    class SyncReactor < SimpleDelegator
      def handle_events(events)
        Router.handle_and_ack_events_for_reactor(__getobj__, events)
      end

      def call(_state, _command, events)
        handle_events(events)
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

      def sync(callable = nil, &block)
        callable ||= block
        callable = case callable
                   when Proc
                     unless (2..3).include?(callable.arity)
                       raise ArgumentError,
                             'sync block must accept 2 or 3 arguments'
                     end

                     callable
                   when ReactorInterface
                     # Wrap reactors here
                     # TODO:
                     # If the sync reactor runs successfully
                     # A). we want to ACK processed events for it in the offsets table
                     # so that if the reactor is moved to async execution
                     # it doesn't reprocess the same events again
                     # B). The reactors .handle_events may return commands
                     # Do we want to dispatch those commands inline?
                     # Or is this another reason to have a separate async command bus
                     SyncReactor.new(callable)
                   when CallableInterface
                     callable
                   else
                     raise ArgumentError, 'sync block must be a Proc, Sourced::ReactorInterface or #call interface'
                   end

        sync_blocks << callable
      end
    end
  end
end
