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
      def call(_state, _command, events)
        Sourced.config.backend.ack_on(consumer_info.group_id, events.last.id) do
          commands =  __getobj__.handle_events(events)
          if commands && commands.any?
            # TODO: Commands may or may not belong to he same stream as events
            # if they belong to the same stream,
            # hey need to be dispached in order to preserve per stream order
            # If they belong to different streams, they can be dispatched in parallel
            # or put in a command bus.
            # TODO2: we also need to handle exceptions here
            # TODO3: this is not tested
            commands.each do |cmd|
              Sourced::Router.handle_command(cmd)
            end
          end
        end
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
          raise ArgumentError, 'sync block must accept 2 or 3 arguments' unless (2..3).include?(callable.arity)
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
