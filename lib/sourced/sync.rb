# frozen_string_literal: true

module Sourced
  # This mixin provides a .sync macro to registering blocks
  # that will run within a transaction. Ie. in Deciders when appending events to the backend,
  # or projectors when persisting their state.
  # @example
  #
  #  class CartDecider < Sourced::Decider
  #    # Run this block within a transaction
  #    # when appending messages to storage
  #    sync do |state, command, events|
  #      # Do something here, like updating a view, sending an email, etc.
  #    end
  #  end
  #
  # When given a ReactorInterface, it will run that reactor synchronously
  # and ACK the offsets for the embedded reactor and consumed events.
  # This is so that reactors can be run in a strong consistency manner
  # within a Decider lifecycle.
  # ACKing the events will ensure that the events are not reprocessed
  # if the child reactor is later moved to eventually consistent processing.
  # @example
  #
  #  class CartDecider < Sourced::Decider
  #    # The CartListings projector
  #    # will be run synchronously when events are appended by this Decider
  #    # Any error raised by the projector will cause the transaction to rollback
  #    sync CartListings
  #  end
  module Sync
    def self.included(base)
      super
      base.extend ClassMethods
    end

    # Host classes will call this method to run any registered .sync blocks
    # within their transactional boundaries.
    # @param state [Object] the current state of the host class
    # @param command [Object, Nil] the command being processed
    # @param events [Array<Sourced::Event>] the events being appended
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

    # Wrap a sync reactor and call it's handle_events method
    # while also ACKing its offsets for the processed events.
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

      # The .sync macro
      # @example
      #
      #  sync do |state, command, events|
      #    # Do something here, like updating a view, sending an email, etc.
      #  end
      #
      #  sync CartListings
      #
      # @param callable [Nil, Proc, ReactorInterface, CallableInterface] the block to run
      # @yieldparam state [Object] the state of the host class
      # @yieldparam command [Object, Nil] the command being processed
      # @yieldparam events [Array<Sourced::Event>] the events being appended
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
