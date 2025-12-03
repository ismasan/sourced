# frozen_string_literal: true

module Sourced
  # This mixin provides a .sync macro to registering blocks
  # that will run within a transaction. Ie. in Actors when appending events to the backend,
  # or projectors when persisting their state.
  # @example
  #
  #  class CartActor < Sourced::Actor
  #    # Run this block within a transaction
  #    # when appending messages to storage
  #    sync do |state:, command:, events:|
  #      # Do something here, like updating a view, sending an email, etc.
  #    end
  #  end
  #
  # When given a ReactorInterface, it will run that reactor synchronously
  # and ACK the offsets for the embedded reactor and consumed events.
  # This is so that reactors can be run in a strong consistency manner
  # within an Actor lifecycle.
  # ACKing the events will ensure that the events are not reprocessed
  # if the child reactor is later moved to eventually consistent processing.
  # @example
  #
  #  class CartActor < Sourced::Actor
  #    # The CartListings projector
  #    # will be run synchronously when events are appended by this Actor
  #    # Any error raised by the projector will cause the transaction to rollback
  #    sync CartListings
  #  end
  module Sync
    CallableInterface = Sourced::Types::Interface[:call]

    def self.included(base)
      super
      base.extend ClassMethods
    end

    def sync_blocks_with(**args)
      self.class.sync_blocks.map do |callable|
        case callable
        when Proc
          proc { instance_exec(**args, &callable) }
        when CallableInterface
          proc { callable.call(**args) }
        else
          raise ArgumentError, "Not a valid sync block: #{callable.inspect}"
        end
      end
    end

    def sync_actions_with(**args)
      sync_blocks_with(**args).map do |bl|
        Actions::Sync.new(bl)
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
        sync_blocks << (block || callable)
      end
    end
  end
end
