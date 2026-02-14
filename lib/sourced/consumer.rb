# frozen_string_literal: true

module Sourced
  # This mixin provides consumer info configuration
  # and a .consumer_info method to access it.
  # @example
  #
  #  class MyConsumer
  #    extend Sourced::Consumer
  #
  #    consumer do |c|
  #      # consumer group
  #      c.group_id = 'my-group'
  #
  #      # Start consuming events from the beginning of history
  #      c.start_from = :beginning
  #    end
  #  end
  #
  #  MyConsumer.consumer_info.group_id # => 'my-group'
  #
  module Consumer
    class ConsumerInfo < Types::Data
      ToBlock = Types::Any.transform(Proc) { |v| -> { v } }
      StartFromBeginning = Types::Value[:beginning] >> Types::Static[nil] >> ToBlock
      StartFromNow = Types::Value[:now] >> Types::Static[-> { Time.now - 5 }.freeze]
      StartFromTime = Types::Interface[:call].check('must return a Time') { |v| v.call.is_a?(Time) }

      StartFrom = (
        StartFromBeginning | StartFromNow | StartFromTime
      ).default { -> { nil } }

      attribute :group_id, Types::String.present, writer: true
      attribute :start_from, StartFrom, writer: true
      attribute :batch_size, Types::Integer.nullable.default(nil), writer: true
    end

    def consumer_info
      @consumer_info ||= ConsumerInfo.new(group_id: name, start_from: :beginning)
    end

    def consumer(&)
      return consumer_info unless block_given?

      info = ConsumerInfo.new(group_id: name)
      yield info
      raise Plumb::ParseError, info.errors unless info.valid?

      @consumer_info = info
    end

    # Implement this in your reactors
    # to manage exception handling in eventually-consistent workflows
    # @example retry with exponential back off
    #
    #   def self.on_exception(exception, _message, group)
    #     retry_count = group.error_context[:retry_count] || 0
    #     if retry_count < 3
    #       later = 5 + 5 * retry_count
    #       group.retry(later, retry_count: retry_count + 1)
    #     else
    #       group.stop(exception)
    #     end
    #   end
    #
    # @param exception [Exception] the exception raised
    # @param message [Sourced::Message] the event or command being handled
    # @param group [#stop, #retry] consumer group object to update state, ie. for retries
    def on_exception(exception, message, group)
      Sourced.config.error_strategy.call(exception, message, group)
    end

    # Default handle_batch implementation that wraps per-message .handle calls.
    # Returns array of [actions, source_message] pairs.
    # Reactors with optimized batch processing (Projector, Actor) override this.
    #
    # @param batch [Array<[Message, Boolean]>] array of [message, replaying] pairs
    # @return [Array<[actions, source_message]>] action pairs
    def handle_batch(batch)
      each_with_partial_ack(batch) do |message, replaying|
        kargs = {}
        kargs[:replaying] = replaying if handle_kargs.include?(:replaying)
        kargs[:logger] = Sourced.config.logger if handle_kargs.include?(:logger)
        actions = handle(message, **kargs)
        [actions, message]
      end
    end

    private

    # Iterate batch with per-message error handling.
    # Collects [actions, message] pairs returned by the block.
    # On mid-batch failure, raises PartialBatchError with pairs collected so far,
    # allowing the backend to ACK up to the last successful message.
    # If the first message fails, raises the original error (no partial ACK possible).
    #
    # Used by Consumer (default handle_batch), Actor, and Projector (reaction loop)
    # to provide partial ACK across all reactor types.
    #
    # @param batch [Array<[Message, Boolean]>] array of [message, replaying] pairs
    # @yield [message, replaying] called for each message in the batch
    # @yieldreturn [Array(actions, Message), nil] action pair, or nil to skip
    # @return [Array<[actions, source_message]>] action pairs
    def each_with_partial_ack(batch)
      results = []
      batch.each do |message, replaying|
        pair = yield(message, replaying)
        results << pair if pair
      rescue StandardError => e
        raise e if results.empty?
        raise PartialBatchError.new(results, message, e)
      end
      results
    end

    # Lazily resolved keyword argument names for the reactor's .handle method.
    # Cached as a class instance variable.
    def handle_kargs
      @handle_kargs ||= Injector.resolve_args(self, :handle)
    end
  end
end
