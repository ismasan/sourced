# frozen_string_literal: true

module Sourced
  # The rules a consumer group's status follows: which commands each status
  # accepts, and where they move it. Never registered as a reactor: the store
  # places it at a group's stored status and context with {Machine.at},
  # decides a command, and writes back its paths and context. The events it
  # produces are discarded.
  #
  #   group = ConsumerGroupMachine.at(['active'], { retry_count: 0 }, group_id: 'shop.orders')
  #   group.handles_command?(ConsumerGroupMachine::Reset.new(payload: { group_id: 'shop.orders' })) # => false
  #
  # Operator commands ({OPERATOR_COMMANDS}) are refused where a status
  # doesn't accept them. Worker commands ({WORKER_COMMANDS}) come from a
  # worker's error handling and acks, and are ignored where it doesn't.
  #
  # Its context is the group's error context, a {Context}: what the worker
  # commands record about a failure, and an operator's stop message.
  class ConsumerGroupMachine < Machine
    partition_by :group_id
    consumer_group 'sourced.consumer_groups'

    Context = Types::Hash[
      retry_count: Types::Integer,
      'retry_at?': Time,
      'offset_id?': Types::Integer,
      'exception_class?': String,
      'exception_message?': String,
      'exception_backtrace?': Types::Array[String],
      'message?': String
    ]

    # Operator commands
    Start = Command(:start)
    Stop = Command(:stop) { attribute? :message, Types::String.nullable }
    Reset = Command(:reset)

    # Worker commands. Retry and Fail carry the failure (Fail, with its
    # backtrace); Retry, the claim that failed, which Recover is for.
    Retry = Command(:retry) do
      attribute :retry_at, Time
      attribute? :offset_id, Types::Integer.nullable
      attribute? :exception_class, Types::String.nullable
      attribute? :exception_message, Types::String.nullable
    end
    Recover = Command(:recover)
    Fail = Command(:fail) do
      attribute? :exception_class, Types::String.nullable
      attribute? :exception_message, Types::String.nullable
      attribute? :exception_backtrace, Types::Array[String].nullable
    end

    OPERATOR_COMMANDS = [Start, Stop, Reset].freeze
    WORKER_COMMANDS = [Retry, Recover, Fail].freeze

    # What the commands record in the context
    RetryScheduled = Event(:retry_scheduled) do
      attribute :retry_at, Time
      attribute? :offset_id, Types::Integer.nullable
      attribute? :exception_class, Types::String.nullable
      attribute? :exception_message, Types::String.nullable
    end
    FailureRecorded = Event(:failure_recorded) do
      attribute? :exception_class, Types::String.nullable
      attribute? :exception_message, Types::String.nullable
      attribute? :exception_backtrace, Types::Array[String].nullable
    end
    StopRecorded = Event(:stop_recorded) { attribute? :message, Types::String.nullable }
    ContextCleared = Event(:context_cleared)

    Active = State(:active)
    Retrying = State(:retrying)
    Stopped = State(:stopped)
    Failed = State(:failed)

    FAILURE = %i[exception_class exception_message].freeze
    FAILURE_WITH_BACKTRACE = [*FAILURE, :exception_backtrace].freeze

    context { { retry_count: 0 } }

    evolve RetryScheduled do |context, evt|
      context[:retry_count] += 1
      context[:retry_at] = evt.payload.retry_at
      context.delete(:offset_id)
      context.merge!(evt.payload.to_h.slice(:offset_id, *FAILURE).compact)
    end

    evolve FailureRecorded do |context, evt|
      context.delete(:retry_at)
      context.merge!(evt.payload.to_h.slice(*FAILURE_WITH_BACKTRACE).compact)
    end

    evolve StopRecorded do |context, evt|
      context.delete(:retry_at)
      context[:message] = evt.payload.message if evt.payload.message
    end

    evolve(ContextCleared) { |context, _evt| context.replace(retry_count: 0) }

    # A registered group starts active.
    initial { command Start => Active }

    state Active do
      command Stop => Stopped do |_context, cmd|
        event StopRecorded, message: cmd.payload.message
      end
      command Retry => Retrying do |_context, cmd|
        event RetryScheduled, **cmd.payload.to_h.except(:group_id)
      end
      command Fail => Failed do |_context, cmd|
        event FailureRecorded, **cmd.payload.to_h.except(:group_id)
      end
    end

    state Retrying do
      # The next attempt failed too: wait again.
      command Retry => Retrying do |_context, cmd|
        event RetryScheduled, **cmd.payload.to_h.except(:group_id)
      end
      command(Recover => Active) { |_context, _cmd| event ContextCleared }
      command Fail => Failed do |_context, cmd|
        event FailureRecorded, **cmd.payload.to_h.except(:group_id)
      end
      # An operator doesn't wait for the next attempt.
      command(Start => Active) { |_context, _cmd| event ContextCleared }
      command Stop => Stopped do |_context, cmd|
        event StopRecorded, message: cmd.payload.message
      end
    end

    state Stopped do
      command(Start => Active) { |_context, _cmd| event ContextCleared }
      command Reset => Stopped
    end

    state Failed do
      command(Start => Active) { |_context, _cmd| event ContextCleared }
      command Stop => Stopped do |_context, cmd|
        event StopRecorded, message: cmd.payload.message
      end
      command Reset => Failed
    end

    # The JSON decoder and encoder for {Context}, compiled on first use, so
    # an app's own encoders, registered before Sourced boots, are in.
    # @return [Array(Plumb::Composable, Plumb::Composable)]
    def self.context_codec = @context_codec ||= Plumb::Codec::JSON.for(Context)

    # @param data [Hash] the context's JSON-native form, String- or Symbol-keyed
    # @return [Hash] the context
    # @raise [Plumb::ParseError] if it isn't a {Context}
    def self.decode_context(data) = context_codec.first.parse(data)

    # @param context [Hash]
    # @return [Hash] its JSON-native form
    # @raise [Plumb::ParseError] if it isn't a {Context}
    def self.encode_context(context) = context_codec.last.parse(context)
  end
end
