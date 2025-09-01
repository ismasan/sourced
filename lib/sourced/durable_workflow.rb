# frozen_string_literal: true

module Sourced
  class DurableWorkflow
    extend Sourced::Consumer

    WorkflowStarted = Sourced::Event.define('durable.workflow.started') do
      attribute :args, Sourced::Types::Array.default([].freeze)
    end
    WorkflowComplete = Sourced::Event.define('durable.workflow.complete') do
      attribute :output, Sourced::Types::Any
    end
    StepStarted = Sourced::Event.define('durable.step.started') do
      attribute :step_name, Sourced::Types::Lax::Symbol
      attribute :args, Sourced::Types::Array.default([].freeze)
    end
    StepFailed = Sourced::Event.define('durable.step.failed') do
      attribute :step_name, Sourced::Types::Lax::Symbol
      attribute :error_class, String
      attribute :backtrace, Sourced::Types::Array[String]
    end
    StepComplete = Sourced::Event.define('durable.step.complete') do
      attribute :step_name, Sourced::Types::Lax::Symbol
      attribute :output, Sourced::Types::Any
    end

    def self.handled_messages
      [
        WorkflowStarted,
        WorkflowComplete,
        StepStarted,
        StepFailed,
        StepComplete
      ]
    end

    def self.handle(message, history:, logger: Sourced.config.logger)
      from(history, logger:).__handle(message)
    end

    class Waiter
      attr_reader :stream_id, :instance

      def initialize(reactor, stream_id, backend: Sourced.config.backend)
        @reactor, @stream_id, @backend = reactor, stream_id, backend
        @instance = @reactor.new(@stream_id)
        @value = nil
      end

      def wait
        while instance.status != :complete
          sleep 0.1
          load
        end
        instance.output
      end

      def load
        history = @backend.read_event_stream(@stream_id)
        instance.__from(history)
      end
    end

    def self.from(history, logger: nil)
      new(logger:).__from(history)
    end

    def self.execute(*args)
      stream_id = "workflow-#{SecureRandom.uuid}"
      evt = WorkflowStarted.parse(stream_id:, payload: { args: })
      Sourced.config.backend.append_next_to_stream(stream_id, evt)
      Waiter.new(self, stream_id, backend: Sourced.config.backend)
    end

    def self.durable(method_name)
      source_method = :"__durable_source_#{method_name}"
      alias_method source_method, method_name
      define_method method_name do |*args|
        cached = @lookup[method_name]
        case cached&.status
        when :complete
          cached.output
        when :started # ready to call.
          begin
            output = send(source_method, *args)
            @new_events << StepComplete.parse(
              stream_id: id, 
              payload: { step_name: method_name, output: }
            )
            throw :halt
          rescue StandardError => e
            @new_events << StepFailed.parse(
              stream_id: id, 
              payload: { step_name: method_name, error_class: e.class.to_s, backtrace: e.backtrace }
            )
            throw :halt
          end
        when :failed # retry. Exponential backoff, etc
          sleep 1
          @new_events << StepStarted.parse(
            stream_id: id, 
            payload: { step_name: method_name, args: }
          )
          throw :halt
        when nil # first call. Schedule StepStarted
          @new_events << StepStarted.parse(
            stream_id: id, 
            payload: { step_name: method_name, args: }
          )
          throw :halt
        end
      end
    end

    Step = Struct.new(:status, :backtrace, :output) do
      def self.build
        new(:started, [], nil)
      end

      def fail_with(backtrace)
        self.status = :failed
        self.backtrace = backtrace
        self
      end

      def complete_with(output)
        self.status = :complete
        self.output = output
        self
      end
    end

    attr_reader :id, :args, :output, :status

    def initialize(logger: nil)
      @id = nil
      @logger = logger
      @status = :new
      @args = []
      @output = nil
      @lookup = {}
      @new_events = []
    end

    def __from(history)
      history.each do |event|
        __evolve(event)
      end
      self
    end

    def __evolve(event)
      @id = event.stream_id
      case event
      when WorkflowStarted
        @args = event.payload.args
        @status = :started
      when StepStarted
        @lookup[event.payload.step_name] = Step.build
      when StepFailed
        @lookup[event.payload.step_name].fail_with(event.payload.backtrace)
      when StepComplete
        @lookup[event.payload.step_name].complete_with(event.payload.output)
      when WorkflowComplete
        @status = :complete
        @output = event.payload.output
      else
        raise "No idea how to handle #{event.inspect}"
      end
    end

    def __handle(message)
      return Sourced::Actions::OK if @status == :complete

      catch(:halt) do
        output = execute(*@args)
        @new_events << WorkflowComplete.parse(
          stream_id: id,
          payload: { output: }
        )
      end

      last_seq = message.seq
      events = @new_events.map { |e| e.with(seq: last_seq += 1 )}
      Sourced::Actions::AppendAfter.new(id, events)
    end
  end
end
