# frozen_string_literal: true

module Sourced
  class DurableWorkflow
    extend Sourced::Consumer

    UnknownMessageError = Class.new(StandardError)

    def self.step_key(step_name, args)
      [step_name, args].hash.to_s
    end

    def self.inherited(child)
      cname = child.name.to_s.gsub(/::/, '.')
        .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
        .gsub(/([a-z\d])([A-Z])/, '\1_\2')
        .tr("-", "_")
        .downcase

      child.const_set(:WorkflowStarted, Sourced::Event.define("#{cname}.workflow.started") do
        attribute :args, Sourced::Types::Array.default([].freeze)
      end)
      child.const_set(:WorkflowComplete, Sourced::Event.define("#{cname}.workflow.complete") do
        attribute :output, Sourced::Types::Any
      end)
      child.const_set(:WorkflowFailed, Sourced::Event.define("#{cname}.workflow.failed"))
      child.const_set(:StepStarted, Sourced::Event.define("#{cname}.step.started") do
        attribute :key, String
        attribute :step_name, Sourced::Types::Lax::Symbol
        attribute :args, Sourced::Types::Array.default([].freeze)
      end)
      child.const_set(:StepFailed, Sourced::Event.define("#{cname}.step.failed") do
        attribute :key, String
        attribute :step_name, Sourced::Types::Lax::Symbol
        attribute :error_class, String
        attribute :backtrace, Sourced::Types::Array[String]
      end)
      child.const_set(:StepComplete, Sourced::Event.define("#{cname}.step.complete") do
        attribute :key, String
        attribute :step_name, Sourced::Types::Lax::Symbol
        attribute :output, Sourced::Types::Any
      end)
    end

    def self.handled_messages
      [
        self::WorkflowStarted,
        self::WorkflowComplete,
        self::StepStarted,
        self::StepFailed,
        self::StepComplete
      ]
    end

    def self.handle(message, history:, logger: Sourced.config.logger)
      from(history, logger:).__handle(message)
    end

    class Waiter
      attr_reader :stream_id, :instance

      def initialize(reactor, stream_id, backend: Sourced.config.backend, logger: Sourced.config.logger)
        @reactor, @stream_id, @backend = reactor, stream_id, backend
        @instance = @reactor.new(logger:)
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
      evt = self::WorkflowStarted.parse(stream_id:, payload: { args: })
      Sourced.config.backend.append_next_to_stream(stream_id, evt)
      Waiter.new(self, stream_id, backend: Sourced.config.backend)
    end

    def self.durable(method_name)
      source_method = :"__durable_source_#{method_name}"
      alias_method source_method, method_name
      define_method method_name do |*args|
        key = self.class.step_key(method_name, args)
        cached = @lookup[key]

        case cached&.status
        when :complete
          cached.output
        when :started # ready to call.
          begin
            output = send(source_method, *args)
            @new_events << self.class::StepComplete.parse(
              stream_id: id, 
              payload: { key:, step_name: method_name, output: }
            )
            throw :halt
          rescue StandardError => e
            @new_events << self.class::StepFailed.parse(
              stream_id: id, 
              payload: { key:, step_name: method_name, error_class: e.class.to_s, backtrace: e.backtrace }
            )
            throw :halt
          end
        when :failed # retry. Exponential backoff, etc
          @new_events << self.class::StepStarted.parse(
            stream_id: id, 
            payload: { key:, step_name: method_name, args: }
          )
          throw :halt
        when nil # first call. Schedule StepStarted
          @new_events << self.class::StepStarted.parse(
            stream_id: id, 
            payload: { key:, step_name: method_name, args: }
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
      when self.class::WorkflowStarted
        @args = event.payload.args
        @status = :started
      when self.class::WorkflowFailed
        @status = :failed
      when self.class::StepStarted
        @lookup[event.payload.key] = Step.build
      when self.class::StepFailed
        @lookup[event.payload.key].fail_with(event.payload.backtrace)
      when self.class::StepComplete
        @lookup[event.payload.key].complete_with(event.payload.output)
      when self.class::WorkflowComplete
        @status = :complete
        @output = event.payload.output
      else
        raise UnknownMessageError, "No idea how to handle #{event.inspect}"
      end
    end

    def __handle(message)
      return Sourced::Actions::OK if @status == :complete || @status == :failed

      catch(:halt) do
        output = execute(*@args)
        @new_events << self.class::WorkflowComplete.parse(
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
