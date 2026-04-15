# frozen_string_literal: true

require 'securerandom'

module Sourced
  module CCC
    # Stream-less port of {Sourced::DurableWorkflow}.
    #
    # A workflow instance is identified by a +workflow_id+ string, which doubles
    # as the partition key. All lifecycle events (WorkflowStarted, StepStarted,
    # StepFailed, StepComplete, ContextUpdated, WaitStarted, WaitEnded,
    # WorkflowComplete, WorkflowFailed) carry +workflow_id+ as their first
    # payload attribute so {CCC::Message#extracted_keys} indexes them for
    # partition queries.
    #
    # The +durable+ / +wait+ / +context+ / +execute+ DSL mirrors
    # {Sourced::DurableWorkflow} 1:1. The step-memoisation mechanism
    # (<tt>@lookup</tt> + <tt>catch(:halt)</tt>) is unchanged; only the
    # persistence and dispatch layer differ.
    class DurableWorkflow
      extend CCC::Consumer
      include CCC::Evolve

      partition_by :workflow_id

      UnknownMessageError = Class.new(StandardError)

      # Stable hash-based key for a given (method, args) pair.
      def self.step_key(step_name, args)
        [step_name, args].hash.to_s
      end

      def self.inherited(child)
        super
        child.partition_by(:workflow_id)
        cname = child.name.to_s.gsub(/::/, '.')
          .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
          .gsub(/([a-z\d])([A-Z])/, '\1_\2')
          .tr('-', '_')
          .downcase

        child.const_set(:WorkflowStarted, CCC::Event.define("#{cname}.workflow.started") do
          attribute :workflow_id, String
          attribute :args, Sourced::Types::Array.default([].freeze)
        end)
        child.const_set(:ContextUpdated, CCC::Event.define("#{cname}.context.updated") do
          attribute :workflow_id, String
          attribute :context, Sourced::Types::Any
        end)
        child.const_set(:WorkflowComplete, CCC::Event.define("#{cname}.workflow.complete") do
          attribute :workflow_id, String
          attribute :output, Sourced::Types::Any
        end)
        child.const_set(:WorkflowFailed, CCC::Event.define("#{cname}.workflow.failed") do
          attribute :workflow_id, String
        end)
        child.const_set(:StepStarted, CCC::Event.define("#{cname}.step.started") do
          attribute :workflow_id, String
          attribute :key, String
          attribute :step_name, Sourced::Types::Lax::Symbol
          attribute :args, Sourced::Types::Array.default([].freeze)
        end)
        child.const_set(:StepFailed, CCC::Event.define("#{cname}.step.failed") do
          attribute :workflow_id, String
          attribute :key, String
          attribute :step_name, Sourced::Types::Lax::Symbol
          attribute :error_message, String
          attribute :error_class, String
          attribute :backtrace, Sourced::Types::Array[String]
        end)
        child.const_set(:StepComplete, CCC::Event.define("#{cname}.step.complete") do
          attribute :workflow_id, String
          attribute :key, String
          attribute :step_name, Sourced::Types::Lax::Symbol
          attribute :output, Sourced::Types::Any
        end)
        child.const_set(:WaitStarted, CCC::Event.define("#{cname}.wait.started") do
          attribute :workflow_id, String
          attribute :count, Integer
          attribute :at, Sourced::Types::Forms::Time
        end)
        child.const_set(:WaitEnded, CCC::Event.define("#{cname}.wait.ended") do
          attribute :workflow_id, String
        end)

        # Register all event classes so:
        #  - Router claims them on our consumer group (`handled_messages`).
        #  - `context_for(workflow_id:)` builds OR conditions for the partition
        #    read (one per event type, via `Message.to_conditions`).
        [
          child::WorkflowStarted, child::ContextUpdated, child::WorkflowComplete,
          child::WorkflowFailed, child::StepStarted, child::StepFailed,
          child::StepComplete, child::WaitStarted, child::WaitEnded
        ].each do |klass|
          child.handled_messages_for_evolve << klass unless child.handled_messages_for_evolve.include?(klass)
        end
      end

      # Message types this consumer claims. Same set as evolve types because
      # every workflow event both advances state and re-triggers the workflow.
      def self.handled_messages
        handled_messages_for_evolve
      end

      # Define the initial context hash. Block receives no arguments.
      def self.context(&block)
        define_method :initial_context, &block
      end

      # Wrap a method so the runtime memoises its result across workflow
      # re-entries. Mirrors {Sourced::DurableWorkflow.durable}.
      def self.durable(method_name, retries: nil)
        source_method = :"__durable_source_#{method_name}"
        alias_method source_method, method_name
        define_method method_name do |*args|
          key = self.class.step_key(method_name, args)
          cached = @lookup[key]

          case cached&.status
          when :complete
            cached.output
          when :started
            begin
              output = send(source_method, *args)
              @new_events << self.class::StepComplete.new(
                payload: { workflow_id: id, key:, step_name: method_name, output: }
              )
              throw :halt
            rescue StandardError => e
              @new_events << self.class::StepFailed.new(
                payload: {
                  workflow_id: id,
                  key:,
                  step_name: method_name,
                  error_message: e.inspect,
                  error_class: e.class.to_s,
                  backtrace: e.backtrace
                }
              )
              if retries && cached.attempts == retries
                @new_events << self.class::WorkflowFailed.new(payload: { workflow_id: id })
              end
              throw :halt
            end
          when :failed
            @new_events << self.class::StepStarted.new(
              payload: { workflow_id: id, key:, step_name: method_name, args: }
            )
            throw :halt
          when nil
            @new_events << self.class::StepStarted.new(
              payload: { workflow_id: id, key:, step_name: method_name, args: }
            )
            throw :halt
          end
        end
      end

      Step = Struct.new(:status, :backtrace, :output, :attempts) do
        def self.build
          new(:started, [], nil, 0)
        end

        def start
          self.status = :started
          self.attempts += 1
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

      # Kick off a new workflow instance. Appends a WorkflowStarted event and
      # returns a {Waiter} that can poll for completion.
      #
      # @param args [Array] positional args passed to the workflow's #execute
      # @param store [CCC::Store] defaults to CCC.store
      # @return [Waiter]
      def self.execute(*args, store: CCC.store)
        workflow_id = "workflow-#{SecureRandom.uuid}"
        evt = self::WorkflowStarted.new(payload: { workflow_id:, args: })
        store.append([evt])
        Waiter.new(self, workflow_id, store:)
      end

      # Router entry point. Drops claim.messages from the read history (the
      # router's +store.read(conditions)+ returns the full partition, including
      # messages being claimed) and delegates to {.handle_batch}.
      def self.handle_claim(claim, history:)
        claim_positions = claim.messages.map { |m| m.position if m.respond_to?(:position) }.compact.to_set
        prior = history.messages.reject { |m| m.respond_to?(:position) && claim_positions.include?(m.position) }
        prior_history = ReadResult.new(messages: prior, guard: history.guard)
        values = claim.partition_value.transform_keys(&:to_sym)
        handle_batch(values, claim.messages, history: prior_history)
      end

      # GWT-compatible entry point. +history.messages+ must be disjoint from
      # +new_messages+ — the caller owns that distinction.
      def self.handle_batch(partition_values, new_messages, history:, replaying: false)
        workflow_id = partition_values[:workflow_id]
        instance = new([workflow_id])
        instance.__replay(history.messages)

        each_with_partial_ack(new_messages) do |msg|
          instance.__evolve(msg)
          actions = instance.__handle(msg, guard: history.guard)
          [actions, msg]
        end
      end

      # Direct handler used by unit tests and by {Waiter}. Mirrors
      # {Sourced::DurableWorkflow.handle}: +history+ should already contain
      # +message+ as its last element.
      def self.handle(message, history:)
        from(history).__handle(message)
      end

      # Rebuild a workflow instance by replaying +history+.
      def self.from(history)
        new.__replay(history)
      end

      # Load a workflow instance for +workflow_id+ from the store.
      def self.load(workflow_id, store: CCC.store)
        _inst, _rr = CCC.load(self, store:, workflow_id: workflow_id)
      end

      # Polls the store for terminal workflow events.
      class Waiter
        attr_reader :workflow_id, :instance

        def initialize(klass, workflow_id, store: CCC.store)
          @klass = klass
          @workflow_id = workflow_id
          @store = store
          @instance = klass.new([workflow_id])
        end

        def wait(timeout: nil)
          deadline = timeout ? Time.now + timeout : nil
          until @instance.status == :complete || @instance.status == :failed
            raise 'DurableWorkflow wait timed out' if deadline && Time.now > deadline

            sleep 0.05
            load
          end
          @instance
        end

        def load
          handled_types = @klass.handled_messages_for_evolve.map(&:type).uniq
          result = @store.read_partition({ workflow_id: @workflow_id }, handled_types:)
          @instance = @klass.new([@workflow_id])
          @instance.__replay(result.messages)
          @instance
        end
      end

      attr_reader :id, :context, :args, :output, :status

      # +partition_values+ may be:
      #   - an Array like +['wf-id']+ (from {.handle_claim})
      #   - a Hash like +{ workflow_id: 'wf-id' }+ (from {CCC.load})
      #   - a String +'wf-id'+
      #   - nil
      def initialize(partition_values = nil)
        @id = case partition_values
              when Array then partition_values.first
              when Hash then partition_values[:workflow_id]
              when String then partition_values
              else nil
              end
        @status = :new
        @args = []
        @output = nil
        @lookup = {}
        @new_events = []
        @wait_count = 0
        @waiters = []
        @context = initial_context
      end

      def initial_context = nil

      def __replay(history)
        Array(history).each { |m| __evolve(m) }
        self
      end

      # Override CCC::Evolve#evolve so {CCC.load} (which calls +instance.evolve+)
      # applies workflow events via our manual dispatcher.
      def evolve(messages)
        __replay(messages)
      end

      def __evolve(event)
        case event
        when self.class::ContextUpdated
          @context = deep_dup(event.payload.context)
        when self.class::WorkflowStarted
          @id ||= event.payload.workflow_id
          @args = event.payload.args
          @status = :started
        when self.class::WorkflowFailed
          @status = :failed
        when self.class::StepStarted
          (@lookup[event.payload.key] ||= Step.build).start
        when self.class::StepFailed
          @lookup[event.payload.key].fail_with(event.payload.backtrace)
        when self.class::WaitStarted
          @waiters[event.payload.count] = true
          @waiting = true
        when self.class::WaitEnded
          @waiting = false
        when self.class::StepComplete
          @lookup[event.payload.key].complete_with(event.payload.output)
        when self.class::WorkflowComplete
          @status = :complete
          @output = event.payload.output
        else
          raise UnknownMessageError, "No idea how to handle #{event.inspect}"
        end
      end

      # Decide the next action given +message+. State is assumed to already
      # reflect +message+ (caller replayed it).
      def __handle(message, guard: nil)
        return Actions::OK if @status == :complete || @status == :failed

        if message.is_a?(self.class::WaitStarted)
          evt = self.class::WaitEnded.new(payload: { workflow_id: id })
          return Actions::Schedule.new([evt], at: message.payload.at)
        end

        @initial_context = deep_dup(@context)

        completed = false
        output = nil

        catch(:halt) do
          output = execute(*@args)
          completed = true
        end

        if @context != @initial_context
          @new_events << self.class::ContextUpdated.new(
            payload: { workflow_id: id, context: deep_dup(@context) }
          )
        end

        if completed
          @new_events << self.class::WorkflowComplete.new(
            payload: { workflow_id: id, output: }
          )
        end

        events = @new_events
        @new_events = []
        return Actions::OK if events.empty?

        Actions::Append.new(events, guard: guard)
      end

      private

      def wait(seconds)
        @wait_count += 1

        if @waiters[@wait_count]
          seconds
        else
          @new_events << self.class::WaitStarted.new(
            payload: { workflow_id: id, count: @wait_count, at: Time.now + seconds }
          )
          throw :halt
        end
      end

      def deep_dup(value)
        case value
        when Hash
          value.each.with_object({}) { |(k, v), h| h[k] = deep_dup(v) }
        when Array
          value.map { |v| deep_dup(v) }
        else
          value.dup rescue value
        end
      end
    end
  end
end
