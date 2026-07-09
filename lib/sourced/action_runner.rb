# frozen_string_literal: true

module Sourced
  # Routes declarative action signals to store operations.
  #
  # A "signal" is either a plain Hash (`{ type: :append, messages: [...], ... }`)
  # or a {Sourced::Actions} value object (which destructures to the same shape via
  # +deconstruct_keys+). This is the *only* code that touches the store on behalf
  # of actions, so third-party reactors can return plain-Hash signals without
  # depending on Sourced's action classes or store API.
  #
  # Correlation (causation/correlation ids) is applied here at execution time.
  #
  # @example
  #   interp = ActionRunner.new(store, index_resolver: ->(type) { :payload })
  #   after_syncs = []
  #   delete = interp.run(signal, source_message, after_syncs)
  class ActionRunner
    # @param store [Sourced::Store]
    # @param index_resolver [#call] maps a message type string to +:id+ or +:payload+
    #   to decide how appended messages are indexed. Defaults to +:payload+.
    def initialize(store, index_resolver: nil)
      @store = store
      @index_resolver = index_resolver || ->(_type) { :payload }
    end

    # Run a single signal for a source message.
    #
    # Appends/schedules/syncs run immediately; after_sync works are collected into
    # +after_syncs+ to run once the transaction commits.
    #
    # @param signal [Hash, Sourced::Actions::*, Symbol] the action signal
    # @param source_message [Sourced::Message] default correlation source
    # @param after_syncs [Array<#call>] collector for deferred works
    # @return [Boolean] whether this signal requests the source message be deleted on ack
    def run(signal, source_message, after_syncs)
      case normalize(signal)
      in { type: :append } => s
        append(s, source_message)
        truthy(s[:delete])
      in { type: :schedule } => s
        schedule(s, source_message)
        truthy(s[:delete])
      in { type: :sync, work: }
        work.call
        false
      in { type: :after_sync, work: }
        after_syncs << work
        false
      in { type: :ack } => s
        truthy(s[:delete])
      end
    end

    private

    # Coerce a value object into a canonical Hash signal (Hashes pass through).
    def normalize(signal)
      signal.is_a?(Hash) ? signal : signal.deconstruct_keys(nil)
    end

    def truthy(value) = value ? true : false

    def append(signal, source_message)
      correlate_from = signal[:source] || source_message
      to_append = Array(signal[:messages]).map { |m| correlate_from.correlate(m) }
      return if to_append.empty?

      # Index each appended message according to its consuming reactor's basis.
      to_append.group_by { |m| @index_resolver.call(m.type) }.each do |basis, group|
        @store.append(group, guard: signal[:guard], index_by: basis)
      end
    end

    def schedule(signal, source_message)
      correlate_from = signal[:source] || source_message
      to_schedule = Array(signal[:messages]).map { |m| correlate_from.correlate(m) }
      return if to_schedule.empty?

      @store.schedule_messages(to_schedule, at: signal[:at])
    end
  end
end
