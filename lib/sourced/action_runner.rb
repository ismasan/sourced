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
  # Correlation (causation/correlation ids and merged metadata) is applied here
  # at execution time, so a reactor never has to correlate what it produces.
  # The hooks in a pair see the result: {#run_pair} hands every +:sync+ and
  # +:after_sync+ work the messages appended so far in that pair, as stored.
  #
  # One runner serves one claimed batch. A batch's appends are guarded by a
  # single {ConsistencyGuard}, read before the first append, so each append
  # after the first would otherwise find the batch's own earlier appends past
  # the guard and report a conflict. The runner remembers the last position it
  # appended and raises every later guard's floor to it: messages this batch
  # wrote are not concurrent writes, while anything another writer slips in
  # after them still is.
  #
  # @example
  #   runner = ActionRunner.new(store)
  #   after_syncs = []
  #   delete = runner.run_pair(signals, source_message, after_syncs)
  class ActionRunner
    # @param store [Sourced::Store]
    def initialize(store)
      @store = store
      @last_appended = nil
    end

    # Run one action pair: every signal a reactor returned for one source
    # message, in order. Messages appended by the pair's +:append+ signals
    # accumulate, correlated, and each +:sync+ work runs with those appended
    # before it; each +:after_sync+ work is deferred with the pair's complete
    # list, since it runs after the commit.
    #
    # @param signals [Array<Hash, Sourced::Actions::*>] the pair's action signals
    # @param source_message [Sourced::Message] the message the pair was produced for
    # @param after_syncs [Array<#call>] collector for deferred works
    # @return [Boolean] whether any signal requests the source message be deleted on ack
    def run_pair(signals, source_message, after_syncs)
      appended = []
      Array(signals).inject(false) do |delete, signal|
        run(signal, source_message, after_syncs, appended:) || delete
      end
    end

    # Run a single signal for a source message.
    #
    # Appends/schedules/syncs run immediately; after_sync works are collected into
    # +after_syncs+ to run once the transaction commits.
    #
    # @param signal [Hash, Sourced::Actions::*, Symbol] the action signal
    # @param source_message [Sourced::Message] default correlation source
    # @param after_syncs [Array<#call>] collector for deferred works
    # @param appended [Array<Sourced::Message>] the pair's accumulator of
    #   correlated appended messages; grown by +:append+, handed to hooks
    # @return [Boolean] whether this signal requests the source message be deleted on ack
    def run(signal, source_message, after_syncs, appended: [])
      case normalize(signal)
      in { type: :append } => s
        appended.concat(append(s, source_message))
        !!s[:delete]
      in { type: :sync, work: }
        Actions.invoke(work, appended)
        false
      in { type: :after_sync, work: }
        after_syncs << -> { Actions.invoke(work, appended) }
        false
      in { type: :ack } => s
        !!s[:delete]
      end
    end

    # The messages an +:append+ signal will store, correlated against the
    # signal's own +source+ or, failing that, the pair's source message. The
    # single definition of what the runner correlates, shared with the GWT
    # test helper so hooks under test see what they would see in production.
    #
    # @param signal [Hash] a normalized +:append+ signal
    # @param source_message [Sourced::Message] default correlation source
    # @return [Array<Sourced::Message>]
    def self.correlate(signal, source_message)
      correlate_from = signal[:source] || source_message
      Array(signal[:messages]).map { |m| correlate_from.correlate(m) }
    end

    private

    # Coerce a value object into a canonical Hash signal (Hashes pass through).
    def normalize(signal)
      signal.is_a?(Hash) ? signal : signal.deconstruct_keys(nil)
    end

    # @return [Array<Sourced::Message>] the correlated messages appended
    def append(signal, source_message)
      to_append = self.class.correlate(signal, source_message)
      return to_append if to_append.empty?

      guard = signal[:guard]
      if guard && @last_appended && @last_appended > guard.last_position
        guard = guard.with(last_position: @last_appended)
      end

      # The store resolves each message's index basis from its consuming group,
      # and defers any future-dated message into the scheduled_messages table.
      # nil when every message was future-dated: scheduled, not positioned, so
      # there is no new floor to claim.
      position = @store.append(to_append, guard: guard)
      @last_appended = position if position
      to_append
    end
  end
end
