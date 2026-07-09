# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/store'
require 'sequel'

module InterpreterTestMessages
  DoThing = Sourced::Command.define('interp_test.do_thing') do
    attribute :thing_id, String
  end

  ThingDone = Sourced::Event.define('interp_test.thing_done') do
    attribute :thing_id, String
  end
end

RSpec.describe Sourced::ActionRunner do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::Store.new(db) }
  let(:interpreter) { described_class.new(store) }
  let(:source) { InterpreterTestMessages::DoThing.new(payload: { thing_id: 't1' }) }
  let(:after_syncs) { [] }

  before { store.install! }

  def new_event
    InterpreterTestMessages::ThingDone.new(payload: { thing_id: 't1' })
  end

  describe 'a plain Hash and a Sourced::Actions value object interpret identically' do
    it 'appends + correlates for both shapes' do
      hash_signal = { type: :append, messages: [new_event] }
      object_signal = Sourced::Actions::Append.new([new_event])

      interpreter.run(hash_signal, source, after_syncs)
      interpreter.run(object_signal, source, after_syncs)

      appended = store.read(InterpreterTestMessages::ThingDone.to_conditions(thing_id: 't1')).messages
      expect(appended.size).to eq(2)
      # Both correlated from the source command
      expect(appended.map(&:causation_id).uniq).to eq([source.id])
      expect(appended.map(&:correlation_id).uniq).to eq([source.correlation_id])
    end
  end

  describe 'delete flag' do
    it 'returns true for an append signal with delete: true' do
      expect(interpreter.run({ type: :append, messages: [new_event], delete: true }, source, after_syncs)).to be true
    end

    it 'returns false for an append signal without delete' do
      expect(interpreter.run({ type: :append, messages: [new_event] }, source, after_syncs)).to be false
    end

    it 'returns true for a bare ack delete signal (no append)' do
      expect(interpreter.run({ type: :ack, delete: true }, source, after_syncs)).to be true
    end

    it 'treats Actions::OK as a non-deleting ack' do
      expect(interpreter.run(Sourced::Actions::OK, source, after_syncs)).to be false
    end
  end

  describe 'sync vs after_sync' do
    it 'runs :sync immediately' do
      ran = false
      interpreter.run({ type: :sync, work: -> { ran = true } }, source, after_syncs)
      expect(ran).to be true
      expect(after_syncs).to be_empty
    end

    it 'defers :after_sync' do
      ran = false
      interpreter.run({ type: :after_sync, work: -> { ran = true } }, source, after_syncs)
      expect(ran).to be false
      expect(after_syncs.size).to eq(1)
      after_syncs.each(&:call)
      expect(ran).to be true
    end
  end

  describe 'index_by resolution (delegated to the store)' do
    it 'indexes appended messages by id when their type is registered id-partitioned' do
      store.register_consumer_group('q', partition_by: ['id'], exclusive: true,
        handled_types: [InterpreterTestMessages::ThingDone.type])

      interpreter.run({ type: :append, messages: [new_event] }, source, after_syncs)

      names = db[:sourced_key_pairs].select_map(:name).uniq
      expect(names).to include('id')
      expect(names).not_to include('thing_id')
    end
  end
end
