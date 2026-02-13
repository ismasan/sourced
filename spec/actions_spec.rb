# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Actions do
  describe Sourced::Actions::AppendNext do
    specify '#==' do
      msg1 = Sourced::Message.new(stream_id: 'one')
      msg2 = Sourced::Message.new(stream_id: 'two')
      action1 = Sourced::Actions::AppendNext.new([msg1])
      action2 = Sourced::Actions::AppendNext.new([msg1])
      action3 = Sourced::Actions::AppendNext.new([msg2])
      expect(action1).to eq(action2)
      expect(action1).not_to eq(action3)
    end

    describe '#execute' do
      it 'correlates messages and appends via backend.append_next_to_stream' do
        backend = Sourced::Backends::TestBackend.new
        source = Sourced::Message.new(stream_id: 'stream-1', seq: 1)
        msg1 = Sourced::Message.new(stream_id: 'stream-1')
        msg2 = Sourced::Message.new(stream_id: 'stream-2')
        action = Sourced::Actions::AppendNext.new([msg1, msg2])

        correlated = action.execute(backend, source)

        expect(correlated.size).to eq(2)
        expect(correlated.map(&:correlation_id)).to all(eq(source.correlation_id))
        expect(correlated.map(&:causation_id)).to all(eq(source.id))
        expect(backend.read_stream('stream-1').size).to eq(1)
        expect(backend.read_stream('stream-2').size).to eq(1)
      end
    end
  end

  describe Sourced::Actions::AppendAfter do
    describe '#execute' do
      it 'correlates messages and appends via backend.append_to_stream' do
        backend = Sourced::Backends::TestBackend.new
        source = Sourced::Message.new(stream_id: 'stream-1', seq: 1)
        msg1 = Sourced::Message.new(stream_id: 'stream-1', seq: 1)
        msg2 = Sourced::Message.new(stream_id: 'stream-1', seq: 2)
        action = Sourced::Actions::AppendAfter.new('stream-1', [msg1, msg2])

        correlated = action.execute(backend, source)

        expect(correlated.size).to eq(2)
        expect(correlated.map(&:correlation_id)).to all(eq(source.correlation_id))
        expect(correlated.map(&:causation_id)).to all(eq(source.id))
        expect(backend.read_stream('stream-1').size).to eq(2)
      end
    end
  end

  describe Sourced::Actions::Schedule do
    describe '#execute' do
      it 'correlates messages and schedules via backend.schedule_messages' do
        backend = Sourced::Backends::TestBackend.new
        source = Sourced::Message.new(stream_id: 'stream-1', seq: 1)
        future_time = Time.now + 3600
        msg = Sourced::Message.new(stream_id: 'stream-1')
        action = Sourced::Actions::Schedule.new([msg], at: future_time)

        correlated = action.execute(backend, source)

        expect(correlated.size).to eq(1)
        expect(correlated.first.correlation_id).to eq(source.correlation_id)
        expect(correlated.first.causation_id).to eq(source.id)
      end
    end
  end

  describe Sourced::Actions::Sync do
    describe '#execute' do
      it 'calls the work block and returns nil' do
        called = false
        action = Sourced::Actions::Sync.new(-> { called = true })
        source = Sourced::Message.new(stream_id: 'stream-1')
        backend = Sourced::Backends::TestBackend.new

        result = action.execute(backend, source)

        expect(called).to be true
        expect(result).to be_nil
      end
    end
  end
end
