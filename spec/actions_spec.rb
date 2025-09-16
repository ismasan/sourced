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

    specify '#deconstruct' do
      msg1 = Sourced::Message.new(stream_id: 'one')
      action = Sourced::Actions::AppendNext.new([msg1])
      expect(action.deconstruct).to eq([:append_next, [msg1]])
    end
  end

  describe Sourced::Actions::AppendAfter do
    specify '#deconstruct' do
      msg1 = Sourced::Message.new(stream_id: 'one')
      action = Sourced::Actions::AppendAfter.new('one', [msg1])
      expect(action.deconstruct).to eq([:append_after, 'one', [msg1]])
    end
  end

  describe Sourced::Actions::Schedule do
    specify '#deconstruct' do
      msg1 = Sourced::Message.new(stream_id: 'one')
      now = Time.now
      action = Sourced::Actions::Schedule.new([msg1], at: now)
      expect(action.deconstruct).to eq([:schedule, [msg1], now])
    end
  end

  describe Sourced::Actions::OK do
    specify '#deconstruct' do
      expect(Sourced::Actions::OK.deconstruct).to eq([:ok])
    end
  end

  describe Sourced::Actions::RETRY do
    specify '#deconstruct' do
      expect(Sourced::Actions::RETRY.deconstruct).to eq([:retry])
    end
  end
end
