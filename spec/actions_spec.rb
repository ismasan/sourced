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
  end
end
