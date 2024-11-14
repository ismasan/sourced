# frozen_string_literal: true

require 'spec_helper'

module TestConsumer
  class TestConsumer
    extend Sourced::Consumer
  end
end

RSpec.describe Sourced::Consumer do
  describe '#group_id' do
    it 'is class name by default' do
      expect(TestConsumer::TestConsumer.consumer_info.group_id).to eq('TestConsumer::TestConsumer')
    end

    it 'can be set' do
      klass = Class.new do
        extend Sourced::Consumer

        consumer do |info|
          info.group_id = 'my-group'
        end
      end

      expect(klass.consumer_info.group_id).to eq('my-group')
    end
  end

  describe '#start_from' do
    specify 'default is nil' do
      expect(TestConsumer::TestConsumer.consumer_info.start_from.call).to be_nil
    end

    it 'can be set to a proc that returns a Time' do
      klass = Class.new do
        extend Sourced::Consumer

        consumer do |info|
          info.group_id = 'my-group'
          info.start_from = -> { Time.new(2020, 1, 1) }
        end
      end

      expect(klass.consumer_info.start_from.call).to be_a(Time)
    end

    it 'can be set to an :now which is a 5 second time window' do
      klass = Class.new do
        extend Sourced::Consumer

        consumer do |info|
          info.group_id = 'my-group'
          info.start_from = :now
        end
      end

      now = Time.now
      Timecop.freeze(now) do
        expect(klass.consumer_info.start_from.call).to eq(now - 5)
      end
    end
  end
end
