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
    specify 'default is :beginning' do
      expect(TestConsumer::TestConsumer.consumer_info.start_from.call).to eq(:beginning)
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

    it 'can be set to an Integer sequence number' do
      klass = Class.new do
        extend Sourced::Consumer

        consumer do |info|
          info.group_id = 'my-group'
          info.start_from = 100
        end
      end

      expect(klass.consumer_info.start_from.call).to eq(100)
    end
  end
end
