# frozen_string_literal: true

require 'spec_helper'

module ConsumerGroupTest
  module Connections
    class LinkedIn
      extend Sourced::Consumer
    end
  end

  class Declared
    extend Sourced::Consumer

    consumer_group 'ConsumerGroupTest::Renamed-Group'
  end
end

RSpec.describe Sourced::Consumer do
  describe '.group_id' do
    it 'defaults to the class name, lowercase and dot-separated' do
      expect(ConsumerGroupTest::Connections::LinkedIn.group_id).to eq('consumer_group_test.connections.linked_in')
    end

    it 'normalizes a declared consumer_group' do
      expect(ConsumerGroupTest::Declared.group_id).to eq('consumer_group_test.renamed_group')
    end

    it "raises for a consumer_group that can't be normalized" do
      klass = Class.new { extend Sourced::Consumer }

      expect { klass.consumer_group('cross post') }.to raise_error(ArgumentError, /invalid consumer_group "cross post"/)
      expect { klass.consumer_group('') }.to raise_error(ArgumentError, /invalid consumer_group ""/)
    end

    it 'is nil for an anonymous class with no declared group' do
      expect(Class.new { extend Sourced::Consumer }.group_id).to be_nil
    end
  end

  describe '.group_label' do
    it 'is the class name, whatever the group_id' do
      expect(ConsumerGroupTest::Connections::LinkedIn.group_label).to eq('ConsumerGroupTest::Connections::LinkedIn')
      expect(ConsumerGroupTest::Declared.group_label).to eq('ConsumerGroupTest::Declared')
    end

    it 'is the group_id for an anonymous class' do
      klass = Class.new { extend Sourced::Consumer }
      klass.consumer_group 'anonymous.group'

      expect(klass.group_label).to eq('anonymous.group')
    end
  end
end

RSpec.describe Sourced::Types::GroupId do
  it 'normalizes module names and loosely written ids' do
    expect(described_class.parse('CrossPost::Connections::LinkedIn')).to eq('cross_post.connections.linked_in')
    expect(described_class.parse('device-decider')).to eq('device_decider')
    expect(described_class.parse('cross_post.account')).to eq('cross_post.account')
  end

  it "rejects what can't be normalized" do
    ['', 'cross post', 'cross..post', '.cross', nil].each do |id|
      expect(described_class.resolve(id)).not_to be_valid
    end
  end
end
