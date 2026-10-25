# frozen_string_literal: true

require 'spec_helper'
require 'stringio'

RSpec.describe Sourced::GroupUpdater do
  let(:log) { StringIO.new }

  def updater(status, offset_id: nil)
    described_class.new('shop.orders', { status:, error_context: nil }, Logger.new(log), offset_id:)
  end

  it 'logs a transition once its status accepts it' do
    updater('active', offset_id: 3).retry(Time.now + 60)
    updater('active').stop

    expect(log.string).to include('retrying consumer group shop.orders', 'stopping consumer group shop.orders')
  end

  it "doesn't log a transition its status refuses" do
    expect(updater('stopped').retry(Time.now + 60)).to be(false)
    expect(updater('stopped').fail(exception: RuntimeError.new('boom'))).to be(false)
    expect { updater('stopped').stop }.to raise_error(Sourced::Store::InvalidConsumerGroupTransitionError)

    expect(log.string).not_to include('retrying consumer group', 'failing consumer group', 'stopping consumer group')
    expect(log.string).to include('ignoring sourced.consumer_groups.retry', 'ignoring sourced.consumer_groups.fail')
  end

  it 'returns whether a worker command moved the group' do
    expect(updater('active', offset_id: 3).retry(Time.now + 60)).to be(true)
    expect(updater('retrying').fail).to be(true)
  end
end
