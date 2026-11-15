# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'
require 'timecop'

module PendingTrackingMessages
  Joined = Sourced::Message.define('pending_tracking.joined') do
    attribute :course_name, String
    attribute :user_id, String
  end

  Closed = Sourced::Message.define('pending_tracking.closed') do
    attribute :course_name, String
  end

  Noted = Sourced::Message.define('pending_tracking.noted') do
    attribute :text, String
  end

  Unclaimed = Sourced::Message.define('pending_tracking.unclaimed') do
    attribute :course_name, String
    attribute :user_id, String
  end

  Job = Sourced::Message.define('pending_tracking.job') do
    attribute :name, String
  end
end

RSpec.describe Sourced::Store, 'offsets next_position' do
  include PendingTrackingMessages

  let(:db) { Sequel.sqlite }
  let(:store) { described_class.new(db) }
  let(:group_id) { 'pending-tracking' }
  let(:selector) do
    Sourced::Selector[
      PendingTrackingMessages::Joined, PendingTrackingMessages::Closed, PendingTrackingMessages::Noted
    ].by(:course_name, :user_id)
  end

  before do
    store.install!
    store.register_consumer_group(group_id, selector)
  end

  def joined(course_name, user_id) = PendingTrackingMessages::Joined.new(payload: { course_name:, user_id: })
  def closed(course_name) = PendingTrackingMessages::Closed.new(payload: { course_name: })

  # { partition_key => next_position } for the group
  def pending(group = group_id)
    store.read_offsets(group_id: group, limit: 1000).offsets.to_h { |o| [o[:partition_key], o[:next_position]] }
  end

  def claim(batch_size: nil) = store.claim_next(group_id, selector, worker_id: 'w-1', batch_size:)

  def ack(claim, position: claim.messages.last.position)
    store.ack(group_id, offset_id: claim.offset_id, version: claim.offset_version, position:)
  end

  def drain
    while (c = claim)
      ack(c)
    end
  end

  let(:joe) { 'course_name:Algebra|user_id:joe' }
  let(:jake) { 'course_name:Algebra|user_id:jake' }
  let(:ann) { 'course_name:Physics|user_id:ann' }

  describe 'appends' do
    it "marks a message's partition at its position, and keeps the first pending one" do
      first = store.append(joined('Algebra', 'joe'))
      store.append(joined('Algebra', 'joe'))

      expect(pending).to eq(joe => first)
    end

    it 'marks every partition a message carrying some of the keys is in, and only those' do
      store.append([joined('Algebra', 'joe'), joined('Algebra', 'jake'), joined('Physics', 'ann')])
      drain
      expect(pending.values).to all(be_nil)

      position = store.append(closed('Algebra'))

      expect(pending).to eq(joe => position, jake => position, ann => nil)
    end

    it "doesn't mark for messages of types the group doesn't claim" do
      store.append(joined('Algebra', 'joe'))
      drain

      store.append(PendingTrackingMessages::Unclaimed.new(payload: { course_name: 'Algebra', user_id: 'joe' }))

      expect(pending).to eq(joe => nil)
    end

    it 'marks through tags' do
      store.append(joined('Algebra', 'joe'))
      drain

      position = store.append(PendingTrackingMessages::Noted.new(payload: { text: 'hi' }).with_tags(course_name: 'Algebra'))

      expect(pending).to eq(joe => position)
    end

    it 'marks scheduled messages when they are promoted' do
      store.append(joined('Algebra', 'joe'))
      drain

      store.append(joined('Algebra', 'joe').at(Time.now + 60))
      expect(pending).to eq(joe => nil)

      Timecop.travel(Time.now + 61) { store.update_schedule! }

      expect(pending[joe]).to eq(db[:sourced_messages].max(:position))
    end
  end

  describe 'acks' do
    it 'clear it once the partition is caught up' do
      store.append([joined('Algebra', 'joe'), joined('Algebra', 'joe')])
      ack(claim)

      expect(pending).to eq(joe => nil)
    end

    it 'move it to the next pending message after a partial batch' do
      p1 = store.append(joined('Algebra', 'joe'))
      store.append(joined('Algebra', 'jake'))
      p3 = store.append(closed('Algebra'))

      c = claim(batch_size: 1)
      expect(c.messages.map(&:position)).to eq([p1])
      ack(c)

      expect(pending[joe]).to eq(p3)
    end

    it 'see a message appended between the claim and the ack' do
      store.append(joined('Algebra', 'joe'))
      c = claim
      later = store.append(joined('Algebra', 'joe'))
      ack(c)

      expect(pending).to eq(joe => later)
    end

    it "leave it alone when the claim's version is stale" do
      first = store.append(joined('Algebra', 'joe'))
      c = claim
      store.release(group_id, offset_id: c.offset_id, version: c.offset_version)
      expect(claim.offset_version).to be > c.offset_version
      ack(c)

      expect(pending).to eq(joe => first)
    end

    it "keep it pending, at the acked position, from a store that doesn't know the group's types" do
      store.append([joined('Algebra', 'joe'), joined('Algebra', 'joe')])
      c = claim
      other = described_class.new(db)

      other.ack(group_id, offset_id: c.offset_id, version: c.offset_version, position: c.messages.last.position)

      expect(pending).to eq(joe => c.messages.last.position)
    end
  end

  it 'releasing a claim keeps it' do
    first = store.append(joined('Algebra', 'joe'))
    c = claim
    store.release(group_id, offset_id: c.offset_id, version: c.offset_version)

    expect(pending).to eq(joe => first)
  end

  it 'reset marks the partitions it rewinds' do
    store.append([joined('Algebra', 'joe'), joined('Physics', 'ann')])
    drain
    store.stop_consumer_group(group_id)

    store.reset_consumer_group(group_id)

    expect(pending).to eq(joe => 0, ann => 0)
  end

  describe 'advance_offset' do
    it 'recomputes it past the advanced position' do
      p1 = store.append(joined('Algebra', 'joe'))
      p2 = store.append(joined('Algebra', 'joe'))

      store.advance_offset(group_id, partition: { 'course_name' => 'Algebra', 'user_id' => 'joe' }, position: p1)

      expect(pending).to eq(joe => p2)
    end
  end

  it 'is set on every partition a claim finds work in, over a mixed workload' do
    rng = Random.new(42)
    courses = %w[Algebra Physics]
    users = %w[joe jake ann]
    claims = []

    200.times do
      case rng.rand(4)
      when 0, 1
        store.append(joined(courses.sample(random: rng), users.sample(random: rng)))
      when 2
        store.append(closed(courses.sample(random: rng)))
      else
        before = pending
        c = claim(batch_size: rng.rand(1..3))
        next unless c

        expect(before[c.partition_key]).not_to be_nil
        claims << c
        rng.rand(3).zero? ? store.release(group_id, offset_id: c.offset_id, version: c.offset_version) : ack(c)
      end
    end
    before = pending
    while (c = claim)
      expect(before[c.partition_key]).not_to be_nil
      ack(c)
      before = pending
    end

    expect(claims.size).to be > 20
    expect(pending.values).to all(be_nil)
  end

  describe 'discovery' do
    it 'sets it on the offsets it creates for messages appended before the group registered' do
      first = store.append(joined('Algebra', 'joe'))
      store.append(closed('Algebra'))
      store.register_consumer_group('late', selector)

      store.claim_next('late', selector, worker_id: 'w-1')

      expect(pending('late')).to eq(joe => first)
    end

    it 'sets it on id-partitioned queue offsets, and ack_and_delete clears it' do
      queue = Sourced::Selector[PendingTrackingMessages::Job].by_message_id
      store.register_consumer_group('jobs', queue, exclusive: true)
      position = store.append(PendingTrackingMessages::Job.new(payload: { name: 'a' }))

      c = store.claim_next('jobs', queue, worker_id: 'w-1')
      expect(pending('jobs').values).to eq([position])

      store.ack_and_delete('jobs', offset_id: c.offset_id, version: c.offset_version, positions: [position])
      expect(pending('jobs').values).to eq([nil])
    end
  end
end
