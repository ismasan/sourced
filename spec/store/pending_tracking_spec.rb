# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'
require 'timecop'

module PendingTrackingMessages
  Joined = Sourced::Message.define('pending_tracking.joined') do
    reference :course_name, String
    reference :user_id, String
  end

  Closed = Sourced::Message.define('pending_tracking.closed') do
    reference :course_name, String
  end

  Noted = Sourced::Message.define('pending_tracking.noted') do
    attribute :text, String
  end

  Unclaimed = Sourced::Message.define('pending_tracking.unclaimed') do
    reference :course_name, String
    reference :user_id, String
  end

  Job = Sourced::Message.define('pending_tracking.job') do
    reference :name, String
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

    it 'marks each partition of a bulk append at its first message' do
      positions = store.append(Array.new(600) { |i| joined('Algebra', "u#{i}") } + [joined('Algebra', 'u0')])

      marks = pending
      expect(marks.size).to eq(600)
      expect(marks['course_name:Algebra|user_id:u0']).to eq(positions - 600)
      expect(marks['course_name:Algebra|user_id:u599']).to eq(positions - 1)
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
      expect(claim).to be_nil
      expect(pending).to eq(joe => nil)
    end
  end

  describe 'a group taken with new types' do
    let(:wider) do
      Sourced::Selector[
        PendingTrackingMessages::Joined, PendingTrackingMessages::Closed, PendingTrackingMessages::Noted,
        PendingTrackingMessages::Unclaimed
      ].by(:course_name, :user_id)
    end
    # Another deploy's process, whose reactor claims one more type
    let(:new_deploy) { described_class.new(db).tap { _1.register_consumer_group(group_id, wider) } }

    def unclaimed(course_name, user_id)
      PendingTrackingMessages::Unclaimed.new(payload: { course_name:, user_id: })
    end

    def new_claim = new_deploy.claim_next(group_id, wider, worker_id: 'w-2')

    it "keeps running under its rule when another deploy's process registers it" do
      store.append(joined('Algebra', 'joe'))
      new_deploy

      expect(new_claim).to be_nil
      expect(claim).not_to be_nil
    end

    it 'marks messages of the new types already in the log' do
      store.append(joined('Algebra', 'joe'))
      drain
      position = store.append(unclaimed('Algebra', 'joe'))
      expect(pending).to eq(joe => nil)

      expect(new_deploy.reclaim_consumer_groups).to eq([group_id])

      expect(pending).to eq(joe => position)
      expect(new_claim.messages.map(&:position)).to eq([position])
      expect(new_deploy.reclaim_consumer_groups).to eq([])
    end

    it 'is claimed only by workers with the rule it runs under' do
      store.append(joined('Algebra', 'joe'))
      new_deploy.reclaim_consumer_groups

      expect(claim).to be_nil
      expect(new_claim).not_to be_nil
    end

    it 'is taken back by the deploy whose workers still run' do
      store.append(joined('Algebra', 'joe'))
      new_deploy.reclaim_consumer_groups
      position = new_deploy.append(unclaimed('Algebra', 'joe'))
      store.reclaim_consumer_groups # an old worker process, restarted

      expect(new_claim).to be_nil
      expect(new_deploy.reclaim_consumer_groups).to eq([group_id])
      expect(new_claim.messages.map(&:position)).to include(position)
    end

    it 'keeps a partition pending when a claim from before it was taken is acked' do
      store.append(joined('Algebra', 'joe'))
      c = claim
      new_deploy.reclaim_consumer_groups
      position = new_deploy.append(unclaimed('Algebra', 'joe'))

      ack(c)

      expect(pending[joe]).to eq(c.messages.last.position)
      expect(new_claim.messages.map(&:position)).to eq([position])
    end
  end

  it 'claims past partitions marked with nothing pending' do
    store.append(joined('Physics', 'ann'))
    c = claim
    described_class.new(db).ack(group_id, offset_id: c.offset_id, version: c.offset_version, position: c.messages.last.position)
    position = store.append(joined('Algebra', 'joe'))
    expect(pending).to eq(ann => c.messages.last.position, joe => position)

    expect(claim.partition_key).to eq(joe)
    expect(pending[ann]).to be_nil
  end

  describe 'a partition a claim reads nothing from' do
    let(:logger) { instance_double('Logger', info: nil, warn: nil, debug: nil, error: nil) }
    let(:logged) { described_class.new(db, logger:).tap { _1.register_consumer_group(group_id, selector) } }

    def logged_claim = logged.claim_next(group_id, selector, worker_id: 'w-1')

    it "isn't tried again in the same claim, and is logged when its pending check and the read disagree" do
      p1 = store.append(joined('Algebra', 'joe'))
      ack(claim)
      db[:sourced_offsets].update(next_position: p1) # a false positive
      p2 = store.append(joined('Physics', 'ann'))
      # A pending check that disagrees with the read: it finds ann's message in joe's partition.
      allow(logged).to receive(:pending_position_sql).and_return(Sequel.lit(p2.to_s))

      expect(logged_claim.partition_key).to eq(ann)
      expect(pending[joe]).to eq(p2)
      expect(logger).to have_received(:error).with(/partition #{Regexp.escape(joe)} is marked pending at #{p2}, but a claim reads nothing after #{p1}/).once
    end

    it "isn't logged for a message appended since the read" do
      p1 = store.append(joined('Algebra', 'joe'))
      ack(claim)
      db[:sourced_offsets].update(next_position: p1)
      p2 = nil
      allow(logged).to receive(:check_empty_claim_mark).and_wrap_original do |m, *args|
        p2 = store.append(joined('Algebra', 'joe')) # by the time the empty claim's mark is checked
        m.call(*args)
      end

      expect(logged_claim).to be_nil
      expect(pending[joe]).to eq(p2)
      expect(logger).not_to have_received(:error)
    end
  end

  it "agrees with the claims' reads on every partition's first pending message, over a mixed workload" do
    logger = instance_double('Logger', info: nil, warn: nil, debug: nil, error: nil)
    rng = Random.new(7)
    courses = %w[Algebra Physics]
    users = %w[joe jake ann]
    wider = Sourced::Selector[
      PendingTrackingMessages::Joined, PendingTrackingMessages::Closed, PendingTrackingMessages::Noted,
      PendingTrackingMessages::Unclaimed
    ].by(:course_name, :user_id)
    # A deploy whose reactor claims one more type, which takes the group halfway through
    new_deploy = described_class.new(db, logger:).tap { _1.register_consumer_group(group_id, wider) }
    current = [store, selector]
    workload = lambda do |steps|
      steps.times do
        course = courses.sample(random: rng)
        user = users.sample(random: rng)
        case rng.rand(7)
        when 0, 1 then store.append(joined(course, user))
        when 2 then store.append(closed(course)) # partial key
        when 3 then store.append(PendingTrackingMessages::Noted.new(payload: { text: 'x' }).with_tags(course_name: course)) # partial key, by tag
        when 4 then store.append(PendingTrackingMessages::Noted.new(payload: { text: 'x' }).with_tags(course_name: course, user_id: user))
        when 5 then store.append(PendingTrackingMessages::Unclaimed.new(payload: { course_name: course, user_id: user }))
        else
          st, sel = current
          c = st.claim_next(group_id, sel, worker_id: 'w-1', batch_size: rng.rand(1..3))
          next unless c

          position = c.messages.take(rng.rand(1..c.messages.size)).last.position # partial acks too
          st.ack(group_id, offset_id: c.offset_id, version: c.offset_version, position:)
        end
      end
    end

    workload.(150)
    store.stop_consumer_group(group_id)
    store.reset_consumer_group(group_id) # replay boundaries
    store.start_consumer_group(group_id)
    workload.(80)
    new_deploy.reclaim_consumer_groups # the group takes a new type
    current = [new_deploy, wider]
    workload.(80)
    while (c = new_deploy.claim_next(group_id, wider, worker_id: 'w-1'))
      new_deploy.ack(group_id, offset_id: c.offset_id, version: c.offset_version, position: c.messages.last.position)
    end
    store.append([joined('Algebra', 'joe'), closed('Physics')]) # some partitions pending, others caught up

    # Every partition a candidate: a claim reads each one, its ack at the cursor recomputes the mark.
    db[:sourced_offsets].update(next_position: 0)
    claims = []
    while (c = new_deploy.claim_next(group_id, wider, worker_id: 'w-1', batch_size: 1))
      claims << c
    end
    unclaimed = pending.reject { |key, _| claims.map(&:partition_key).include?(key) }
    expect(unclaimed.values).to all(be_nil)

    cursors = store.read_offsets(group_id:, limit: 1000).offsets.to_h { |o| [o[:partition_key], o[:last_position]] }
    claims.each do |c|
      new_deploy.ack(group_id, offset_id: c.offset_id, version: c.offset_version, position: cursors[c.partition_key])
    end
    expect(pending.slice(*claims.map(&:partition_key))).to eq(claims.to_h { [_1.partition_key, _1.messages.first.position] })
    expect(claims.size).to be > 3
    expect(unclaimed.size).to be > 0
    expect(logger).not_to have_received(:error)
  end

  it 'marks what is pending when a group without a stored rule (an upgraded database) registers' do
    store.append(joined('Physics', 'ann'))
    position = store.append(joined('Algebra', 'joe'))
    expect(claim.tap { ack(_1) }.partition_key).to eq(ann)
    db[:sourced_offsets].update(next_position: nil)
    db[:sourced_consumer_groups].update(claim_digest: nil)

    described_class.new(db).register_consumer_group(group_id, selector)

    expect(pending).to eq(joe => position, ann => nil)
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

  describe 'sweep_pending' do
    # A store that doesn't register the group: its appends mark nothing.
    let(:unregistered) { described_class.new(db) }

    before do
      store.append([joined('Algebra', 'joe'), joined('Algebra', 'jake'), joined('Physics', 'ann')])
      drain
      store.sweep_pending # starts at the log's end
    end

    it "marks the partitions of messages appended since, that nothing marked" do
      position = unregistered.append(joined('Algebra', 'joe'))
      fanout = unregistered.append(closed('Physics'))
      expect(pending.values).to all(be_nil)

      expect(store.sweep_pending).to eq(group_id => 2)

      expect(pending).to eq(joe => position, jake => nil, ann => fanout)
      expect(store.sweep_pending).to eq({})
    end

    it 'leaves claimed partitions, and those whose cursor is past the message' do
      unregistered.append([joined('Algebra', 'joe'), joined('Physics', 'ann')])
      store.append(joined('Algebra', 'jake')) # marked by its append
      ack(claim) # jake's: caught up
      db[:sourced_offsets].where(partition_key: ann).update(claimed: 1)
      db[:sourced_offsets].where(partition_key: joe).update(last_position: db[:sourced_messages].max(:position))

      expect(store.sweep_pending).to eq({})
      expect(pending).to eq(joe => nil, jake => nil, ann => nil)
    end

    it 'checks up to limit positions further on each call' do
      positions = 3.times.map { |i| unregistered.append(joined('Algebra', "u#{i}")) }
      %w[u0 u1 u2].each { |u| store.append(joined('Algebra', u)) } # offsets for them
      db[:sourced_offsets].update(next_position: nil, last_position: 0)

      expect(store.sweep_pending(limit: 2)).to eq(group_id => 2)
      expect(store.sweep_pending(limit: 2)).to eq(group_id => 1)
      expect(pending.values_at(*%w[u0 u1 u2].map { "course_name:Algebra|user_id:#{_1}" })).to eq(positions)
    end

    it 'leaves a group whose row has another rule to the store that took it' do
      unregistered.append(joined('Algebra', 'joe'))
      db[:sourced_consumer_groups].update(claim_digest: 'another-deploy')

      expect(store.sweep_pending).to eq({})
      expect(pending[joe]).to be_nil
    end

    it 'runs in maintain, which warns about what it marked' do
      logger = instance_double('Logger', info: nil, warn: nil, debug: nil)
      logged = described_class.new(db, logger:).tap { _1.register_consumer_group(group_id, selector) }
      logged.maintain
      unregistered.append(joined('Algebra', 'joe'))

      logged.maintain

      expect(logger).to have_received(:warn).with(/marked 1 partitions of pending-tracking with pending work nothing had marked/).once
      expect(pending[joe]).not_to be_nil
    end
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
