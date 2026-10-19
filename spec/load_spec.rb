# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'

module LoadTestMessages
  CourseCreated = Sourced::Message.define('load_test.course.created') do
    attribute :course_id, String
    attribute :title, String
  end

  StudentEnrolled = Sourced::Message.define('load_test.student.enrolled') do
    attribute :course_id, String
    attribute :student_id, String
  end

  AssignmentSubmitted = Sourced::Message.define('load_test.assignment.submitted') do
    attribute :course_id, String
    attribute :student_id, String
    attribute :grade, String
  end
end

class LoadTestDecider < Sourced::Decider
  partition_by :course_id, :student_id
  consumer_group 'load-test-decider'

  context { { enrolled: false, grades: [] } }

  evolve LoadTestMessages::StudentEnrolled do |context, _evt|
    context[:enrolled] = true
  end

  evolve LoadTestMessages::AssignmentSubmitted do |context, evt|
    context[:grades] << evt.payload.grade
  end
end

class LoadTestProjection < Sourced::Projection
  context { { course_id: nil, students: [] } }

  evolve LoadTestMessages::StudentEnrolled do |context, evt|
    context[:course_id] = evt.payload.course_id
    context[:students] << evt.payload.student_id
  end
end

class LoadTestProjector < Sourced::Projector::StateStored
  partition_by :course_id
  consumer_group 'load-test-projector'

  context do
    { course_id: nil, title: nil, student_count: 0 }
  end

  evolve LoadTestMessages::CourseCreated do |context, evt|
    context[:course_id] = evt.payload.course_id
    context[:title] = evt.payload.title
  end

  evolve LoadTestMessages::StudentEnrolled do |context, _evt|
    context[:student_count] += 1
  end
end

RSpec.describe 'Sourced.load' do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::Store.new(db) }

  before { store.install! }

  describe 'loading a Decider' do
    before do
      store.append([
        LoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'joe' }
        ),
        LoadTestMessages::AssignmentSubmitted.new(
          payload: { course_id: 'algebra', student_id: 'joe', grade: 'A' }
        ),
        LoadTestMessages::AssignmentSubmitted.new(
          payload: { course_id: 'algebra', student_id: 'joe', grade: 'B' }
        ),
        # Different partition — should not be loaded
        LoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'jane' }
        )
      ])
    end

    it 'returns an evolved instance and a ReadResult' do
      instance, read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(instance).to be_a(LoadTestDecider)
      expect(read_result).to be_a(Sourced::ReadResult)
    end

    it 'evolves context from matching messages' do
      instance, _read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(instance.context[:enrolled]).to be true
      expect(instance.context[:grades]).to eq(%w[A B])
    end

    it 'sets partition_values on the instance' do
      instance, _read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(instance.partition_values).to eq({ course_id: 'algebra', student_id: 'joe' })
    end

    it 'read_result contains the messages used for evolution' do
      _instance, read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      types = read_result.messages.map(&:type)
      expect(types).to include('load_test.student.enrolled')
      expect(types).to include('load_test.assignment.submitted')
    end

    it 'read_result contains a guard for subsequent appends' do
      _instance, read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(read_result.guard).to be_a(Sourced::PartitionGuard)
      expect(read_result.guard.last_position).to be > 0
    end

    it 'guard can be used for optimistic concurrency on append' do
      instance, read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      # Append with guard succeeds when no conflicts
      new_msg = LoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'C' }
      )
      expect {
        store.append(new_msg, guard: read_result.guard)
      }.not_to raise_error

      # Subsequent append with same guard fails (conflict)
      another = LoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'D' }
      )
      expect {
        store.append(another, guard: read_result.guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    it 'excludes messages from other partitions (AND filtering at SQL level)' do
      instance, read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      # jane's StudentEnrolled shares course_id=algebra but has student_id=jane.
      # AND filtering excludes it at the query level: the message declares
      # student_id, and it doesn't match joe.
      expect(instance.context[:enrolled]).to be true
      expect(instance.context[:grades]).to eq(%w[A B])

      # read_result.messages only contains joe's messages (3 total)
      expect(read_result.messages.size).to eq(3)
      student_ids = read_result.messages
        .select { |m| m.payload.respond_to?(:student_id) }
        .map { |m| m.payload.student_id }
        .uniq
      expect(student_ids).to eq(['joe'])
    end

    it 'guard detects conflicts from concurrent writes to the partition' do
      _instance, read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      # Concurrent write to the same partition
      store.append(LoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'X' }
      ))

      new_msg = LoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'C' }
      )
      expect {
        store.append(new_msg, guard: read_result.guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    describe 'with upto: (partition-local rank)' do
      # joe's partition has 3 matching messages (StudentEnrolled, two AssignmentSubmitted).
      # jane's StudentEnrolled is excluded by AND filtering.
      it 'loads only the first N partition-matching messages' do
        instance, _read_result = Sourced.load(
          LoadTestDecider, store: store, upto: 1,
          course_id: 'algebra', student_id: 'joe'
        )

        expect(instance.context[:enrolled]).to be true
        expect(instance.context[:grades]).to eq([])
      end

      it 'counts partition messages, not global positions (upto: 2 → first 2 matches)' do
        instance, read_result = Sourced.load(
          LoadTestDecider, store: store, upto: 2,
          course_id: 'algebra', student_id: 'joe'
        )

        expect(read_result.messages.size).to eq(2)
        expect(instance.context[:grades]).to eq(%w[A])
      end

      it 'returns all partition messages when upto equals the partition size' do
        instance, read_result = Sourced.load(
          LoadTestDecider, store: store, upto: 3,
          course_id: 'algebra', student_id: 'joe'
        )

        expect(read_result.messages.size).to eq(3)
        expect(instance.context[:grades]).to eq(%w[A B])
      end

      it 'returns all partition messages when upto exceeds the partition size' do
        instance, read_result = Sourced.load(
          LoadTestDecider, store: store, upto: 9999,
          course_id: 'algebra', student_id: 'joe'
        )

        expect(read_result.messages.size).to eq(3)
        expect(instance.context[:grades]).to eq(%w[A B])
      end

      it 'returns empty when upto is 0' do
        instance, read_result = Sourced.load(
          LoadTestDecider, store: store, upto: 0,
          course_id: 'algebra', student_id: 'joe'
        )

        expect(instance.context[:enrolled]).to be false
        expect(read_result.messages).to be_empty
      end

      it 'guard last_position anchors at the last returned message when truncated' do
        _instance, read_result = Sourced.load(
          LoadTestDecider, store: store, upto: 2,
          course_id: 'algebra', student_id: 'joe'
        )

        expect(read_result.guard.last_position).to eq(read_result.messages.last.position)
      end

      it 'guard detects unseen partition messages as conflicts when truncated' do
        _instance, read_result = Sourced.load(
          LoadTestDecider, store: store, upto: 2,
          course_id: 'algebra', student_id: 'joe'
        )

        new_msg = LoadTestMessages::AssignmentSubmitted.new(
          payload: { course_id: 'algebra', student_id: 'joe', grade: 'C' }
        )
        expect {
          store.append(new_msg, guard: read_result.guard)
        }.to raise_error(Sourced::ConcurrentAppendError)
      end
    end
  end

  describe 'loading a Projector' do
    before do
      store.append([
        LoadTestMessages::CourseCreated.new(
          payload: { course_id: 'algebra', title: 'Algebra 101' }
        ),
        LoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'joe' }
        ),
        LoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'jane' }
        ),
        # Different course — should not be loaded
        LoadTestMessages::CourseCreated.new(
          payload: { course_id: 'physics', title: 'Physics 201' }
        )
      ])
    end

    it 'evolves projector context from matching messages' do
      instance, _read_result = Sourced.load(
        LoadTestProjector, store: store,
        course_id: 'algebra'
      )

      expect(instance.context[:title]).to eq('Algebra 101')
      expect(instance.context[:student_count]).to eq(2)
    end

  end

  describe 'loading a Projection' do
    before do
      store.append([
        LoadTestMessages::StudentEnrolled.new(payload: { course_id: 'algebra', student_id: 'joe' }),
        LoadTestMessages::StudentEnrolled.new(payload: { course_id: 'physics', student_id: 'ann' }),
        LoadTestMessages::StudentEnrolled.new(payload: { course_id: 'algebra', student_id: 'jane' })
      ])
    end

    it 'uses the given values as the partition' do
      projection, read_result = Sourced.load(LoadTestProjection, store: store, course_id: 'algebra')

      expect(projection).to be_a(LoadTestProjection)
      expect(projection.context).to eq(course_id: 'algebra', students: %w[joe jane])
      expect(read_result.messages.size).to eq(2)
    end

    it 'evolves over at most upto messages' do
      projection, _read_result = Sourced.load(LoadTestProjection, store: store, upto: 1, course_id: 'algebra')

      expect(projection.context[:students]).to eq(['joe'])
    end
  end

  describe 'empty history' do
    it 'returns instance with initial context when no matching messages' do
      instance, read_result = Sourced.load(
        LoadTestDecider, store: store,
        course_id: 'nonexistent', student_id: 'nobody'
      )

      expect(instance.context[:enrolled]).to be false
      expect(instance.context[:grades]).to eq([])
      expect(read_result.messages).to be_empty
    end
  end
end
