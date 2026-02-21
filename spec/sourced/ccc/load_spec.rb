# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sequel'

module CCCLoadTestMessages
  CourseCreated = Sourced::CCC::Message.define('load_test.course.created') do
    attribute :course_id, String
    attribute :title, String
  end

  StudentEnrolled = Sourced::CCC::Message.define('load_test.student.enrolled') do
    attribute :course_id, String
    attribute :student_id, String
  end

  AssignmentSubmitted = Sourced::CCC::Message.define('load_test.assignment.submitted') do
    attribute :course_id, String
    attribute :student_id, String
    attribute :grade, String
  end
end

class LoadTestDecider < Sourced::CCC::Decider
  partition_by :course_id, :student_id
  consumer_group 'load-test-decider'

  state { |_| { enrolled: false, grades: [] } }

  evolve CCCLoadTestMessages::StudentEnrolled do |state, _evt|
    state[:enrolled] = true
  end

  evolve CCCLoadTestMessages::AssignmentSubmitted do |state, evt|
    state[:grades] << evt.payload.grade
  end
end

class LoadTestProjector < Sourced::CCC::Projector
  partition_by :course_id
  consumer_group 'load-test-projector'

  state do |(course_id)|
    { course_id: course_id, title: nil, student_count: 0 }
  end

  evolve CCCLoadTestMessages::CourseCreated do |state, evt|
    state[:title] = evt.payload.title
  end

  evolve CCCLoadTestMessages::StudentEnrolled do |state, _evt|
    state[:student_count] += 1
  end
end

RSpec.describe 'Sourced::CCC.load' do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::CCC::Store.new(db) }

  before { store.install! }

  describe 'loading a Decider' do
    before do
      store.append([
        CCCLoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'joe' }
        ),
        CCCLoadTestMessages::AssignmentSubmitted.new(
          payload: { course_id: 'algebra', student_id: 'joe', grade: 'A' }
        ),
        CCCLoadTestMessages::AssignmentSubmitted.new(
          payload: { course_id: 'algebra', student_id: 'joe', grade: 'B' }
        ),
        # Different partition — should not be loaded
        CCCLoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'jane' }
        )
      ])
    end

    it 'returns an evolved instance and a ReadResult' do
      instance, read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(instance).to be_a(LoadTestDecider)
      expect(read_result).to be_a(Sourced::CCC::ReadResult)
    end

    it 'evolves state from matching messages' do
      instance, _read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(instance.state[:enrolled]).to be true
      expect(instance.state[:grades]).to eq(%w[A B])
    end

    it 'sets partition_values on the instance' do
      instance, _read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(instance.partition_values).to eq(%w[algebra joe])
    end

    it 'read_result contains the messages used for evolution' do
      _instance, read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      types = read_result.messages.map(&:type)
      expect(types).to include('load_test.student.enrolled')
      expect(types).to include('load_test.assignment.submitted')
    end

    it 'read_result contains a guard for subsequent appends' do
      _instance, read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      expect(read_result.guard).to be_a(Sourced::CCC::ConsistencyGuard)
      expect(read_result.guard.last_position).to be > 0
    end

    it 'guard can be used for optimistic concurrency on append' do
      instance, read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      # Append with guard succeeds when no conflicts
      new_msg = CCCLoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'C' }
      )
      expect {
        store.append(new_msg, guard: read_result.guard)
      }.not_to raise_error

      # Subsequent append with same guard fails (conflict)
      another = CCCLoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'D' }
      )
      expect {
        store.append(another, guard: read_result.guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    it 'excludes messages from other partitions (AND filtering at SQL level)' do
      instance, read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      # jane's StudentEnrolled shares course_id=algebra but has student_id=jane.
      # AND filtering excludes it at the query level: the message declares
      # student_id, and it doesn't match joe.
      expect(instance.state[:enrolled]).to be true
      expect(instance.state[:grades]).to eq(%w[A B])

      # read_result.messages only contains joe's messages (3 total)
      expect(read_result.messages.size).to eq(3)
      student_ids = read_result.messages
        .select { |m| m.payload.respond_to?(:student_id) }
        .map { |m| m.payload.student_id }
        .uniq
      expect(student_ids).to eq(['joe'])
    end

    it 'guard detects conflicts from concurrent writes to the partition' do
      _instance, read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'algebra', student_id: 'joe'
      )

      # Concurrent write to the same partition
      store.append(CCCLoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'X' }
      ))

      new_msg = CCCLoadTestMessages::AssignmentSubmitted.new(
        payload: { course_id: 'algebra', student_id: 'joe', grade: 'C' }
      )
      expect {
        store.append(new_msg, guard: read_result.guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end
  end

  describe 'loading a Projector' do
    before do
      store.append([
        CCCLoadTestMessages::CourseCreated.new(
          payload: { course_id: 'algebra', title: 'Algebra 101' }
        ),
        CCCLoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'joe' }
        ),
        CCCLoadTestMessages::StudentEnrolled.new(
          payload: { course_id: 'algebra', student_id: 'jane' }
        ),
        # Different course — should not be loaded
        CCCLoadTestMessages::CourseCreated.new(
          payload: { course_id: 'physics', title: 'Physics 201' }
        )
      ])
    end

    it 'evolves projector state from matching messages' do
      instance, _read_result = Sourced::CCC.load(
        LoadTestProjector, store: store,
        course_id: 'algebra'
      )

      expect(instance.state[:title]).to eq('Algebra 101')
      expect(instance.state[:student_count]).to eq(2)
    end

    it 'passes partition values to state initializer' do
      instance, _read_result = Sourced::CCC.load(
        LoadTestProjector, store: store,
        course_id: 'algebra'
      )

      expect(instance.state[:course_id]).to eq('algebra')
    end
  end

  describe 'empty history' do
    it 'returns instance with initial state when no matching messages' do
      instance, read_result = Sourced::CCC.load(
        LoadTestDecider, store: store,
        course_id: 'nonexistent', student_id: 'nobody'
      )

      expect(instance.state[:enrolled]).to be false
      expect(instance.state[:grades]).to eq([])
      expect(read_result.messages).to be_empty
    end
  end
end
