# frozen_string_literal: true

require 'bundler/setup'
require 'sourced'
require 'sourced'
require 'sequel'
require 'fileutils'
require 'json'


module CourseApp
  # --- Messages ---

  class Event < Sourced::Message; end
  class Command < Sourced::Message; end

  CreateCourse    = Command.define('courses.create')   { attribute :course_id, String; attribute :course_name, String }
  CourseCreated   = Event.define('courses.created')   { attribute :course_id, String; attribute :course_name, String }
  EnrolStudent    = Command.define('courses.enrol')     { attribute :course_id, String; attribute :student_id, String }
  StudentEnrolled = Event.define('courses.enrolled')  { attribute :course_id, String; attribute :student_id, String }

  # --- Deciders ---

  # Enforces course name uniqueness.
  # Partition by :course_name so Sourced.load reads all CourseCreated with that name.
  class CourseDecider < Sourced::Decider
    partition_by :course_name

    state do |_partition_values|
      { name_taken: false }
    end

    evolve CourseCreated do |state, _event|
      state[:name_taken] = true
    end

    command CreateCourse do |state, cmd|
      raise "Course '#{cmd.payload.course_name}' already exists" if state[:name_taken]

      event CourseCreated, course_id: cmd.payload.course_id, course_name: cmd.payload.course_name
    end
  end

  # Enforces enrolment rules: course must exist, no duplicates, max 20 students.
  # Partition by :course_id so Sourced.load reads CourseCreated + StudentEnrolled for that course.
  class EnrolmentDecider < Sourced::Decider
    partition_by :course_id

    state do |_partition_values|
      { course_exists: false, student_ids: [], student_count: 0 }
    end

    evolve CourseCreated do |state, _event|
      state[:course_exists] = true
    end

    evolve StudentEnrolled do |state, event|
      state[:student_ids] << event.payload.student_id
      state[:student_count] += 1
    end

    command EnrolStudent do |state, cmd|
      raise "Course '#{cmd.payload.course_id}' does not exist" unless state[:course_exists]
      raise "Student '#{cmd.payload.student_id}' is already enrolled" if state[:student_ids].include?(cmd.payload.student_id)
      raise "Course is full (max 20 students). Has #{state[:student_count]}" if state[:student_count] >= 20

      event StudentEnrolled,
        course_id: cmd.payload.course_id,
        student_id: cmd.payload.student_id
    end
  end

  # --- Projector (async read model) ---

  # Builds a file-backed course catalog from events.
  # Each course is written to a JSON file in storage/projections/.
  # Registered with Sourced for background processing by workers.
  class CourseCatalogProjector < Sourced::Projector::EventSourced
    partition_by :course_id

    PROJECTIONS_DIR = File.join(__dir__, 'storage', 'projections')

    class << self
      def projection_path(course_id)
        File.join(PROJECTIONS_DIR, "#{course_id}.json")
      end

      def read_course(course_id)
        path = projection_path(course_id)
        return nil unless File.exist?(path)

        JSON.parse(File.read(path), symbolize_names: true)
      end

      def all_courses
        Dir.glob(File.join(PROJECTIONS_DIR, '*.json')).filter_map do |path|
          JSON.parse(File.read(path), symbolize_names: true)
        rescue JSON::ParserError
          nil
        end
      end
    end

    state do |partition_values|
      { course_id: nil, course_name: nil, students: [] }
    end

    evolve CourseCreated do |state, event|
      state[:course_id] = event.payload.course_id
      state[:course_name] = event.payload.course_name
    end

    evolve StudentEnrolled do |state, event|
      state[:students] << event.payload.student_id
    end

    sync do |state:, messages:, **|
      next unless state[:course_id]

      FileUtils.mkdir_p(PROJECTIONS_DIR)
      data = {
        course_id: state[:course_id],
        course_name: state[:course_name],
        students: state[:students],
        student_count: state[:students].size
      }
      File.write(self.class.projection_path(state[:course_id]), JSON.pretty_generate(data))
    end
  end

  # --- Configuration ---

  DB_PATH = File.join(__dir__, 'storage', 'ccc_app.db')

  Sourced.configure do |c|
    c.store = Sequel.sqlite(DB_PATH)
  end

  Sourced.register(CourseDecider)
  Sourced.register(EnrolmentDecider)
  Sourced.register(CourseCatalogProjector)
end
