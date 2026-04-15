# frozen_string_literal: true

require_relative 'domain'
require 'sinatra/base'
require 'json'
require 'securerandom'

class App < Sinatra::Base
  set :default_content_type, 'application/json'

  helpers do
    def json_body
      JSON.parse(request.body.read, symbolize_names: true)
    rescue JSON::ParserError
      halt 400, { error: 'Invalid JSON' }.to_json
    end
  end

  # Generic command endpoint
  post '/commands' do
    data = json_body
    message = CourseApp::Command.from(data.merge(metadata: { source: 'http' }))

    unless message.valid?
      halt 422, message.errors.to_json
    end

    Sourced.store.append([message])

    status 202
    { id: message.id, type: message.type }.to_json

  rescue Sourced::UnknownMessageError => e
    halt 422, { error: e.message }.to_json
  end

  # List all courses
  get '/' do
    courses = CourseApp::CourseCatalogProjector.all_courses.map do |c|
      { course_id: c[:course_id], course_name: c[:course_name], student_count: c[:student_count] }
    end
    courses.to_json
  end

  # Create a course
  post '/courses' do
    body = json_body
    course_id = SecureRandom.uuid

    cmd = CourseApp::CreateCourse.new(payload: { course_id: course_id, course_name: body[:course_name] })
    cmd, _decider, _events = Sourced.handle!(CourseApp::CourseDecider, cmd)

    halt 422, cmd.errors.to_json unless cmd.valid?

    status 201
    { course_id: course_id, course_name: cmd.payload.course_name }.to_json

  rescue RuntimeError => e
    halt 422, { error: e.message }.to_json
  rescue Sourced::ConcurrentAppendError
    halt 409, { error: 'Concurrent modification — please retry' }.to_json
  end

  # Course detail
  get '/courses/:id' do
    course = CourseApp::CourseCatalogProjector.read_course(params[:id])

    halt 404, { error: 'Course not found' }.to_json unless course

    course.to_json
  end

  # Enrol a student
  post '/courses/:id/enrolments' do
    body = json_body

    cmd = CourseApp::EnrolStudent.new(payload: {
      course_id: params[:id],
      student_id: body[:student_id]
    })
    cmd, _decider, _events = Sourced.handle!(CourseApp::EnrolmentDecider, cmd)

    halt 422, cmd.errors.to_json unless cmd.valid?

    status 201
    { course_id: cmd.payload.course_id, student_id: cmd.payload.student_id }.to_json

  rescue RuntimeError => e
    halt 422, { error: e.message }.to_json
  rescue Sourced::ConcurrentAppendError
    halt 409, { error: 'Concurrent modification — please retry' }.to_json
  end
end
