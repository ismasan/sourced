# frozen_string_literal: true

require 'json'
require_relative 'message'

class EventStore
  def initialize(db)
    @db = db
  end

  def append(events)
    rows = events.map { |e| serialize(e) }
    db[:events].multi_insert(rows)
  end

  def read_stream(stream_id)
    db[:events].where(stream_id:).order(:global_seq).map do |row|
      deserialize(row)
    end
  end

  private

  attr_reader :db

  def serialize(event)
    row = event.to_h
    row[:payload] = JSON.dump(row[:payload]) if row[:payload]
    row
  end

  def deserialize(row)
    Message.from(row)
  end
end
