# frozen_string_literal: true

require 'thread'
require 'fileutils'
require 'json'
require 'sourced/array_based_event_store'

module Sourced
  class FileEventStore
    include ArrayBasedEventStore

    def initialize(dir: ".", event_registry: Sourced::Event)
      @mutex = Mutex.new
      @dir = dir
      @file_name = File.join(@dir, 'events')
      @event_registry = event_registry
    end

    def append_to_stream(stream_id, evts, expected_seq: nil)
      evts = [evts].flatten
      return evts unless evts.any?

      validate_stream_ids!(stream_id, evts)

      with_sequence_constraint(evts.last, expected_seq) do
        encoded = evts.map do |e|
          JSON.generate(e.to_h)
        end
        append_to_file(encoded)
        evts
      end
    end

    private
    attr_reader :mutex, :event_registry

    def events
      if File.exists?(@file_name)
        f = File.new(@file_name)
        f.each_line.map do |line|
          data = JSON.parse(line, symbolize_names: true)
          event_registry.from(data)
        end
      else
        []
      end
    end

    def append_to_file(encoded)
      mutex.synchronize {
        create_file
        File.open(@file_name, 'a') do |f|
          encoded.each do |evt|
            f.puts evt
          end
        end
      }
    end

    def create_file
      FileUtils.mkdir_p @dir
      FileUtils.touch(@file_name) unless File.exists?(@file_name)
    end
  end
end
