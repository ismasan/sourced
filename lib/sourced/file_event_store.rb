require 'thread'
require 'fileutils'
require 'json'
require 'sourced/by_aggregate_id'

module Sourced
  class FileEventStore
    include ByAggregateId

    def initialize(dir = ".")
      @mutex = Mutex.new
      @dir = dir
      @file_name = File.join(@dir, 'events')
    end

    def append(events)
      events = Array(events)
      encoded = events.map do |e|
        JSON.generate(e.to_h)
      end
      append_to_file(encoded)
      events
    end

    private
    attr_reader :mutex

    def events
      if File.exists?(@file_name)
        Enumerator.new do |yielder|
          f = File.new(@file_name)
          f.each_line.map do |line|
            data = JSON.parse(line, symbolize_names: true)
            yielder.yield Sourced::Event.from(data)
          end
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
