require 'bundler/setup'
require 'async'

running = true

trap('INT') { running = false }

Sync do
  5.times do |i|
    Async do
      count = 0
      while running && count <= 100
        puts "aa #{i}"
        count += 1
        sleep rand
      end
    end
  end
end

puts 'byebye'
