require_relative 'test'

count = 1
COMMANDS.poll do |command|
  puts "Received command: #{command.inspect}"
  sleep 10
  count += 1
  COMMANDS.stop if count > 10
end
