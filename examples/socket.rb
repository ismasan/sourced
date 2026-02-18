require 'sourced'
require 'sourced/pubsub/socket'

Msg = Sourced::Event.define('test') do
  attribute :name, String
end

# Setup
PUB = Sourced::PubSub::Socket.new(socket_dir: '/tmp/sourced_sockets')

# Publishing
# pubsub.publish('events', some_event)

# Subscribing
CH = PUB.subscribe('events')
# channel.start do |event, ch|
#   # handle event
# end
