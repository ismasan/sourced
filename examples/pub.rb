require 'sequel'
require 'sqlite3'
require 'sourced/pubsub/sqlite'

PUB = Sourced::PubSub::SQLite.new(
  db: Sequel.sqlite('./sourced_pubsub.db'),
  serializer: Sourced::PubSub::SQLite::JSONSerializer.new,
)

CH = PUB.subscribe('foo')
