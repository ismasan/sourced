require 'bundler/setup'
require 'sequel'
require 'json'
require 'digest/md5'

module Sequel
  def self.parse_json(json)
    JSON.parse(json, symbolize_names: true)
  end
end

DB = Sequel.postgres('decider')
DB.extension :pg_json

class Commander
  def initialize(db)
    @db = db
    @running = false
  end

  def add(stream_id, command)
    # db.transaction do
      # db[:streams].insert_conflict.insert(stream_id:)
      db[:commands].insert(stream_id:, data: command.to_json)
    # end
  end

  def stop
    @running = false
  end

  def poll(&)
    @running = true
    while @running
      reserve_next(&)
      sleep 1
    end
    puts 'Polling stopped'
  end

  def reserve_next(&)
    cmd = db[:commands].order(Sequel[:commands][:id]).first
    reserve(cmd[:stream_id], &) if cmd
  end

  def reserve(stream_id, &)
    command = db.transaction do
      if lock(stream_id)
        db[:commands].where(stream_id:).order(Sequel[:commands][:id]).first
      else
        nil
      end
    end

    return nil unless command

    yield command if block_given?
    db[:commands].where(id: command[:id]).delete
    command
  ensure
    unlock(stream_id)
  end

  def lock(stream_id)
    db.fetch('select pg_try_advisory_lock(?) AS lock', hash(stream_id)).first[:lock]
  end

  def unlock(stream_id)
    db.fetch('select pg_advisory_unlock(?) AS lock', hash(stream_id)).first[:lock]
  end

  def hash(str)
    # binary_string = ('0' + Digest::MD5.hexdigest(str)[0...16]).hex.to_s(2)
    binary_string = ('0' + Digest::MD5.hexdigest(str)[0...10]).hex
    return binary_string
    # Convert the binary string to a signed BigInt
    bigint_value = binary_string.to_i(2)

    # Check if the most significant bit (bit 63) is set (indicating a negative value in 2's complement)
    if binary_string[0] == '1'
      # Handle negative value by applying 2's complement
      bigint_value -= 2**64
    end

    bigint_value.abs
  end

  private

  attr_reader :db
end

COMMANDS = Commander.new(DB)
# commands.add('cart-123', { name: 'add_item', payload: { sku: '111' } })
# commands.add('cart-123', { name: 'add_item', payload: { sku: '222' } })
# p DB[:commands].first[:data]
