# frozen_string_literal: true

require 'plumb'
require 'time'
require 'json'
require 'securerandom'

module Types
  include Plumb::Types

  # A UUID string, or generate a new one
  AutoUUID = UUID::V4.default { SecureRandom.uuid }

  module JSON
    class SerializableTime < ::Time
      def to_json(*)
        "\"#{strftime('%Y-%m-%dT%H:%M:%S.%N%z')}\""
      end
    end

    UTCTime = Any[SerializableTime] | (String >> Any.build(SerializableTime, :parse).policy(:rescue,
                                                                                            ::ArgumentError)).invoke(:utc)
    AutoUTCTime = UTCTime.default { SerializableTime.new.utc }
  end
end
