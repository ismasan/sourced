# frozen_string_literal: true

module Sourced
  module Consumer
    class ConsumerInfo < Types::Data
      ToBlock = Types::Any.transform(Proc) { |v| -> { v } }
      StartFromBeginning = Types::Value[:beginning] >> Types::Static[nil] >> ToBlock
      StartFromNow = Types::Value[:now] >> Types::Static[-> { Time.now - 5 }.freeze]
      StartFromTime = Types::Interface[:call].check('must return a Time') { |v| v.call.is_a?(Time) }

      StartFrom = (
        StartFromBeginning | StartFromNow | StartFromTime
      ).default { -> { nil } }

      attribute :group_id, Types::String.present, writer: true
      attribute :start_from, StartFrom, writer: true
      attribute :async, Types::Boolean.default(true), writer: true

      def sync!
        self.async = false
      end

      def async!
        self.async = true
      end
    end

    def consumer_info
      @consumer_info ||= ConsumerInfo.new(group_id: name, start_from: :beginning)
    end

    def consumer(&)
      return consumer_info unless block_given?

      info = ConsumerInfo.new(group_id: name)
      yield info
      raise Plumb::ParseError, info.errors unless info.valid?

      @consumer_info = info
    end
  end
end
