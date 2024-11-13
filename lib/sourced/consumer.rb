# frozen_string_literal: true

module Sourced
  module Consumer
    class ConsumerInfo < Types::Data
      ToBlock = Types::Any.transform(Proc) { |v| -> { v } }
      StartFromSymbols = Types::Symbol.options(%i[beginning now]) >> ToBlock
      StartFromTime = Types::Interface[:call].check('must return a Time') { |v| v.call.is_a?(Time) }
      StartFromSequence = Types::Integer >> ToBlock

      StartFrom = (
        StartFromSymbols | StartFromTime | StartFromSequence
      ).default { -> { :beginning } }

      attribute :group_id, Types::String.present, writer: true
      attribute :start_from, StartFrom, writer: true
    end

    def consumer_info
      @consumer_info ||= ConsumerInfo.new(group_id: name)
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
