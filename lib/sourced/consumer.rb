# frozen_string_literal: true

module Sourced
  module Consumer
    ConsumerInfo = Data.define(:group_id)

    def consumer_info
      ConsumerInfo.new(group_id: name)
    end
  end
end
