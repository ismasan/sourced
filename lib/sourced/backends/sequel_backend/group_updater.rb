# frozen_string_literal: true

module Sourced
  module Backends
    class SequelBackend
      class GroupUpdater
        attr_reader :group_id, :updates, :error_context

        def initialize(group_id, row, logger)
          @group_id = group_id
          @row = row
          @logger = logger
          @error_context = row[:error_context]
          @updates = { error_context: @error_context.dup }
        end

        def stop(reason = nil)
          @logger.error "stopping consumer group #{group_id}"
          @updates[:status] = STOPPED
          @updates[:retry_at] = nil
          @updates[:updated_at] = Time.now
          @updates[:error_context][:reason] = reason if reason
        end

        def retry(time, ctx = {})
          @logger.warn "retrying consumer group #{group_id} at #{time}"
          @updates[:updated_at] = Time.now
          @updates[:retry_at] = time
          @updates[:error_context].merge!(ctx)
        end
      end
    end
  end
end
