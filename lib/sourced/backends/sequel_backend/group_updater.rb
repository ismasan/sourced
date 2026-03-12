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

        def stop(message: nil)
          @logger.error "stopping consumer group #{group_id}"
          @updates[:status] = STOPPED
          @updates[:retry_at] = nil
          @updates[:updated_at] = Time.now
          @updates[:error_context][:message] = message if message
        end

        def fail(exception: nil)
          @logger.error "failing consumer group #{group_id}"
          @updates[:status] = FAILED
          @updates[:retry_at] = nil
          @updates[:updated_at] = Time.now
          if exception
            @updates[:error_context][:exception_class] = exception.class.to_s
            @updates[:error_context][:exception_message] = exception.message
          end
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
