# frozen_string_literal: true

module Sourced
  module Backends
    class TestBackend
      class Group
        attr_reader :group_id
        attr_accessor :status, :error_context, :retry_at

        Offset = Struct.new(:stream_id, :index, :locked)

        def initialize(group_id, backend)
          @group_id = group_id
          @backend = backend
          @status = :active
          @error_context = {}
          @retry_at = nil
          @highest_index = -1
          reset!
        end

        def active? = @status == :active

        def stop(reason = nil)
          @error_context[:reason] = reason if reason
          @status = :stopped
        end

        def reset!
          @offsets = {}
          reindex
        end

        def retry(time, ctx = {})
          @error_context.merge!(ctx)
          @retry_at = time
        end

        def to_h
          active_offsets = @offsets.values.select { |o| o.index >= 0 }
          oldest_processed = (active_offsets.min_by(&:index)&.index || -1) + 1
          newest_processed = (active_offsets.max_by(&:index)&.index || -1) + 1
          stream_count = active_offsets.size

          { 
            group_id:, 
            status: @status.to_s, 
            oldest_processed:, 
            newest_processed:, 
            stream_count:,
            retry_at:
          }
        end

        def reindex
          backend.messages.each do |e|
            @offsets[e.stream_id] ||= Offset.new(e.stream_id, -1, false)
          end
        end

        def ack_on(message_id, &)
          global_seq = backend.messages.find_index { |e| e.id == message_id }
          return unless global_seq

          evt = backend.messages[global_seq]
          offset = @offsets[evt.stream_id]
          if offset.locked
            raise Sourced::ConcurrentAckError, "Stream for message #{message_id} is being concurrently processed by #{group_id}"
          else
            offset.locked = true
            yield if block_given?
            offset.index = global_seq
            @highest_index = global_seq if global_seq > @highest_index
            offset.locked = false
          end
        end

        NOOP_FILTER = ->(_) { true } 

        def reserve_next(handled_messages, time_window, process_actions, &)
          time_filter = time_window.is_a?(Time) ? ->(e) { e.created_at > time_window } : NOOP_FILTER
          evt = nil
          offset = nil
          index = -1

          backend.messages.each.with_index do |e, idx|
            offset = @offsets[e.stream_id]
            if offset.locked # stream locked by another consumer in the group
              next
            elsif idx > offset.index && handled_messages.include?(e.class) && time_filter.call(e) # new message for the stream
              evt = e
              offset.locked = true
              index = idx
              break
            else # messages already consumed
            end
          end

          if evt
            replaying = @highest_index >= index
            if block_given?
              actions = yield(evt, replaying)

              acker = -> { ack(offset, index) }
              process_actions.(group_id, actions, acker, evt, offset)
            end

            offset.locked = false
          end

          evt
        end

        private

        def ack(offset, index)
          # ACK reactor/message
          offset.index = index
          @highest_index = index if index > @highest_index
        end

        attr_reader :backend
      end

    end
  end
end
