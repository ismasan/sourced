# frozen_string_literal: true

module Sourced
  module Handler
    PREFIX = 'handle'

    def self.included(base)
      base.send :extend, Consumer
      base.send :extend, ClassMethods
    end

    def handle(message, history:, replaying: false)
      handler_name = Sourced.message_method_name(PREFIX, message.class.name)
      return Actions::OK unless respond_to?(handler_name)

      args = {history:, replaying:}
      expected_args = self.class.__args_lookup[handler_name]
      handler_args = expected_args.each.with_object({}) do |key, memo|
        memo[key] = args[key]
      end

      send(handler_name, message, **handler_args)
    end

    module ClassMethods
      def handled_messages
        @handled_messsages ||= []
      end

      def on(*args, &block)
        case args
          in [Symbol => msg_name, Hash => payload_schema]
            __register_named_message_handler(msg_name, payload_schema, &block)
          in [Symbol => msg_name]
            __register_named_message_handler(msg_name, &block)
          in [Class => msg_type] if msg_type < Sourced::Message
            __register_class_message_handler(msg_type, &block)
        else
          args.each do |arg|
            on(*arg, &block)
          end
        end
      end

      def handle(message, history: [], replaying: false)
        results = new.handle(message, history:, replaying:)
        Actions.build_for(results)
      end

      def __args_lookup
        @__args_lookup ||= {}
      end

      private

      def __register_named_message_handler(msg_name, payload_schema = nil, &block)
        msg_class = Sourced::Message.define(__message_type(msg_name), payload_schema:)
        klass_name = msg_name.to_s.split('_').map(&:capitalize).join
        const_set(klass_name, msg_class)
        __register_class_message_handler(msg_class, &block)
      end

      def __register_class_message_handler(msg_type, &block)
        handled_messages << msg_type
        handler_name = Sourced.message_method_name(PREFIX, msg_type.name)
        __args_lookup[handler_name] = Sourced::Injector.resolve_args(block)
        define_method(handler_name, &block)
      end

      # TODO: these are in Actor too
      def __message_type(msg_name)
        [__message_type_prefix, msg_name].join('.').downcase
      end

      def message_namespace
        Types::ModuleToMessageType.parse(name.to_s)
      end

      def __message_type_prefix
        @__message_type_prefix ||= message_namespace
      end
    end
  end
end
