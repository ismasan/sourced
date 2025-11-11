# frozen_string_literal: true

module Sourced
  # The Injector analyzes method signatures to determine which keyword arguments
  # should be automatically provided when calling methods. This enables dependency
  # injection based on method parameter declarations.
  #
  # @example Basic usage
  #   class MyReactor
  #     def self.handle(event, replaying:, history:)
  #       # Method that expects replaying and history arguments
  #     end
  #   end
  #   
  #   args = Injector.resolve_args(MyReactor, :handle)
  #   # => [:replaying, :history]
  class Injector
    # Parameter types that indicate injectable keyword arguments
    # - :keyreq - Required keyword arguments (replaying:)  
    # - :key - Optional keyword arguments (replaying: false)
    KEYS = %i[keyreq key].freeze

    class << self
      # Analyze a method signature and return the names of keyword arguments
      # that should be automatically injected.
      #
      # @param args [Array] Method specification - either [Class, Symbol] or [Proc]
      # @return [Array<Symbol>] List of keyword argument names to inject
      #
      # @example Analyze a class method
      #   Injector.resolve_args(MyClass, :handle)
      #   # => [:replaying, :history]
      #
      # @example Analyze a constructor
      #   Injector.resolve_args(MyClass, :new) 
      #   # => [:backend, :logger]
      #
      # @example Analyze a proc
      #   my_proc = proc { |event, replaying:| ... }
      #   Injector.resolve_args(my_proc)
      #   # => [:replaying]
      def resolve_args(*args)
        parameters = case args
          in [Proc] # single proc
          args.first.parameters
          in [Class => obj, :new]
          obj.instance_method(:initialize).parameters
          in [Object => obj, Symbol => meth]
          obj.method(meth).parameters
        end

        parameters.each_with_object([]) do |(type, name), list|
          list << name if KEYS.include?(type)
        end
      end
    end
  end
end
