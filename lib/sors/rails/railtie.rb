# frozen_string_literal: true

module Sors
  module Rails
    class Railtie < ::Rails::Railtie
      # TODO: review this.
      # Workers use Async, so this is needed
      # but not sure this can be safely used with non Async servers like Puma.
      # config.active_support.isolation_level = :fiber

      generators do
        require 'sors/rails/install_generator'
      end
    end
  end
end
