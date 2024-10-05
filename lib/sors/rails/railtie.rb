# frozen_string_literal: true

module Sors
  module Rails
    class Railtie < ::Rails::Railtie
      generators do
        require 'sors/rails/install_generator'
      end
    end
  end
end
