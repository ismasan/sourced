# frozen_string_literal: true

require 'rails/generators'
require 'rails/generators/active_record'

module Sors
  module Rails
    class InstallGenerator < ::Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path('templates', __dir__)

      class_option :prefix, type: :string, default: 'sors'

      def copy_initializer_file
        copy_file 'initializer.rb', 'config/initializers/sors.rb'
      end

      def create_migration_file
        migration_template 'create_sors_tables.rb.erb', File.join(db_migrate_path, 'create_sors_tables.rb')
      end

      private

      def migration_version
        "[#{ActiveRecord::VERSION::STRING.to_f}]"
      end

      def table_prefix
        options['prefix']
      end
    end
  end
end
