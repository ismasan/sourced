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
        create_file 'config/initializers/sors.rb' do
          <<~CONTENT
            # frozen_string_literal: true

            require 'sors'
            require 'sors/backends/active_record_backend'

            # This table prefix is used to generate the initial database migrations.
            # If you change the table prefix here,
            # make sure to migrate your database to the new table names.
            Sors::Backends::ActiveRecordBackend.table_prefix = '#{table_prefix}'

            # Configure Sors to use the ActiveRecord backend
            Sors.configure do |config|
              config.backend = Sors::Backends::ActiveRecordBackend.new
              config.logger = Rails.logger
            end
          CONTENT
        end
      end

      def copy_bin_file
        copy_file 'bin_sors', 'bin/sors'
        chmod 'bin/sors', 0o755
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
