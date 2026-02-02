# frozen_string_literal: true

require 'sequel'
require 'sequel/extensions/migration'
require 'erb'

module Sourced
  module Backends
    class SequelBackend
      class Installer
        def initialize(db, logger:, schema: nil, prefix: 'sourced')
          raise ArgumentError, "invalid prefix: #{prefix}" unless prefix.match?(/\A[a-zA-Z_]\w*\z/)
          raise ArgumentError, "invalid schema: #{schema}" if schema && !schema.match?(/\A[a-zA-Z_]\w*\z/)

          @db = db
          @logger = logger
          @schema = schema
          @prefix = prefix
        end

        # Eval the rendered migration and apply :up directly.
        # No Migrator, no schema_info tracking table.
        def install
          migration.apply(db, :up)
          logger.info("Sourced tables installed (prefix: #{prefix}, schema: #{schema || 'default'})")
        end

        # Check that all expected tables exist.
        def installed?
          all_table_names.all? { |t| db.table_exists?(t) }
        end

        # Apply :down on the migration to drop tables.
        def uninstall
          raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'

          migration.apply(db, :down)
          if schema
            db.run("DROP SCHEMA IF EXISTS #{db.literal(schema.to_sym)}")
          end
        end

        # Render the migration to a file for use with the host app's Sequel::Migrator.
        #
        #   installer.copy_migration_to("db/migrations")
        #   installer.copy_migration_to { "db/migrations/#{Time.now.strftime('%Y%m%d%H%M%S')}_create_sourced_tables.rb" }
        #
        def copy_migration_to(dir = nil, &block)
          path = block ? block.call : File.join(dir, '001_create_sourced_tables.rb')
          File.write(path, rendered_migration)
          logger.info("Copied Sourced migration to #{path}")
          path
        end

        private

        attr_reader :db, :logger, :schema, :prefix

        def migration
          @migration ||= eval(rendered_migration) # rubocop:disable Security/Eval
        end

        def rendered_migration
          @rendered_migration ||= begin
            template_path = File.join(__dir__, 'migrations', '001_create_sourced_tables.rb.erb')
            ERB.new(File.read(template_path)).result(binding)
          end
        end

        # Used in ERB template to produce table name expressions.
        # Returns Ruby source code strings, e.g. `:sourced_streams` or `Sequel[:my_schema][:sourced_streams]`
        def table_name(name)
          t = :"#{prefix}_#{name}"
          schema ? "Sequel[:#{schema}][:#{t}]" : ":#{t}"
        end

        # Returns actual Sequel identifiers for use in installed? checks.
        def all_table_names
          %i[streams consumer_groups offsets messages scheduled_messages workers].map do |name|
            t = :"#{prefix}_#{name}"
            schema ? Sequel[schema.to_sym][t] : t
          end
        end
      end
    end
  end
end
