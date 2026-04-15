# frozen_string_literal: true

require 'sequel'
require 'sequel/extensions/migration'
require 'erb'

module Sourced
  class Installer
    TABLE_SUFFIXES = %i[messages key_pairs message_key_pairs scheduled_messages consumer_groups offsets offset_key_pairs workers].freeze

    attr_reader :messages_table, :key_pairs_table, :message_key_pairs_table,
                :scheduled_messages_table, :consumer_groups_table, :offsets_table,
                :offset_key_pairs_table, :workers_table

    def initialize(db, logger:, prefix: 'sourced', migration_template: '001_create_sourced_tables.rb.erb')
      raise ArgumentError, "invalid prefix: #{prefix}" unless prefix.match?(/\A[a-zA-Z_]\w*\z/)

      @db = db
      @logger = logger
      @prefix = prefix
      @migration_template = migration_template

      TABLE_SUFFIXES.each do |suffix|
        instance_variable_set(:"@#{suffix}_table", :"#{prefix}_#{suffix}")
      end
    end

    # Eval the rendered migration and apply :up directly.
    def install
      migration.apply(db, :up)
      logger.info("Sourced tables installed (prefix: #{prefix})")
    end

    # Check that all expected tables exist.
    def installed?
      all_table_names.all? { |t| db.table_exists?(t) }
    end

    # Apply :down on the migration to drop tables.
    def uninstall
      raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'

      migration.apply(db, :down)
    end

    # Render the migration to a file for use with the host app's Sequel::Migrator.
    #
    #   installer.copy_migration_to("db/migrations")
    #   installer.copy_migration_to { "db/migrations/#{Time.now.strftime('%Y%m%d%H%M%S')}_create_ccc_tables.rb" }
    #
    def copy_migration_to(dir = nil, &block)
      path = block ? block.call : File.join(dir, '001_create_sourced_tables.rb')
      File.write(path, rendered_migration)
      logger.info("Copied Sourced migration to #{path}")
      path
    end

    private

    attr_reader :db, :logger, :prefix

    def migration
      @migration ||= eval(rendered_migration) # rubocop:disable Security/Eval
    end

    def rendered_migration
      @rendered_migration ||= begin
        template_path = File.join(__dir__, 'migrations', @migration_template)
        ERB.new(File.read(template_path)).result(binding)
      end
    end

    # Returns actual symbols for use in installed? checks.
    def all_table_names
      TABLE_SUFFIXES.map { |suffix| instance_variable_get(:"@#{suffix}_table") }
    end
  end
end
