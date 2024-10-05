# frozen_string_literal: true

require 'spec_helper'
require 'sors/backends/active_record_backend'

RSpec.describe Sors::Backends::ActiveRecordBackend, type: :backend do
  subject(:backend) { Sors::Backends::ActiveRecordBackend.new }

  before :all do
    described_class.table_prefix = 'sors_ar'

    ActiveRecord::Base.establish_connection(
      adapter: 'postgresql',
      database: 'sors_test'
    )

    Migrator.new(table_prefix: described_class.table_prefix).up
  end

  after :all do
    Migrator.new(table_prefix: described_class.table_prefix).down
  end

  after do
    backend.clear!
  end

  it_behaves_like 'a backend'

  class Migrator
    attr_reader :migration_version, :table_prefix

    def initialize(table_prefix: 'sors', root_dir: File.expand_path('../..', __dir__))
      @table_prefix = table_prefix
      @root_dir = root_dir
      @migration_version = "[#{ActiveRecord::VERSION::STRING.to_f}]"
      @migdir = File.join(@root_dir, 'spec', 'db', 'migrate')
      @migfilename = File.join(@migdir, 'create_sors_tables.rb')
    end

    def up
      return if Sors::Backends::ActiveRecordBackend.installed?

      migfile = File.read(File.join(@root_dir, 'lib', 'sors', 'rails', 'templates', 'create_sors_tables.rb.erb'))
      migcontent = ERB.new(migfile).result(binding)
      FileUtils.mkdir_p(@migdir)
      File.write(@migfilename, migcontent)
      require @migfilename.sub('.rb', '')
      CreateSorsTables.new.change
    end

    def down
      Sors::Backends::ActiveRecordBackend.uninstall!
      File.delete(@migfilename) if File.exist?(@migfilename)
    end
  end
end
