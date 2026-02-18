# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sqlite_backend'

RSpec.describe 'Sourced::Backends::SQLiteBackend', type: :backend do
  before(:all) do
    @db = Sequel.sqlite
    @backend = Sourced::Backends::SQLiteBackend.new(@db)
    @backend.install
  end

  subject(:backend) { @backend }
  let(:db) { @db }

  before do
    backend.clear!
  end

  it_behaves_like 'a backend'

  describe '#update_schedule!' do
    it 'blocks concurrent workers from selecting the same messages' do
      now = Time.now - 10
      cmd1 = BackendExamples::Tests::DoSomething.parse(stream_id: 'as1', payload: { account_id: 1 })
      cmd2 = BackendExamples::Tests::DoSomething.parse(stream_id: 'as1', payload: { account_id: 1 })
      backend.schedule_messages([cmd1, cmd2], at: now)
      Sourced.config.executor.start do |t|
        2.times.each do
          t.spawn do
            backend.update_schedule!
          end
        end
      end

      expect(backend.read_stream('as1').map(&:id)).to eq([cmd1, cmd2].map(&:id))
    end
  end
end
