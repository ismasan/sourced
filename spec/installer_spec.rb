# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

RSpec.describe Sourced::Installer do
  let(:db) { Sequel.sqlite }

  describe 'prefix' do
    it 'names tables after it' do
      installer = described_class.new(db, logger: Sourced::NULL_LOGGER, prefix: 'billing')

      expect(installer.messages_table).to eq(:billing_messages)
    end

    it 'must be an identifier, as it is interpolated into table names' do
      expect {
        described_class.new(db, logger: Sourced::NULL_LOGGER, prefix: 'billing; drop table')
      }.to raise_error(ArgumentError, /invalid prefix: "billing; drop table"/)
    end
  end

  describe 'TablePrefix' do
    it 'accepts identifiers' do
      expect(described_class::TablePrefix === 'sourced_2').to be(true)
      expect(described_class::TablePrefix === '2sourced').to be(false)
    end
  end
end
