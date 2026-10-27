# frozen_string_literal: true

require 'spec_helper'

module SelectorTestMessages
  E1 = Sourced::Message.define('selector_test.e1') { attribute :account_id, String }
  E2 = Sourced::Message.define('selector_test.e2') { attribute :account_id, String }
  E3 = Sourced::Message.define('selector_test.e3') { attribute :user_id, String }
end

RSpec.describe Sourced::Selector do
  describe '.[]' do
    it 'is one part with the types, sorted by type, and no keys' do
      selector = described_class[SelectorTestMessages::E2, SelectorTestMessages::E1]

      expect(selector.parts.size).to eq(1)
      expect(selector.parts.first).to have_attributes(types: [SelectorTestMessages::E1, SelectorTestMessages::E2], keys: [], values: {})
      expect(selector).to eq(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2])
    end

    it 'is Selector::NONE without types' do
      expect(described_class[]).to eq(described_class::NONE)
      expect(described_class::NONE.none?).to be(true)
    end

    it 'resolves anything answering to_message_class' do
      handle = Class.new { def self.to_message_class = SelectorTestMessages::E1 }
      expect(described_class[handle]).to eq(described_class[SelectorTestMessages::E1])
    end

    it 'is frozen' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id)
      expect(selector).to be_frozen
      expect(selector.parts).to be_frozen
      expect(selector.parts.first.types).to be_frozen
    end
  end

  describe '#by' do
    it 'adds keys, sorted' do
      expect(described_class[SelectorTestMessages::E1].by(:user_id).by('account_id').keys).to eq(%i[account_id user_id])
    end

    it 'scopes every part' do
      selector = (described_class[SelectorTestMessages::E1] + described_class[SelectorTestMessages::E3].by(:user_id)).by(:account_id)
      expect(selector.parts.map(&:keys)).to eq([%i[account_id], %i[account_id user_id]])
    end
  end

  describe '#bind' do
    it 'binds stringified values to the keys of every part, ignoring others' do
      selector = (described_class[SelectorTestMessages::E1].by(:account_id) + described_class[SelectorTestMessages::E3].by(:user_id))
        .bind(account_id: 1, user_id: 'u1', tenant_id: 't1')

      expect(selector.parts.map(&:values)).to eq([{ account_id: '1' }, { user_id: 'u1' }])
      expect(selector).to be_bound
    end

    it 'takes String keys, as claims carry them' do
      expect(described_class[SelectorTestMessages::E1].by(:account_id).bind('account_id' => 'a1'))
        .to eq(described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: 'a1'))
    end

    it 'binds partially, and in steps' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id, :user_id).bind(account_id: 'a1')
      expect(selector).not_to be_bound

      expect(selector.bind(user_id: 'u1'))
        .to eq(described_class[SelectorTestMessages::E1].by(:account_id, :user_id).bind(account_id: 'a1', user_id: 'u1'))
    end

    it 'keeps keys added after binding unbound' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: 'a1').by(:user_id)
      expect(selector).not_to be_bound
      expect(selector.bind(user_id: 'u1')).to be_bound
    end

    it 'accepts the same value again' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: 'a1')
      expect(selector.bind(account_id: 'a1')).to eq(selector)
    end

    it 'raises when rebinding a key to a different value' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: 'a1')
      expect { selector.bind(account_id: 'a2') }.to raise_error(ArgumentError, /account_id/)
    end

    it 'raises for a nil value' do
      expect { described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: nil) }
        .to raise_error(Sourced::MissingPartitionKeyError, /account_id/)
    end

    it 'reports unbound keys, and raises for them with #bound!' do
      selector = (described_class[SelectorTestMessages::E1].by(:account_id) + described_class[SelectorTestMessages::E3].by(:user_id))
        .bind(user_id: 'u1')

      expect(selector.unbound_keys).to eq([:account_id])
      expect { selector.bound! }.to raise_error(described_class::UnboundKeyError, /account_id/)
      expect(selector.bind(account_id: 'a1').bound!).to be_bound
    end

    it 'leaves a selector without keys bound' do
      expect(described_class[SelectorTestMessages::E1]).to be_bound
      expect(described_class[SelectorTestMessages::E1].bind(account_id: 'a1')).to eq(described_class[SelectorTestMessages::E1])
    end
  end

  describe '#inspect' do
    it 'shows each part as its types and scope, unbound keys as ?' do
      selector = (described_class[SelectorTestMessages::E2, SelectorTestMessages::E1].by(:account_id, :user_id) +
                  described_class[SelectorTestMessages::E3]).bind(account_id: 'a1')

      expect(selector.inspect).to eq(
        '#<Sourced::Selector [SelectorTestMessages::E3] + ' \
        '[SelectorTestMessages::E1, SelectorTestMessages::E2].by(account_id: "a1", user_id: ?)>'
      )
    end

    it 'names anonymous message classes by type' do
      anon = Class.new(Sourced::Message) { def self.type = 'selector_test.anon' }
      expect(described_class[anon].by(:k).inspect).to eq('#<Sourced::Selector [selector_test.anon].by(k: ?)>')
    end

    it 'is Selector::NONE for no parts' do
      expect(described_class::NONE.inspect).to eq('#<Sourced::Selector::NONE>')
    end
  end

  describe '#+' do
    it 'merges parts with the same keys and values' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id) + described_class[SelectorTestMessages::E2].by(:account_id)
      expect(selector).to eq(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2].by(:account_id))
    end

    it 'keeps parts with different keys apart' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id) + described_class[SelectorTestMessages::E3].by(:user_id)
      expect(selector.parts.map(&:keys)).to eq([%i[account_id], %i[user_id]])
      expect(selector.types).to eq([SelectorTestMessages::E1, SelectorTestMessages::E3])
      expect(selector.keys).to eq(%i[account_id user_id])
    end

    it 'keeps parts with different values apart' do
      a1 = described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: 'a1')
      a2 = described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: 'a2')
      expect((a1 + a2).parts.map(&:values)).to eq([{ account_id: 'a1' }, { account_id: 'a2' }])
    end

    it 'merges parts that become equal once bound' do
      selector = (described_class[SelectorTestMessages::E1].by(:account_id) + described_class[SelectorTestMessages::E2].by(:account_id).bind(account_id: 'a1'))
        .bind(account_id: 'a1')
      expect(selector).to eq(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2].by(:account_id).bind(account_id: 'a1'))
    end

    it 'is commutative' do
      a = described_class[SelectorTestMessages::E1].by(:account_id).bind(account_id: 'a1')
      b = described_class[SelectorTestMessages::E3].by(:user_id)
      c = described_class[SelectorTestMessages::E2]
      expect(a + b + c).to eq(c + b + a)
    end

    it 'has Selector::NONE as its identity' do
      selector = described_class[SelectorTestMessages::E1].by(:account_id)
      expect(described_class::NONE + selector).to eq(selector)
      expect(selector + described_class::NONE).to eq(selector)
      expect([selector, described_class[SelectorTestMessages::E2].by(:account_id)].reduce(described_class::NONE, :+))
        .to eq(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2].by(:account_id))
    end
  end
end
