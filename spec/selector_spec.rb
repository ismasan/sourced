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

  describe '#by with values' do
    it 'scopes by their keys and binds them' do
      expect(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2].by(account_id: 1, user_id: 2))
        .to eq(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2].by(:account_id, :user_id).bind(account_id: 1, user_id: 2))
    end

    it 'takes keys and values together, leaving the keys unbound' do
      selector = described_class[SelectorTestMessages::E1].by(:user_id, account_id: 1)
      expect(selector.keys).to eq(%i[account_id user_id])
      expect(selector.unbound_keys).to eq([:user_id])
    end
  end

  describe '.[] with values' do
    it 'is shorthand for #by with them' do
      expect(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2, account_id: 1, user_id: 2])
        .to eq(described_class[SelectorTestMessages::E1, SelectorTestMessages::E2].by(account_id: 1, user_id: 2))
    end

    it 'is NONE without types' do
      expect(described_class[account_id: 1]).to eq(Sourced::Selector::NONE)
    end
  end

  describe '#by_message_id' do
    it 'scopes every part by the reserved message id key' do
      selector = described_class[SelectorTestMessages::E1].by_message_id
      expect(selector.keys).to eq([:__id])
      expect(Sourced::IdPartition.keys?(selector.keys)).to be true
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

module SelectorAssertionTest
  Cancelled = Sourced::Event.define('selector_assertion_test.cancelled') { attribute :order_id, String }
  Reopened = Sourced::Event.define('selector_assertion_test.reopened') { attribute :order_id, String }
  Paid = Sourced::Event.define('selector_assertion_test.paid') { attribute :order_id, String }
  Noted = Sourced::Event.define('selector_assertion_test.noted') do
    attribute :order_id, String
    attribute? :user_id, String
  end
end

RSpec.describe Sourced::Selector, 'assertions' do
  include SelectorAssertionTest

  def cancelled(order_id = 'o1') = SelectorAssertionTest::Cancelled.new(payload: { order_id: })
  def reopened(order_id = 'o1') = SelectorAssertionTest::Reopened.new(payload: { order_id: })
  def paid(order_id = 'o1') = SelectorAssertionTest::Paid.new(payload: { order_id: })
  def noted(order_id = 'o1', user_id = nil) = SelectorAssertionTest::Noted.new(payload: { order_id:, user_id: }.compact)

  let(:lifecycle) { described_class[SelectorAssertionTest::Cancelled, SelectorAssertionTest::Reopened].by(:order_id) }
  let(:cancel_state) { lifecycle.ends_with(SelectorAssertionTest::Cancelled) }

  describe '#selects?' do
    let(:selector) do
      (described_class[SelectorAssertionTest::Cancelled].by(:order_id) +
       described_class[SelectorAssertionTest::Noted].by(:order_id, :user_id) +
       described_class[SelectorAssertionTest::Paid]).bind(order_id: 'o1', user_id: 'u1')
    end

    it 'applies the partition rule, part by part' do
      expect(selector.selects?(cancelled('o1'))).to be(true)
      expect(selector.selects?(cancelled('o2'))).to be(false)
      expect(selector.selects?(reopened('o1'))).to be(false) # not one of its types
      expect(selector.selects?(paid('o2'))).to be(true)      # a part without keys
      expect(selector.selects?(noted('o1', 'u1'))).to be(true)
      expect(selector.selects?(noted('o1'))).to be(true)      # carries only some of the keys
      expect(selector.selects?(noted('o1', 'u2'))).to be(false)
    end

    it 'reads keys from tags' do
      by_user = described_class[SelectorAssertionTest::Cancelled].by(:order_id, :user_id).bind(order_id: 'o1', user_id: 'u1')
      expect(by_user.selects?(cancelled.with_tags(user_id: 'u1'))).to be(true)
      expect(by_user.selects?(cancelled.with_tags(user_id: 'u2'))).to be(false)
    end

    it "requires a message to carry one of a keyed part's keys" do
      none = described_class[SelectorAssertionTest::Noted].by(:user_id).bind(user_id: 'u1')
      expect(none.selects?(noted('o1'))).to be(false)
    end

    it 'unwraps messages' do
      wrapped = Struct.new(:to_message).new(cancelled('o1'))
      expect(selector.selects?(wrapped)).to be(true)
    end

    it 'raises unless bound' do
      expect { lifecycle.selects?(cancelled) }.to raise_error(Sourced::Selector::UnboundKeyError)
    end
  end

  describe 'built-ins' do
    let(:bound) { ->(s) { s.bind(order_id: 'o1') } }

    it 'is_empty holds when nothing is selected' do
      s = bound.(lifecycle.is_empty)
      expect(s.holds?([])).to be(true)
      expect(s.holds?([cancelled('o2')])).to be(true)
      expect(s.holds?([reopened])).to be(false)
    end

    it 'includes and excludes' do
      inc = bound.(lifecycle.includes(SelectorAssertionTest::Cancelled))
      exc = bound.(lifecycle.excludes(SelectorAssertionTest::Cancelled))
      [[[], false], [[reopened], false], [[reopened, cancelled], true], [[cancelled('o2')], false]].each do |history, included|
        expect(inc.holds?(history)).to be(included)
        expect(exc.holds?(history)).to be(!included)
      end
    end

    it 'starts_with looks at the first selected message' do
      s = bound.(lifecycle.starts_with(SelectorAssertionTest::Cancelled))
      expect(s.holds?([])).to be(false)
      expect(s.holds?([reopened('o2'), cancelled, reopened])).to be(true)
      expect(s.holds?([reopened, cancelled])).to be(false)
    end

    it 'ends_with looks at the last selected message' do
      s = bound.(cancel_state)
      {
        [] => false,
        [cancelled] => true,
        [cancelled, reopened] => false,
        [cancelled, reopened, cancelled] => true,
        [cancelled, reopened('o2')] => true
      }.each { |history, holds| expect(s.holds?(history)).to be(holds), history.map(&:type).inspect }
    end

    it 'raises for a type the selector does not read' do
      expect { lifecycle.excludes(SelectorAssertionTest::Paid) }
        .to raise_error(ArgumentError, /isn't one of .* types/)
    end
  end

  describe 'scope' do
    it 'covers what the selector selects where the assertion is attached' do
      scoped = (cancel_state + described_class[SelectorAssertionTest::Paid].by(:order_id)).bind(order_id: 'o1')
      expect(scoped.holds?([cancelled, paid])).to be(true)

      combined = (lifecycle + described_class[SelectorAssertionTest::Paid].by(:order_id))
        .ends_with(SelectorAssertionTest::Cancelled).bind(order_id: 'o1')
      expect(combined.holds?([cancelled, paid])).to be(false)
    end

    it 'keeps its keys: a later part of the same type with other keys is not in it' do
      by_order = described_class[SelectorAssertionTest::Noted].by(:order_id).ends_with(SelectorAssertionTest::Noted)
      by_user = described_class[SelectorAssertionTest::Noted, SelectorAssertionTest::Paid].by(:user_id)
      s = (by_order + by_user).bind(order_id: 'o1', user_id: 'u1')

      expect(s.holds?([noted('o1')])).to be(true)
      expect(s.holds?([noted('o2', 'u1')])).to be(false)
    end

    it 'is the same whether scoped before or after attaching' do
      before = described_class[SelectorAssertionTest::Cancelled, SelectorAssertionTest::Reopened].by(:order_id)
        .ends_with(SelectorAssertionTest::Cancelled)
      after = described_class[SelectorAssertionTest::Cancelled, SelectorAssertionTest::Reopened]
        .ends_with(SelectorAssertionTest::Cancelled).by(:order_id)
      expect(after).to eq(before)
    end

    it 'is scoped and bound with the selector' do
      s = described_class[SelectorAssertionTest::Cancelled].includes(SelectorAssertionTest::Cancelled).by(:order_id)
      expect(s.assertion.leaves.first.scope.keys).to eq([:order_id])
      expect(s.bind(order_id: 'o1').holds?([cancelled('o2')])).to be(false)
      expect(s.bind(order_id: 'o1').holds?([cancelled('o1')])).to be(true)
    end
  end

  describe 'combinators' do
    let(:paid_sel) { described_class[SelectorAssertionTest::Paid].by(:order_id).includes(SelectorAssertionTest::Paid) }

    it '+ is and, ~ is not' do
      shippable = (paid_sel + ~cancel_state).bind(order_id: 'o1')
      expect(shippable.holds?([paid])).to be(true)
      expect(shippable.holds?([paid, cancelled])).to be(false)
      expect(shippable.holds?([paid, cancelled, reopened])).to be(true)
      expect(shippable.holds?([reopened])).to be(false)
    end

    it '| is or' do
      either = (paid_sel | cancel_state).bind(order_id: 'o1')
      expect(either.holds?([])).to be(false)
      expect(either.holds?([cancelled])).to be(true)
      expect(either.holds?([paid, reopened])).to be(true)
    end

    it 'cancels a double negation' do
      expect(~~cancel_state).to eq(cancel_state)
    end

    it 'compares equal however it was combined' do
      expect(paid_sel + cancel_state).to eq(cancel_state + paid_sel)
      expect(paid_sel | cancel_state).to eq(cancel_state | paid_sel)
    end

    it 'keeps an assertion when adding a plain selector' do
      plain = described_class[SelectorAssertionTest::Paid].by(:order_id)
      expect((cancel_state + plain).assertion).to eq(cancel_state.assertion)
    end

    it 'raises for | or ~ without an assertion' do
      expect { lifecycle | cancel_state }.to raise_error(ArgumentError, /both sides need an assertion/)
      expect { ~lifecycle }.to raise_error(ArgumentError, /needs an assertion/)
    end
  end

  describe 'definition-time checks' do
    it 'raises for an assertion that never holds' do
      expect { lifecycle.includes(SelectorAssertionTest::Cancelled).excludes(SelectorAssertionTest::Cancelled) }
        .to raise_error(Sourced::Selector::AssertionError, /never holds/)
    end

    it 'raises for an assertion that always holds' do
      expect {
        lifecycle.ends_with(SelectorAssertionTest::Cancelled) |
          lifecycle.ends_with(SelectorAssertionTest::Reopened) |
          lifecycle.is_empty
      }.to raise_error(Sourced::Selector::AssertionError, /always holds/)
    end

    it 'builds no automaton for selectors without assertions' do
      expect(Sourced::Selector::Automaton).not_to receive(:new)
      (lifecycle + described_class[SelectorAssertionTest::Paid]).bind(order_id: 'o1').by(:user_id)
      Sourced::Selector::NONE.bind(order_id: 'o1')
    end

    it 'keeps only the letters some message can produce when keys are bound' do
      x = lifecycle.bind(order_id: 'x').ends_with(SelectorAssertionTest::Cancelled)
      y = lifecycle.bind(order_id: 'y').includes(SelectorAssertionTest::Reopened)
      letters = (x + y).automaton.alphabet.map { |l| [l.type, l.signatures.map { |_, values| values[:order_id] }] }
      expect(letters).to contain_exactly(
        [SelectorAssertionTest::Cancelled, ['x']], [SelectorAssertionTest::Cancelled, ['y']],
        [SelectorAssertionTest::Reopened, ['x']], [SelectorAssertionTest::Reopened, ['y']]
      )
    end

    it 'over-approximates scopes that select a type differently, so it never raises wrongly' do
      by_order = described_class[SelectorAssertionTest::Noted].by(:order_id).includes(SelectorAssertionTest::Noted)
      by_user = described_class[SelectorAssertionTest::Noted].by(:user_id).excludes(SelectorAssertionTest::Noted)
      s = by_order + by_user # a Noted for the order but another user satisfies both
      expect(s.bind(order_id: 'o1', user_id: 'u1').holds?([noted('o1', 'u2')])).to be(true)
    end
  end

  describe 'incremental evaluation' do
    let(:selector) do
      (described_class[SelectorAssertionTest::Paid].by(:order_id).includes(SelectorAssertionTest::Paid) + ~cancel_state)
        .bind(order_id: 'o1')
    end
    let(:pool) { [paid, cancelled, reopened, cancelled('o2'), paid('o2')] }

    it 'advances a stored state with new messages only, as a full pass would' do
      rng = Random.new(42)
      50.times do
        history = Array.new(rng.rand(0..8)) { pool.sample(random: rng) }
        cut = rng.rand(0..history.size)
        state = selector.advance(selector.start, history.first(cut))
        expect(selector.accepts?(selector.advance(state, history.drop(cut)))).to eq(selector.holds?(history))
      end
    end

    it 'accepts the same histories when minimised' do
      automaton = selector.automaton
      blocks = automaton.minimal_blocks
      block_of = blocks.each_with_index.flat_map { |block, i| block.map { |s| [s, i] } }.to_h
      letter_for = lambda do |msg|
        sigs = selector.parts.select { |p| p.types.include?(msg.class) && p.selects_pairs?(msg.extracted_keys.to_h) }
                       .map { |p| [p.keys, p.values] }
        automaton.alphabet.find { |l| l.type == msg.class && l.signatures == sigs }
      end

      rng = Random.new(7)
      50.times do
        history = Array.new(rng.rand(0..8)) { pool.sample(random: rng) }
        block = block_of[automaton.initial]
        history.each do |msg|
          letter = letter_for.(msg) or next
          block = block_of[automaton.transitions[blocks[block].first][letter]]
        end
        expect(automaton.accept?(blocks[block].first)).to eq(selector.holds?(history))
      end
    end

    it 'raises unless bound' do
      expect { cancel_state.holds?([]) }.to raise_error(Sourced::Selector::UnboundKeyError)
    end

    it 'always holds without an assertion' do
      expect(lifecycle.bind(order_id: 'o1').holds?([cancelled])).to be(true)
    end
  end

  describe '#to_mermaid' do
    it 'draws the minimal automaton, its states valid or invalid' do
      expect((~cancel_state).to_mermaid).to eq(<<~MERMAID.chomp)
        stateDiagram-v2
            classDef accepting font-weight:bold,stroke-width:3px
            state "valid" as s0
            state "invalid" as s1
            [*] --> s0
            s0 --> s1: Cancelled
            s0 --> s0: Reopened
            s1 --> s1: Cancelled
            s1 --> s0: Reopened
            class s0 accepting
      MERMAID
    end

    it 'merges states that accept the same futures' do
      # starts_with(C) ∨ includes(C) is includes(C): four product states, two classes
      s = lifecycle.starts_with(SelectorAssertionTest::Cancelled) | lifecycle.includes(SelectorAssertionTest::Cancelled)
      expect(s.automaton.states.size).to eq(4)
      expect(s.automaton.minimal_blocks.size).to eq(2)
    end

    it 'draws one valid state that every selected message keeps, without an assertion' do
      expect(lifecycle.to_mermaid).to eq(<<~MERMAID.chomp)
        stateDiagram-v2
            classDef accepting font-weight:bold,stroke-width:3px
            state "valid" as s0
            [*] --> s0
            s0 --> s0: Cancelled, Reopened
            class s0 accepting
      MERMAID
    end
  end

  it 'shows its assertion in #inspect' do
    expect((~cancel_state).inspect).to eq(
      '#<Sourced::Selector [SelectorAssertionTest::Cancelled, SelectorAssertionTest::Reopened].by(order_id: ?) ' \
      'where ¬ends_with(Cancelled)>'
    )
  end
end
