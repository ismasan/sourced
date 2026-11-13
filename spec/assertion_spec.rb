# frozen_string_literal: true

require 'spec_helper'

module AssertionTest
  Cancelled = Sourced::Event.define('assertion_test.cancelled') { attribute :order_id, String }
  Reopened = Sourced::Event.define('assertion_test.reopened') { attribute :order_id, String }
  Paid = Sourced::Event.define('assertion_test.paid') { attribute :order_id, String }
end

RSpec.describe Sourced::Assertion do
  let(:cancelled) { AssertionTest::Cancelled.new(payload: { order_id: 'o1' }) }
  let(:reopened) { AssertionTest::Reopened.new(payload: { order_id: 'o1' }) }
  let(:paid) { AssertionTest::Paid.new(payload: { order_id: 'o1' }) }
  let(:is_cancelled) { described_class.ends_with(AssertionTest::Cancelled) }
  let(:is_paid) { described_class.includes(AssertionTest::Paid) }

  it 'evaluates the built-ins over a history, by message type' do
    {
      described_class.is_empty => { [] => true, [paid] => false },
      is_paid => { [] => false, [cancelled, paid] => true },
      described_class.excludes(AssertionTest::Paid) => { [] => true, [cancelled, paid] => false },
      described_class.starts_with(AssertionTest::Cancelled) => { [] => false, [cancelled, reopened] => true, [reopened, cancelled] => false },
      is_cancelled => { [] => false, [cancelled] => true, [cancelled, reopened] => false, [cancelled, reopened, cancelled] => true }
    }.each do |assertion, cases|
      cases.each { |history, holds| expect(assertion.holds?(history)).to be(holds), "#{assertion.inspect} over #{history.map(&:type)}" }
    end
  end

  it 'combines with + (and), | (or) and ~ (not)' do
    shippable = is_paid + ~is_cancelled
    either = is_paid | is_cancelled

    expect(shippable.holds?([paid, cancelled])).to be(false)
    expect(shippable.holds?([paid, cancelled, reopened])).to be(true)
    expect(either.holds?([])).to be(false)
    expect(either.holds?([cancelled])).to be(true)
  end

  it 'chains built-ins as and' do
    chained = described_class.includes(AssertionTest::Paid)
                             .excludes(AssertionTest::Reopened)
                             .ends_with(AssertionTest::Cancelled)

    expect(chained).to eq(is_paid + described_class.excludes(AssertionTest::Reopened) + is_cancelled)
    expect(chained.holds?([paid, cancelled])).to be(true)
    expect(chained.holds?([paid, cancelled, reopened, cancelled])).to be(false)
    expect((is_paid | is_cancelled).starts_with(AssertionTest::Paid).holds?([paid, cancelled])).to be(true)
  end

  it 'builds built-ins with [], from classes and open ranges' do
    expect(described_class[]).to eq(described_class.is_empty)
    expect(described_class[AssertionTest::Paid, AssertionTest::Cancelled])
      .to eq(is_paid + described_class.includes(AssertionTest::Cancelled))
    expect(described_class[AssertionTest::Paid.., ..AssertionTest::Cancelled])
      .to eq(described_class.starts_with(AssertionTest::Paid) + is_cancelled)
  end

  it 'raises for a range [] has no built-in for' do
    expect { described_class[AssertionTest::Paid...] }.to raise_error(ArgumentError, /takes A\.\. .* or \.\.A/)
    expect { described_class[...AssertionTest::Paid] }.to raise_error(ArgumentError)
    expect { described_class[nil..] }.to raise_error(ArgumentError)
  end

  it 'subtracts with -, and not' do
    expect(is_paid - is_cancelled).to eq(is_paid + ~is_cancelled)
    expect((described_class[AssertionTest::Paid] - described_class[AssertionTest::Reopened..]).holds?([paid])).to be(true)
  end

  it 'is === its holds?, for case/when' do
    result = case [paid, cancelled]
             when is_cancelled then :cancelled
             else :open
             end

    expect(result).to eq(:cancelled)
  end

  it 'unwraps messages' do
    expect(is_paid.holds?([Struct.new(:to_message).new(paid)])).to be(true)
  end

  it 'raises for anything but an Array of messages' do
    expect { is_paid === { items: [] } }.to raise_error(ArgumentError, /takes an Array of messages, got Hash/)
  end

  it 'normalises, so equal assertions compare equal' do
    expect(is_paid + is_cancelled).to eq(is_cancelled + is_paid)
    expect(is_paid | is_cancelled).to eq(is_cancelled | is_paid)
    expect(~~is_cancelled).to eq(is_cancelled)
  end

  it 'inspects as Assertion[] syntax that builds it again' do
    paid, cancelled, reopened = AssertionTest::Paid, AssertionTest::Cancelled, AssertionTest::Reopened
    {
      described_class[] => 'Assertion[]',
      ~is_cancelled => '~Assertion[..Cancelled]',
      described_class[..cancelled, paid, reopened..] - described_class[cancelled..] =>
        'Assertion[Reopened.., Paid, ..Cancelled] - Assertion[Cancelled..]',
      (is_paid | described_class[reopened]) + is_cancelled - described_class[paid, ..reopened] =>
        'Assertion[..Cancelled] + (Assertion[Paid] | Assertion[Reopened]) - Assertion[Paid, ..Reopened]',
      ~(is_paid | is_cancelled) => '~(Assertion[Paid] | Assertion[..Cancelled])'
    }.each do |assertion, inspected|
      expect(assertion.inspect).to eq(inspected)
      expect(assertion.pretty_inspect).to eq("#{inspected}\n")
      rebuilt = Module.new { include AssertionTest }.module_eval(inspected.gsub('Assertion', 'Sourced::Assertion'))
      expect(rebuilt).to eq(assertion)
    end
  end

  it "isn't a source: reactors can't evolve one" do
    expect(Sourced::SourceInterface === is_paid).to be(false)
  end
end
