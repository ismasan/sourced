# frozen_string_literal: true

require 'spec_helper'

module MachineAtTest
  def self.command(name) = Sourced::Command.define("machine_at_test.#{name}") { attribute :order_id, String }

  Place = command(:place)
  Pay = command(:pay)
  Pack = command(:pack)
  Review = command(:review)
  Cancel = command(:cancel)
  Note = command(:note)

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Noted = Event(:noted)

    context { { reviews: 0 } }

    evolve(Noted) { |context, _evt| context[:reviews] += 1 }

    initial { command Place => :placed }

    state :placed do
      command Cancel => :cancelled
      command(Note) { |_context, _cmd| event Noted }
      command(Review => [:approved, nil]) { |context, _cmd| to(:approved) if context[:reviews].zero? }

      region :payment do
        initial { command Pay => :paid }
        final :paid
      end

      region :shipping do
        initial { command Pack => :packed }
        final :packed
      end

      done :completed
    end

    final :approved
    final :completed
    final :cancelled
  end
end

RSpec.describe 'Sourced::Machine.at' do
  let(:m) { MachineAtTest }
  let(:machine) { m::OrderMachine }

  def cmd(klass) = klass.new(payload: { order_id: 'o1' })

  it 'is an instance at the given paths' do
    instance = machine.at(['placed', 'placed.payment.paid'], order_id: 'o1')
    expect(instance).to be_a(machine)
    expect(instance.partition_values).to eq(order_id: 'o1')
    expect(instance.machine.paths).to eq(['placed', 'placed.payment.paid'])
    expect(instance.available_commands).to eq(Set[m::Cancel, m::Note, m::Review, m::Pack])
    expect(instance.context).to eq(reviews: 0)
  end

  it 'is at the initial position with no paths' do
    instance = machine.at([], order_id: 'o1')
    expect(instance.machine.value).to eq(:initial)
    expect(instance.available_commands).to eq(Set[m::Place])
  end

  it 'decides as an instance evolved to the same configuration does' do
    live = machine.new(order_id: 'o1')
    [m::Place, m::Pay].each { |klass| live.decide(cmd(klass)) }

    placed = machine.at(live.machine.paths, order_id: 'o1')
    events = placed.decide(cmd(m::Pack))
    expect(events.map(&:class)).to eq(live.decide(cmd(m::Pack)).map(&:class))
    expect(events.last.payload.order_id).to eq('o1')
    expect(placed.machine.paths).to eq(live.machine.paths)
    expect(placed.machine.value).to eq(machine::Completed)
  end

  it 'runs handler blocks on its context' do
    instance = machine.at(['placed'], order_id: 'o1')
    instance.decide(cmd(m::Review))
    expect(instance.machine.paths).to eq(['approved'])
  end

  it 'starts from the given context' do
    context = { reviews: 1 }
    instance = machine.at(['placed'], context, order_id: 'o1')
    expect(instance.context).to equal(context)

    instance.decide(cmd(m::Review))
    expect(instance.machine.paths).to eq(['placed'])

    instance.decide(cmd(m::Note))
    expect(context).to eq(reviews: 2)
  end

  it "doesn't handle a command no active state accepts" do
    instance = machine.at(['placed'], order_id: 'o1')
    expect(instance.handles_command?(cmd(m::Place))).to be(false)
    expect(instance.decide(cmd(m::Place))).to eq([])
    expect(instance.machine.paths).to eq(['placed'])
  end

  it 'raises without the partition keys' do
    expect { machine.at(['placed']) }.to raise_error(Sourced::MissingPartitionKeyError, /order_id/)
    expect { machine.at(['placed'], order_id: nil) }.to raise_error(Sourced::MissingPartitionKeyError)
  end

  it 'raises for paths that are no configuration' do
    expect { machine.at(['nope'], order_id: 'o1') }.to raise_error(Sourced::Machine::UnresolvablePathError)
    expect { machine.at(['placed.payment.paid'], order_id: 'o1') }.to raise_error(ArgumentError, /inactive region/)
    expect { machine.at(%w[placed cancelled], order_id: 'o1') }.to raise_error(ArgumentError, /in one region/)
  end
end
