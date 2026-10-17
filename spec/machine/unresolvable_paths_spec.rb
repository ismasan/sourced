# frozen_string_literal: true

require 'spec_helper'

module MachineUnresolvablePathsTest
  Open = Sourced::Command.define('machine_unresolvable_paths_test.open') { attribute :doc_id, String }
  Tick = Sourced::Command.define('machine_unresolvable_paths_test.tick') { attribute :doc_id, String }

  class DocMachine < Sourced::Machine
    partition_by :doc_id

    Opened = State(:opened)

    initial { command Open => Opened }
    state(Opened) { after(60) { dispatch Tick } }
  end
end

RSpec.describe 'Sourced::Machine stored paths that no longer resolve' do
  let(:machine) { MachineUnresolvablePathsTest::DocMachine }

  # A stored event from a definition that had a state at +gone+.
  def stored(entered: %w[opened], exited: [])
    machine::Opened.new(payload: { doc_id: 'd1', trigger: 'x', source: 'initial', target: entered.last, exited:, entered: })
  end

  def error_for(event) = /no state at "gone" \(#{Regexp.escape(event.type)} #{event.id}\)/

  it 'raises when replaying an entered path' do
    event = stored(entered: %w[gone])
    expect { machine.new(doc_id: 'd1').evolve([event]) }.to raise_error(Sourced::Machine::UnresolvablePathError, error_for(event))
  end

  it 'raises when replaying an exited path' do
    event = stored(exited: %w[gone])
    expect { machine.new(doc_id: 'd1').evolve([event]) }.to raise_error(Sourced::Machine::UnresolvablePathError, error_for(event))
  end

  it 'raises when running the actions of a claimed event' do
    event = stored(entered: %w[gone])
    expect { machine.new(doc_id: 'd1').__react(event) }.to raise_error(Sourced::Machine::UnresolvablePathError, error_for(event))
  end

  it "raises for a timer whose state's path is gone" do
    entering = stored
    instance = machine.new(doc_id: 'd1').tap { |m| m.evolve([entering]) }
    timer = MachineUnresolvablePathsTest::Tick.new(
      payload: { doc_id: 'd1' }, causation_id: entering.id,
      metadata: { Sourced::Machine::ENTERED_BY => entering.id, Sourced::Machine::ENTERED_AT => 'gone' }
    )

    expect { instance.handles_command?(timer) }.to raise_error(Sourced::Machine::UnresolvablePathError, error_for(timer))
  end
end
