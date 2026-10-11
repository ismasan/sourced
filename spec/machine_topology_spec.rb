# frozen_string_literal: true

require 'spec_helper'

module MachineTopologyTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_topology_test.#{name}") { attrs.each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_topology_test.#{name}") { attrs.each { |a| attribute a, String } }

  Open = command(:open, :trip_id)
  Opened = event(:opened, :trip_id)
  Note = command(:note, :trip_id)
  Noted = event(:noted, :trip_id)
  Book = command(:book, :trip_id)
  Booked = event(:booked, :trip_id)
  Cancel = command(:cancel, :trip_id)
  Cancelled = event(:cancelled, :trip_id)
  Done = event(:done, :trip_id)
  AddLeg = command(:add_leg, :trip_id, :leg_id)
  LegAdded = event(:leg_added, :trip_id, :leg_id)
  Confirm = command(:confirm, :trip_id, :leg_id)
  LegConfirmed = event(:leg_confirmed, :trip_id, :leg_id)
  LegDropped = event(:leg_dropped, :trip_id, :leg_id)
  Remind = command(:remind, :trip_id)
  Pay = command(:pay, :trip_id)
  Paid = event(:paid, :trip_id)
  Ship = command(:ship, :trip_id)
  Shipped = event(:shipped, :trip_id)

  class LegMachine < Sourced::Machine
    partition_by :trip_id, :leg_id

    start LegAdded do
      command Confirm => LegConfirmed
    end
    final LegConfirmed
    on_stop LegDropped
  end

  class TripMachine < Sourced::Machine
    partition_by :trip_id

    spawns LegMachine, as: :legs, stop_on: Cancelled

    initial do
      command Open => Opened
    end

    checkpoint Opened do
      entry { |_s, _evt| dispatch Remind }
      command(Note) { |_s, _cmd| event Noted }
      command(AddLeg) { |_s, cmd| spawn :legs, leg_id: cmd.payload.leg_id }
      command Book => Booked
      command Cancel => Cancelled
    end

    checkpoint Booked do
      region :payment do
        initial { command Pay => Paid }
        final Paid
      end
      region :shipping do
        initial { command Ship => Shipped }
        final Shipped
      end
      command Cancel => Cancelled
      done Done
    end

    final Done
    final Cancelled
  end

RSpec.describe 'Sourced::Machine topology' do
  def t(name) = "machine_topology_test.#{name}"

  describe 'Sourced::Topology.build' do
    let(:nodes) { Sourced::Topology.build([TripMachine, LegMachine]) }

    def command(name) = nodes.find { |n| n.type == 'command' && n.id == t(name) }
    def automation(name) = nodes.find { |n| n.type == 'automation' && n.name == name }

    it 'has commands produce their checkpoints, emitted events and spawned children' do
      expect(command(:open).produces).to eq([t(:opened)])
      expect(command(:note).produces).to eq([t(:noted)])
      expect(command(:add_leg).produces).to eq([t(:leg_added)])
      expect(command(:cancel).produces).to eq([t(:cancelled)])
      expect(command('leg_machine.stop').produces).to eq([t(:leg_dropped)])
    end

    it 'describes actions, joins and stopping children as automations' do
      expect(automation('actions(MachineTopologyTest::Opened)').produces).to eq([t(:remind)])
      expect(automation('done(MachineTopologyTest::Booked)')).to have_attributes(
        consumes: [t(:paid), t(:shipped)], produces: [t(:done)]
      )
      expect(automation('stop(MachineTopologyTest::LegMachine)')).to have_attributes(
        consumes: [t(:cancelled)], produces: [t('leg_machine.stop')]
      )
    end

    it 'includes every checkpoint and data event' do
      events = nodes.select { |n| n.type == 'event' }.map(&:id)
      expect(events).to include(t(:opened), t(:noted), t(:booked), t(:paid), t(:shipped), t(:done), t(:leg_added))
    end
  end

  describe '.to_mermaid' do
    let(:diagram) { TripMachine.to_mermaid.gsub('MachineTopologyTest_', '') }

    it 'draws checkpoints, regions and the commands between them' do
      expect(diagram).to eq(<<~MERMAID.chomp)
        stateDiagram-v2
          state "Opened" as Opened
          state "Booked" as Booked {
            state "Paid" as Paid
            [*] --> Paid : Pay
            Paid --> [*]
            --
            state "Shipped" as Shipped
            [*] --> Shipped : Ship
            Shipped --> [*]
          }
          state "Done" as Done
          state "Cancelled" as Cancelled
          [*] --> Opened : Open
          Opened --> Booked : Book
          Opened --> Cancelled : Cancel
          Booked --> Cancelled : Cancel
          Booked --> Done : done
          Done --> [*]
          Cancelled --> [*]
      MERMAID
    end

    it "draws a child's start and Stop" do
      diagram = LegMachine.to_mermaid
      expect(diagram).to include('[*] --> MachineTopologyTest_LegAdded')
      expect(diagram).to include('MachineTopologyTest_LegAdded --> MachineTopologyTest_LegDropped : Stop')
    end

    it "draws transitions to an ancestor region's checkpoint from the state that takes them" do
      m = Class.new(Sourced::Machine) do
        def self.name = 'MachineTopologyTest::Ancestors'
        partition_by :trip_id
        initial { command Open => Opened }
        checkpoint Opened do
          region :payment do
            initial do
              command Pay => Paid
              command Note => Cancelled
            end
            checkpoint(Paid) { command Cancel => Cancelled }
          end
        end
        final Cancelled
      end
      diagram = m.to_mermaid.gsub('MachineTopologyTest_', '')

      expect(diagram).to include("\n  Paid --> Cancelled : Cancel\n")
      expect(diagram).to include("\n  Opened --> Cancelled : Note\n")
    end
  end
end
end
