# frozen_string_literal: true

require 'spec_helper'

module MachineTopologyTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_topology_test.#{name}") { attrs.each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_topology_test.#{name}") { attrs.each { |a| attribute a, String } }

  Open = command(:open, :trip_id)
  Note = command(:note, :trip_id)
  Noted = event(:noted, :trip_id)
  Book = command(:book, :trip_id)
  Cancel = command(:cancel, :trip_id)
  AddLeg = command(:add_leg, :trip_id, :leg_id)
  Confirm = command(:confirm, :trip_id, :leg_id)
  Remind = command(:remind, :trip_id)
  Pay = command(:pay, :trip_id)
  Ship = command(:ship, :trip_id)

  class LegMachine < Sourced::Machine
    partition_by :trip_id, :leg_id

    start :added do
      command Confirm => :confirmed
    end
    final :confirmed
    on_stop :dropped
  end

  class TripMachine < Sourced::Machine
    partition_by :trip_id

    Opened = Checkpoint(:opened)
    Booked = Checkpoint(:booked)
    Done = Checkpoint(:done)
    Cancelled = Checkpoint(:cancelled)

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
        initial { command Pay => :paid }
        final :paid
      end
      region :shipping do
        initial { command Ship => :shipped }
        final :shipped
      end
      command Cancel => Cancelled
      done Done
    end

    final Done
    final Cancelled
  end

  RSpec.describe 'Sourced::Machine topology' do
    def t(name) = "machine_topology_test.#{name}"

    def trip(name) = t("trip_machine.runtime.#{name}")
    def leg(name) = t("leg_machine.runtime.#{name}")

    describe 'Sourced::Topology.build' do
      let(:nodes) { Sourced::Topology.build([TripMachine, LegMachine]) }

      def command(name) = nodes.find { |n| n.type == 'command' && n.id == t(name) }
      def automation(name) = nodes.find { |n| n.type == 'automation' && n.name == name }

      it "has commands produce their targets' checkpoint classes, data events and spawned children" do
        expect(command(:open).produces).to eq([trip(:opened)])
        expect(command(:note).produces).to eq([t(:noted)])
        expect(command(:add_leg).produces).to eq([t('leg_machine.runtime.child.started')])
        expect(command(:cancel).produces).to eq([trip(:cancelled)])
        # Reaching a root final finishes the child, which its parent's joins consume.
        expect(command(:confirm).produces).to eq([leg(:confirmed), t('leg_machine.runtime.child.finished')])
        expect(command('leg_machine.runtime.child.stop').produces).to eq([leg(:dropped), t('leg_machine.runtime.child.finished')])
      end

      it 'describes actions, joins and stopping children as automations, consuming the checkpoints that run them' do
        expect(automation('actions(opened)')).to have_attributes(consumes: [trip(:opened)], produces: [t(:remind)])
        expect(automation('done(booked)')).to have_attributes(consumes: [trip(:paid), trip(:shipped)], produces: [trip(:done)])
        expect(automation('stop(MachineTopologyTest::LegMachine)')).to have_attributes(
          consumes: [trip(:cancelled)], produces: [t('leg_machine.runtime.child.stop')]
        )
      end

      it 'includes checkpoint classes, child events and data events' do
        events = nodes.select { |n| n.type == 'event' }.map(&:id)
        expect(events).to include(trip(:opened), t(:noted), t('leg_machine.runtime.child.started'), leg(:confirmed))
      end
    end

    describe '.to_mermaid' do
      it 'draws checkpoints, regions and the commands between them' do
        expect(TripMachine.to_mermaid).to eq(<<~MERMAID.chomp)
          stateDiagram-v2
            state "opened" as opened
            state "booked" as booked {
              state "paid" as booked_payment_paid
              [*] --> booked_payment_paid : Pay
              booked_payment_paid --> [*]
              --
              state "shipped" as booked_shipping_shipped
              [*] --> booked_shipping_shipped : Ship
              booked_shipping_shipped --> [*]
            }
            state "done" as done
            state "cancelled" as cancelled
            [*] --> opened : Open
            opened --> booked : Book
            opened --> cancelled : Cancel
            booked --> cancelled : Cancel
            booked --> done : done
            done --> [*]
            cancelled --> [*]
        MERMAID
      end

      it "draws a child's start and Stop" do
        diagram = LegMachine.to_mermaid
        expect(diagram).to include("\n  [*] --> added\n")
        expect(diagram).to include("\n  added --> dropped : Stop\n")
      end

      it "draws transitions to an ancestor region's checkpoint from the state that takes them" do
        machine = Class.new(Sourced::Machine) do
          def self.name = 'MachineTopologyTest::Ancestors'
          partition_by :trip_id
          initial { command Open => :opened }
          checkpoint :opened do
            region :payment do
              initial do
                command Pay => :paid
                command Note => :cancelled
              end
              checkpoint(:paid) { command Cancel => :cancelled }
            end
          end
          final :cancelled
        end
        diagram = machine.to_mermaid

        expect(diagram).to include("\n  opened_payment_paid --> cancelled : Cancel\n")
        expect(diagram).to include("\n  opened --> cancelled : Note\n")
      end
    end
  end
end
