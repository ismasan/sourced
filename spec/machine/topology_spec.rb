# frozen_string_literal: true

require 'spec_helper'

module MachineTopologyTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_topology_test.#{name}") { attrs.each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_topology_test.#{name}") { attrs.each { |a| attribute a, String } }

  Open = command(:open, :trip_id)
  Note = command(:note, :trip_id)
  Noted = event(:noted, :trip_id)
  Settled = event(:settled, :trip_id)
  Book = command(:book, :trip_id)
  Cancel = command(:cancel, :trip_id)
  AddLeg = command(:add_leg, :trip_id, :leg_id)
  Confirm = command(:confirm, :trip_id, :leg_id)
  Remind = command(:remind, :trip_id)
  Pay = command(:pay, :trip_id)
  Ship = command(:ship, :trip_id)
  Archive = command(:archive, :trip_id)

  class LegMachine < Sourced::Machine
    partition_by :trip_id, :leg_id

    started :added do
      command Confirm => :confirmed
    end
    final :confirmed
    stopped :dropped
  end

  class TripMachine < Sourced::Machine
    partition_by :trip_id

    Opened = State(:opened)
    Booked = State(:booked)
    Done = State(:done)
    Cancelled = State(:cancelled)

    initial do
      command Open => Opened
    end

    state Opened do
      entry { |_s, _evt| dispatch Remind }
      command(Note) { |_s, _cmd| event Noted }
      command AddLeg, starts: LegMachine
      command Book => Booked
      command Cancel => Cancelled
    end

    state Booked do
      region :payment do
        initial { command Pay => :paid }
        final :paid
      end
      region :shipping do
        initial { command Ship => :shipped }
        final :shipped
      end
      exit { |_s, _evt| dispatch Archive }
      command Cancel => Cancelled
      done Done
    end

    final Done
    final(Cancelled) { stops LegMachine }
  end

  RSpec.describe 'Sourced::Machine topology' do
    def t(name) = "machine_topology_test.#{name}"

    def trip(name) = t("trip_machine.runtime.#{name}")
    def leg(name) = t("leg_machine.runtime.#{name}")

    describe 'Sourced::Topology.build' do
      let(:nodes) { Sourced::Topology.build([TripMachine, LegMachine]) }

      def command(name) = nodes.find { |n| n.type == 'command' && n.id == t(name) }
      def automation(name) = nodes.find { |n| n.type == 'automation' && n.name == name }

      it "has commands produce their targets' state classes, data events and started children" do
        expect(command(:open).produces).to eq([trip(:opened)])
        expect(command(:note).produces).to eq([t(:noted)])
        expect(command(:add_leg).produces).to eq([t('leg_machine.runtime.child.started')])
        expect(command(:cancel).produces).to eq([trip(:cancelled)])
        # Reaching a root final finishes the child, which its parent's joins consume.
        expect(command(:confirm).produces).to eq([leg(:confirmed), t('leg_machine.runtime.child.finished')])
        expect(command('leg_machine.runtime.child.stop').produces).to eq([leg(:dropped), t('leg_machine.runtime.child.finished')])
      end

      it 'describes actions, joins and stopping children as automations, consuming the states that run them' do
        expect(automation('actions(opened)')).to have_attributes(consumes: [trip(:opened)], produces: [t(:remind)])
        # Exit actions run on the transitions that leave the state, not on entering it.
        expect(automation('exit(booked)')).to have_attributes(consumes: [trip(:cancelled), trip(:done)], produces: [t(:archive)])
        expect(automation('done(booked)')).to have_attributes(consumes: [trip(:paid), trip(:shipped)], produces: [trip(:done)])
        expect(automation('stop(MachineTopologyTest::LegMachine)')).to have_attributes(
          consumes: [trip(:cancelled)], produces: [t('leg_machine.runtime.child.stop')]
        )
      end

      it "stops a state's child on the transitions that leave its state" do
        machine = Class.new(Sourced::Machine) do
          def self.name = 'MachineTopologyTest::Invoking'
          partition_by :trip_id

          initial { command Open => :waiting }
          state :waiting do
            start LegMachine do
              payload { |_s, _state| { leg_id: 'l1' } }
              on_done LegMachine.declared_states.fetch(:confirmed) => :booked
            end
            region :notes do
              initial { command Note => :noted }
              state :noted
            end
            command Cancel => :cancelled
          end
          final :booked
          final :cancelled
        end
        nodes = Sourced::Topology.build([machine])
        stop = nodes.find { |n| n.type == 'automation' && n.name == 'stop(MachineTopologyTest::LegMachine)' }
        invoking = ->(name) { t("invoking.runtime.#{name}") }

        # Not :noted, which moves within :waiting's region without leaving it.
        expect(stop.consumes).to contain_exactly(invoking.call(:booked), invoking.call(:cancelled))
        start = nodes.find { |n| n.type == 'automation' && n.name == 'start(MachineTopologyTest::LegMachine)' }
        expect(start).to have_attributes(id: "waiting-start-#{LegMachine.group_id}-#{machine.group_id}-aut",
                                         consumes: [invoking.call(:waiting)], produces: [leg('child.started')])
      end

      it "produces the Started of the children every kind of handler starts, parts' too" do
        machine = Class.new(Sourced::Machine) do
          def self.name = 'MachineTopologyTest::Starting'
          partition_by :trip_id

          initial { command Open => :opened }
        end
        lines = Class.new(Sourced::Machine::Part) do
          part_of machine
          part_prefix 'lines'
          initial { command AddLeg, starts: LegMachine }
          final(:closed) { stops LegMachine }
        end
        machine.class_exec do
          state :opened do
            region :lines, lines
            on(Settled => :settled, starts: LegMachine) { |_s, _evt| start LegMachine, leg_id: 'l1' }
            command(Book => :settled, starts: LegMachine | nil) { |_s, _cmd| nil }
          end
          state :settled do
            children_done(LegMachine => :done, starts: LegMachine) { |_s| start LegMachine, leg_id: 'l2' }
          end
          final :done
        end
        nodes = Sourced::Topology.build([machine])
        find = ->(type, name) { nodes.find { |n| n.type == type && (n.id == name || n.name == name) } }
        started = leg('child.started')
        starting = ->(name) { t("starting.runtime.#{name}") }

        expect(find.call('command', t(:add_leg)).produces).to eq([started])
        expect(find.call('command', t(:book)).produces).to eq([starting.call(:settled), started])
        expect(find.call('automation', 'on(opened, Settled)').produces).to eq([starting.call(:settled), started])
        expect(find.call('automation', "children_done(#{LegMachine.group_id})")).to have_attributes(
          consumes: [leg('child.finished')], produces: [starting.call(:done), started]
        )
        expect(find.call('automation', 'stop(MachineTopologyTest::LegMachine)')).to have_attributes(
          consumes: [t('starting.runtime.lines.closed')], produces: [leg('child.stop')]
        )
      end

      it "follows a part's on_final from the command that reaches its final" do
        outer = Class.new(Sourced::Machine) do
          def self.name = 'MachineTopologyTest::Outer'
          partition_by :trip_id
          initial { command Open => :busy }
        end
        inner = Class.new(Sourced::Machine::Part) do
          part_of outer
          part_prefix 'inner'
          started :working do
            command Confirm => :finished
          end
          final :finished
        end
        outer.class_exec do
          state(:busy, inner) { on_final inner.declared_states.fetch(:finished) => :done }
          final :done
        end
        nodes = Sourced::Topology.build([outer])
        confirm = nodes.find { |n| n.type == 'command' && n.id == t(:confirm) }

        expect(confirm.produces).to eq([t('outer.runtime.inner.finished'), t('outer.runtime.done')])
      end

      it 'includes state classes, child events and data events' do
        events = nodes.select { |n| n.type == 'event' }.map(&:id)
        expect(events).to include(trip(:opened), t(:noted), t('leg_machine.runtime.child.started'), leg(:confirmed))
      end
    end

    describe '.to_mermaid' do
      it 'draws states, regions and the commands between them' do
        expect(TripMachine.to_mermaid).to eq(<<~MERMAID.chomp)
          stateDiagram-v2
            state "opened" as opened
            state "booked" as booked {
              state "payment" as booked_payment {
                state "paid" as booked_payment_paid
                [*] --> booked_payment_paid : Pay
                booked_payment_paid --> [*]
              }
              --
              state "shipping" as booked_shipping {
                state "shipped" as booked_shipping_shipped
                [*] --> booked_shipping_shipped : Ship
                booked_shipping_shipped --> [*]
              }
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
            note right of opened
              AddLeg / starts LegMachine
            end note
            note right of cancelled
              stops LegMachine
            end note
        MERMAID
      end

      it "draws a child's start and Stop" do
        diagram = LegMachine.to_mermaid
        expect(diagram).to include("\n  [*] --> added\n")
        expect(diagram).to include("\n  added --> dropped : Stop\n")
      end

      it "draws transitions to an ancestor region's state from the state that takes them" do
        machine = Class.new(Sourced::Machine) do
          def self.name = 'MachineTopologyTest::Ancestors'
          partition_by :trip_id
          initial { command Open => :opened }
          state :opened do
            region :payment do
              initial do
                command Pay => :paid
                command Note => :cancelled
              end
              state(:paid) { command Cancel => :cancelled }
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
