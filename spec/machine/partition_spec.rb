# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

module MachinePartitionTest
  class OrderNotes < Sourced::Machine
    partition_by :order_id

    PlaceOrder = Command(:place_order)
    Acknowledge = Command(:acknowledge)
    Placed = State(:placed)

    context { { notes: [] } }

    # A part with messages of its own: they declare the owner's keys.
    class NoteMachine < Sourced::Machine::Part
      part_of OrderNotes

      AddNote = Command(:add_note) { attribute :text, String }
      Close = Command(:close)
      Notify = Command(:notify)
      NoteAdded = Event(:note_added) { attribute :text, String }

      Open = State(:open)
      Closed = State(:closed)

      evolve(NoteAdded) { |context, evt| context[:notes] << evt.payload.text }

      initial do
        command AddNote => Open do |_context, cmd|
          event NoteAdded, text: cmd.payload.text
        end
      end

      state Open do
        entry { |_context, _evt| dispatch Notify }
        command Close => Closed
      end

      final Closed
    end

    initial { command PlaceOrder => Placed }
    state(Placed) { region :notes, NoteMachine }

    reaction(NoteMachine::NoteAdded) { |_context, _evt| dispatch Acknowledge }
  end

  class LineMachine < Sourced::Machine
    partition_by :order_id, :line_id

    Ship = Command(:ship)
    Added = State(:added)
    Shipped = State(:shipped)

    started(Added) { command Ship => Shipped }
    final Shipped
    stopped :cancelled
  end

  class LinesOrder < Sourced::Machine
    partition_by :order_id

    AddLine = Command(:add_line) { attribute :line_id, String }
    Open = State(:open)

    initial { command AddLine => Open, starts: LineMachine }
    state Open
  end

  # Defined outside any machine: declares only some of LineNotes' keys.
  OrderNoted = Sourced::Event.define('machine_partition_test.order_noted') do
    attribute :order_id, String
  end

  class LineNotes < Sourced::Machine
    partition_by :order_id, :line_id

    Note = Command(:note)
    Misnote = Command(:misnote)
    Noted = Event(:noted)

    initial do
      command(Note) { |_context, _cmd| event OrderNoted }
      # Puts another line's id in an event declaring the key.
      command(Misnote) { |_context, _cmd| event Noted, line_id: 'l2' }
    end
  end
end

RSpec.describe 'Sourced::Machine partition' do
  let(:mod) { MachinePartitionTest }
  let(:notes) { mod::OrderNotes::NoteMachine }
  let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }

  def run(router, *messages)
    store.append(messages)
    router.drain
  end

  def log = store.read_all(limit: 100).messages
  def of(klass) = log.select { |m| m.instance_of?(klass) }

  describe 'a part with its own messages' do
    let(:router) { Sourced::Router.new(store:, reactors: [mod::OrderNotes]).setup! }
    let(:keys) { { order_id: 'o1' } }

    before do
      run(router, mod::OrderNotes::PlaceOrder.new(payload: keys), notes::AddNote.new(payload: { **keys, text: 'hi' }))
    end

    it "declares the owner's keys first" do
      expect(notes::AddNote.payload_attribute_names).to eq(%i[order_id text])
      expect(notes::NoteAdded.payload_attribute_names).to eq(%i[order_id text])
      expect(notes::AddNote.type).to eq('machine_partition_test.order_notes.note.add_note')
    end

    it "decides a command carrying the owner's keys, through a worker" do
      snapshot = mod::OrderNotes.load(store:, order_id: 'o1')
      expect(snapshot.value).to eq(mod::OrderNotes::Placed => { notes: notes::Open })
      expect(snapshot.context[:notes]).to eq(['hi'])
    end

    it "fills the owner's keys into its data events, transitions and dispatched commands, without tags" do
      [notes::NoteAdded, notes::Open, notes::Notify, mod::OrderNotes::Acknowledge].each do |klass|
        expect(of(klass).map(&:tags)).to eq([{}]), klass.name
        expect(of(klass).map { |m| m.payload.order_id }).to eq(['o1']), klass.name
      end
      expect(of(mod::OrderNotes::Acknowledge).first.payload.order_id).to eq('o1')
      expect(of(notes::Notify).first.payload.order_id).to eq('o1')
      expect(of(notes::NoteAdded).first.payload.to_h).to eq(order_id: 'o1', text: 'hi')
    end

    it 'leaves other partitions alone' do
      expect(mod::OrderNotes.load(store:, order_id: 'o2').value).to eq(:initial)
    end

    it 'replays to the same state, appending nothing' do
      count = log.size
      store.reset_consumer_group(mod::OrderNotes)
      router.drain

      expect(log.size).to eq(count)
      expect(mod::OrderNotes.load(store:, order_id: 'o1').value).to eq(mod::OrderNotes::Placed => { notes: notes::Open })
    end

    it 'is rebuilt by Sourced.load' do
      instance, = Sourced.load(mod::OrderNotes, store:, order_id: 'o1')
      expect(instance.machine.value).to eq(mod::OrderNotes::Placed => { notes: notes::Open })
      expect(instance.context[:notes]).to eq(['hi'])
    end

    it 'decides a command through Sourced.handle!' do
      result = Sourced.handle!(mod::OrderNotes, notes::Close.new(payload: keys), store:)

      expect(result.events.map(&:class)).to eq([notes::Closed])
      expect(result.events.map { |e| e.payload.order_id }).to eq(['o1'])
      expect(mod::OrderNotes.load(store:, order_id: 'o1').value).to eq(mod::OrderNotes::Placed => { notes: notes::Closed })
    end
  end

  it "carries a started child's keys in the payloads of its events" do
    router = Sourced::Router.new(store:, reactors: [mod::LinesOrder, mod::LineMachine]).setup!
    run(router, mod::LinesOrder::AddLine.new(payload: { order_id: 'o1', line_id: 'l1' }))
    run(router, mod::LineMachine::Ship.new(payload: { order_id: 'o1', line_id: 'l1' }))

    [mod::LineMachine::Started, mod::LineMachine::Shipped, mod::LineMachine::Finished].each do |klass|
      expect(of(klass).map { |m| m.payload.to_h.slice(:order_id, :line_id) }).to eq([{ order_id: 'o1', line_id: 'l1' }]), klass.name
      expect(of(klass).map(&:tags)).to eq([{}]), klass.name
    end
  end

  describe 'appending to its partition' do
    let(:instance) { mod::LineNotes.new(order_id: 'o1', line_id: 'l1') }
    let(:keys) { { order_id: 'o1', line_id: 'l1' } }

    it "raises for an event that doesn't declare every partition key" do
      expect { instance.decide(mod::LineNotes::Note.new(payload: keys)) }
        .to raise_error(Sourced::Machine::InvalidEventError, /OrderNoted doesn't declare partition key line_id/)
    end

    it "raises for an event declaring another partition's key value" do
      expect { instance.decide(mod::LineNotes::Misnote.new(payload: keys)) }
        .to raise_error(Sourced::Machine::InvalidEventError, /has line_id "l2", but is appended to the partition where line_id is "l1"/)
    end
  end
end
