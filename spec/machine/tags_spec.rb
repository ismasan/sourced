# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

module MachineTagsTest
  # Mounted only, with messages of its own: none declares a mounter's partition keys.
  class NoteMachine < Sourced::Machine
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

  class OrderNotes < Sourced::Machine
    partition_by :order_id

    PlaceOrder = Command(:place_order)
    Acknowledge = Command(:acknowledge)
    Placed = State(:placed)

    context { { notes: [] } }

    initial { command PlaceOrder => Placed }
    state(Placed) { region :notes, NoteMachine }

    reaction(NoteMachine::NoteAdded) { |_context, _evt| dispatch Acknowledge }
  end

  class TicketNotes < Sourced::Machine
    partition_by :tenant_id, :ticket_id

    OpenTicket = Command(:open_ticket)
    Opened = State(:opened)

    context { { notes: [] } }

    initial { command OpenTicket => Opened }
    state(Opened) { region :notes, NoteMachine }
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

  # Its handler puts another order's id in an event declaring the key.
  class Misnoted < Sourced::Machine
    partition_by :order_id

    Note = Command(:note)
    Noted = Event(:noted)

    initial do
      command Note do |_context, _cmd|
        event Noted, order_id: 'o2'
      end
    end
  end
end

RSpec.describe 'Sourced::Machine tags' do
  let(:mod) { MachineTagsTest }
  let(:notes) { mod::NoteMachine }
  let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }

  def run(router, *messages)
    store.append(messages)
    router.drain
  end

  def log = store.read_all(limit: 100).messages
  def of(klass) = log.select { |m| m.instance_of?(klass) }

  describe 'a mounted machine with its own messages' do
    let(:router) { Sourced::Router.new(store:, reactors: [mod::OrderNotes]).setup! }
    let(:tags) { { order_id: 'o1' } }

    before do
      run(router, mod::OrderNotes::PlaceOrder.new(payload: tags), notes::AddNote.new(payload: { text: 'hi' }).with_tags(**tags))
    end

    it "declares none of the mounter's keys" do
      expect(notes::AddNote.payload_attribute_names).to eq([:text])
      expect(notes::NoteAdded.payload_attribute_names).to eq([:text])
    end

    it "decides a command tagged with the mounter's keys, through a worker" do
      snapshot = mod::OrderNotes.load(store:, order_id: 'o1')
      expect(snapshot.value).to eq(mod::OrderNotes::Placed => { notes: notes::Open })
      expect(snapshot.context[:notes]).to eq(['hi'])
    end

    it "tags its data events, transitions and dispatched commands with the mounter's partition" do
      [notes::NoteAdded, mod::OrderNotes.state_class(notes::Open), notes::Notify, mod::OrderNotes::Acknowledge].each do |klass|
        expect(of(klass).map(&:tags)).to eq([tags]), klass.name
      end
      expect(of(mod::OrderNotes::Acknowledge).first.payload.order_id).to eq('o1')
      expect(of(notes::NoteAdded).first.payload.to_h).to eq(text: 'hi')
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

    it 'decides a tagged command through Sourced.handle!, tagging what it appends' do
      result = Sourced.handle!(mod::OrderNotes, notes::Close.new.with_tags(**tags), store:)

      expect(result.events.map(&:class)).to eq([mod::OrderNotes.state_class(notes::Closed)])
      expect(result.events.map(&:tags)).to eq([tags])
      expect(mod::OrderNotes.load(store:, order_id: 'o1').value).to eq(mod::OrderNotes::Placed => { notes: notes::Closed })
    end

    it 'raises on an untagged command through Sourced.handle!' do
      expect { Sourced.handle!(mod::OrderNotes, notes::Close.new, store:) }
        .to raise_error(Sourced::MissingPartitionKeyError, /order_id/)
    end
  end

  describe 'the same machine under a mounter with other keys' do
    let(:router) { Sourced::Router.new(store:, reactors: [mod::TicketNotes]).setup! }
    let(:tags) { { tenant_id: 't1', ticket_id: 'k1' } }

    it 'tags with that partition, and reads only it' do
      run(
        router,
        mod::TicketNotes::OpenTicket.new(payload: tags),
        mod::TicketNotes::OpenTicket.new(payload: { tenant_id: 't1', ticket_id: 'k2' }),
        notes::AddNote.new(payload: { text: 'hi' }).with_tags(**tags)
      )

      expect(of(notes::NoteAdded).map(&:tags)).to eq([tags])
      expect(of(mod::TicketNotes.state_class(notes::Open)).map(&:tags)).to eq([tags])
      expect(mod::TicketNotes.load(store:, **tags).value).to eq(mod::TicketNotes::Opened => { notes: notes::Open })
      expect(mod::TicketNotes.load(store:, **tags).context[:notes]).to eq(['hi'])
      expect(mod::TicketNotes.load(store:, tenant_id: 't1', ticket_id: 'k2').value).to eq(mod::TicketNotes::Opened => { notes: :initial })
    end
  end

  it 'tags a started child with its own partition' do
    router = Sourced::Router.new(store:, reactors: [mod::LinesOrder, mod::LineMachine]).setup!
    run(router, mod::LinesOrder::AddLine.new(payload: { order_id: 'o1', line_id: 'l1' }))
    run(router, mod::LineMachine::Ship.new(payload: { order_id: 'o1', line_id: 'l1' }))

    expect(of(mod::LineMachine::Started).map(&:tags)).to eq([{ order_id: 'o1' }])
    expect(of(mod::LineMachine::Shipped).map(&:tags)).to eq([{ order_id: 'o1', line_id: 'l1' }])
    expect(of(mod::LineMachine::Finished).map(&:tags)).to eq([{ order_id: 'o1', line_id: 'l1' }])
  end

  it 'raises when a payload declares a partition key with another value' do
    instance = mod::Misnoted.new(order_id: 'o1')
    expect { instance.decide(mod::Misnoted::Note.new(payload: { order_id: 'o1' })) }
      .to raise_error(Sourced::Message::TagConflictError, /order_id: "o1" conflicts with its payload value "o2"/)
  end
end
