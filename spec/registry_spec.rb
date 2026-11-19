# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

RSpec.describe Sourced::Registry do
  subject(:registry) { described_class.new }

  def reactor(group_id)
    Class.new do
      define_singleton_method(:group_id) { group_id }
      def self.claim_selector = Sourced::Selector::NONE
      def self.handle_claim(_claim) = []
    end
  end

  let(:a) { reactor('registry.a') }
  let(:b) { reactor('registry.b') }

  def group(*reactors)
    Module.new.tap { |m| m.define_singleton_method(:to_sourced_reactors) { reactors } }
  end

  describe '#register' do
    it 'keeps reactors once each, in registration order' do
      expect(registry.register(a)).to be(a)
      registry.register(b)
      registry.register(a)

      expect(registry.reactors).to eq([a, b])
      expect(registry.to_a).to eq([a, b])
      expect(registry.registered?(a)).to be(true)
      expect(registry.registered?(reactor('registry.c'))).to be(false)
    end

    it 'applies the reactor defaults' do
      plain = Class.new do
        def self.name = 'RegistryPlain'
        def self.claim_selector = Sourced::Selector::NONE
        def self.handle_claim(_claim) = []
      end

      registry.register(plain)

      expect(plain.group_id).to eq('registry_plain')
    end

    it 'registers what to_sourced_reactors returns, recursively, returning the first one' do
      expect(registry.register(group(a, group(b, a)))).to be(a)
      expect(registry.reactors).to eq([a, b])
    end

    it 'raises when to_sourced_reactors returns nothing' do
      expect { registry.register(group) }.to raise_error(ArgumentError, /returned no reactors/)
    end

    it 'raises for a different reactor with the same group_id, until the first one is unregistered' do
      registry.register(a)
      reloaded = reactor('registry.a')

      expect { registry.register(reloaded) }
        .to raise_error(ArgumentError, /already uses group_id "registry.a"/)

      registry.unregister(a)
      registry.register(reloaded)
      expect(registry.reactors).to eq([reloaded])
    end

    it 'registers all of a group or none of it' do
      registry.register(a)

      expect { registry.register(group(b, reactor('registry.a'))) }.to raise_error(ArgumentError)
      expect(registry.reactors).to eq([a])
    end

    it 'registers nothing a router would reject' do
      bad = reactor('Not Valid')

      expect { registry.register(bad) }.to raise_error(ArgumentError, /isn't lowercase and dot-separated/)
      expect(registry.reactors).to eq([])
    end

    it "doesn't register what isn't a reactor" do
      expect { registry.register(Class.new) }.to raise_error(ArgumentError, /isn't a reactor/)
      expect(registry.reactors).to eq([])
    end
  end

  describe '#unregister' do
    let(:child) { reactor('registry.child') }

    def parent(group_id, child)
      reactor(group_id).tap { |r| r.define_singleton_method(:to_sourced_reactors) { [r, child] } }
    end

    it 'removes the reactor, and what its to_sourced_reactors returns' do
      registry.register(a)
      registry.register(b)

      expect(registry.unregister(group(a, b))).to be(true)
      expect(registry.reactors).to eq([])
      expect(registry.unregister(a)).to be(false)
    end

    it 'keeps a child another reactor still starts' do
      first = parent('registry.first', child)
      second = parent('registry.second', child)
      registry.register(first)
      registry.register(second)

      registry.unregister(first)

      expect(registry.reactors).to eq([child, second])
    end

    it "raises for a child unregistered without its parent, and keeps it" do
      machine = parent('registry.machine', child)
      registry.register(machine)

      expect { registry.unregister(child) }.to raise_error(Sourced::Registry::MissingReactorsError, /needs .* registered with it/)
      expect(registry.reactors).to eq([machine, child])
    end

    it 'takes a child and its parent in either order within a batch, as a reloader unloads them' do
      machine = parent('registry.machine', child)
      registry.register(machine)

      registry.batch do
        registry.unregister(child)
        registry.unregister(machine)
      end

      expect(registry.reactors).to eq([])
    end
  end

  describe 'listeners' do
    let(:calls) { [] }

    before { registry.on_change { |r| calls << r.reactors } }

    it 'hear of each change, and not of changes that change nothing' do
      registry.register(a)
      registry.register(a)
      registry.unregister(b)
      registry.unregister(a)

      expect(calls).to eq([[a], []])
    end

    it 'hear of a batch once, when the outermost one ends' do
      registry.batch do
        registry.register(a)
        registry.batch { registry.register(b) }
        expect(calls).to eq([])
        registry.unregister(a)
      end

      expect(calls).to eq([[b]])
    end

    it "don't hear of a batch that raised, which is undone" do
      registry.register(a)

      expect {
        registry.batch do
          registry.register(b)
          raise 'boom'
        end
      }.to raise_error('boom')

      expect(registry.reactors).to eq([a])
      expect(calls).to eq([[a]])
    end

    it "don't hear of a batch whose new set is invalid, which is undone" do
      registry.register(a)

      expect {
        registry.batch do
          registry.unregister(a)
          registry.register(reactor('registry.b'))
          registry.register(reactor('registry.b'))
        end
      }.to raise_error(ArgumentError, /already uses group_id "registry.b"/)

      expect(registry.reactors).to eq([a])
      expect(calls).to eq([[a]])
    end

    it 'run outside the lock, so another thread can read the registry' do
      reads = []
      registry.on_change { reads << Thread.new { registry.reactors }.value }

      registry.register(a)

      expect(reads).to eq([[a]])
    end

    it 'raise to the call that made the change, which is kept' do
      registry.on_change { raise 'listener failed' }

      expect { registry.register(a) }.to raise_error('listener failed')
      expect(registry.reactors).to eq([a])
    end
  end
end
