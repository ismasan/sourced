# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sourced is an Event Sourcing / CQRS library for Ruby built around the "Decide, Evolve, React" pattern. It provides eventual consistency by default with an actor-like execution model for building event-sourced applications.

## Core Architecture

### Key Components
- **Actors**: Classes that hold state, handle commands, produce events, and react to events (lib/sourced/actor.rb)
- **Commands**: Intents to effect change in the system 
- **Events**: Facts describing state changes that have occurred
- **Projectors**: React to events to build views, caches, or other representations (lib/sourced/projector.rb)
- **Backends**: Storage adapters (ActiveRecord, Sequel, test backend) in lib/sourced/backends/
- **Router**: Routes commands and events to appropriate handlers (lib/sourced/router.rb)
- **Supervisor**: Manages background worker processes (lib/sourced/supervisor.rb)

### Message Flow
Commands → Actors (Decide) → Events → Storage → Reactors (React) → New Commands

### Concurrency Model
Sourced processes events by acquiring locks on `[reactor_group_id][stream_id]` combinations, ensuring sequential processing within streams while allowing concurrent processing across different streams.

## Development Commands

### Testing
```bash
# Run all tests (default rake task)
rake

# Run specific test file
bundle exec rspec spec/actor_spec.rb

# Run backend tests
bundle exec rspec spec/backends/

# Run with specific database (PostgreSQL required for some tests)
DATABASE_URL=postgres://localhost/sourced_test bundle exec rspec
```

### Database Setup for Tests
The gem supports multiple backends:
- PostgreSQL (via Sequel or ActiveRecord)
- SQLite (via Sequel or ActiveRecord)  
- In-memory test backend

Test databases are automatically created/cleared by the test suite.

### Console/IRB
```bash
# Interactive console for experimentation
bin/console
```

## Configuration Patterns

### Backend Configuration
```ruby
# PostgreSQL via Sequel (default production setup)
Sourced.configure do |config|
  config.backend = Sequel.connect(ENV.fetch('DATABASE_URL'))
end

# Test backend (default, in-memory)
Sourced.configure do |config|
  config.backend = Sourced::Backends::TestBackend.new
end
```

### Registering Components
```ruby
# Register actors and projectors for background processing
Sourced.register(SomeActor)
Sourced.register(SomeProjector)
```

## Key DSL Patterns

### Actor Definition
```ruby
class SomeActor < Sourced::Actor
  # Initial state factory
  state do |id|
    { id: id, status: 'new' }
  end
  
  # Command handler
  command :create_something, name: String do |state, cmd|
    event :something_created, cmd.payload
  end
  
  # Event handler (state evolution)
  event :something_created, name: String do |state, event|
    state[:name] = event.payload.name
  end
  
  # Reaction (workflow orchestration)
  reaction :something_created do |event|
    stream_for(event).command :next_step
  end
end
```

### Message Definitions
```ruby
# Expanded syntax for complex validation/coercion
CreateLead = Sourced::Command.define('leads.create') do
  attribute :name, Types::String.present
  attribute :email, Types::Email.present
end

LeadCreated = Sourced::Event.define('leads.created') do
  attribute :name, String
  attribute :email, String
end
```

## Backend Implementation Notes

- All backends must implement the BackendInterface defined in lib/sourced/configuration.rb
- SequelBackend is the main production backend (lib/sourced/backends/sequel_backend.rb)
- ActiveRecordBackend provides Rails integration (lib/sourced/backends/active_record_backend.rb)
- TestBackend provides in-memory storage for testing (lib/sourced/backends/test_backend.rb)

## Testing Considerations

- Use shared examples from spec/shared_examples/backend_examples.rb when testing backends
- Time manipulation available via Timecop gem
- Database isolation handled automatically per test
- Concurrent testing patterns available for testing race conditions

## Error Handling

- Default error strategy logs exceptions and stops consumer groups
- Configurable retry/backoff strategies available
- Consumer groups can be stopped/started programmatically via backend API

## CCC Module — Stream-less Event Sourcing (Experimental)

`Sourced::CCC` is a prototype for aggregateless, stream-less event sourcing inspired by "Context-driven Consistency Checks". Events go into a flat, globally-ordered log. Consistency context is assembled dynamically by querying relevant facts via normalized key-value pairs extracted from event payloads.

### Files
- `lib/sourced/ccc.rb` — module entrypoint
- `lib/sourced/ccc/message.rb` — `CCC::Message` base class, `QueryCondition`
- `lib/sourced/ccc/store.rb` — `CCC::Store` (SQLite), `PositionedMessage` wrapper
- `spec/sourced/ccc/` — specs (40 examples)

### CCC::Message
- Extends `Types::Data` like `Sourced::Message`, but without `stream_id`, `seq`, `causation_id`, `correlation_id`
- Own `Registry`, separate from `Sourced::Message.registry`
- `.define(type_str, &block)` — creates subclass with typed payload (same DSL as `Sourced::Message`)
- `.from(hash)` — instantiate correct subclass from type string
- `#extracted_keys` — auto-extracts `[[name, value], ...]` from all top-level payload attributes (skips nils)

### CCC::Store
- Accepts a `Sequel::SQLite::Database` connection
- 3 tables: `ccc_messages` (append-only log with auto-increment position), `ccc_key_pairs` (deduplicated name/value pairs), `ccc_message_key_pairs` (join table)
- `append(messages)` — writes messages + auto-indexes all payload keys, returns last position
- `read(conditions, from_position:, limit:)` — queries by `QueryCondition` array (message_type + key_name + key_value), OR semantics
- `messages_since(conditions, position)` — conflict detection (messages matching conditions after position)
- `PositionedMessage` — `SimpleDelegator` wrapper adding `#position` to frozen `Types::Data` instances

### CCC::QueryCondition
- `Data.define(:message_type, :key_name, :key_value)` — used to query the store

### Design Reference
- Article: `plans/ccc/ccc.md`
- TypeScript reference: [Boundless SQLite storage](https://github.com/SBortz/boundless)