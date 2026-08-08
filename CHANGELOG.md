## [Unreleased]

### Added

- **Message codecs.** Messages are declared with native Ruby types (`Date`, `Time`,
  `Symbol`, `BigDecimal`, `URI`, `Range`, …) and `Plumb::Codec::JSON` translates them to
  and from the store's JSON columns. Apps register encoders for their own value types
  on a subclass of it and set `config.codec = MyCodec`.
- `Sourced::Store::MessageCodec` — the SQLite store's serializer, namespaced under and
  owned by it. It registers each message class's payload type in a Plumb codec instance
  keyed by message type string, built and frozen at `setup!`; the
  envelope is the store's own business. `config.codec = MyCodec` sets the format, which is global; a
  future store with a different layout owns a different serializer, or none. A message
  type the store can't persist raises `Plumb::TypeError` at `setup!` — the app fails to
  boot rather than raising on the first append.
- `Sourced::Types::JSONData` — any JSON value, recursively — for payload attributes
  whose shape isn't known up front.

### Fixed

- `Date` and `Time` payload attributes never round-tripped through the store: they
  were written via `#to_s` and read back as Strings, producing invalid messages. This
  affected `DurableWorkflow`'s `WaitStarted#at`.
- Scheduled messages recorded `scheduled_at` metadata via `Time#to_s` rather than ISO 8601.
- A message's `created_at` was stored at whole-second precision in the log while the
  scheduled-message blob kept microseconds. Both now come from the encoded message, at
  microsecond precision.

### Changed

- `Configuration::StoreInterface` requires `setup!` instead of `install!` / `installed?`.
  A store prepares itself however it needs to at boot; creating tables is one store's
  answer, not the contract. `Sourced::Store#setup!` creates its tables and compiles its
  serializer; `install!` / `installed?` remain public for scripts and tests.

- Reading a message whose stored payload no longer satisfies its schema raises
  `Sourced::Store::MessageCodec::DecodeError` instead of silently producing an invalid
  message. Appending an invalid message raises `Store::MessageCodec::EncodeError`.
- Reading a message whose type isn't registered raises
  `Sourced::Message::UnknownMessageError` instead of returning a base
  `Sourced::Message` with its payload silently dropped.
- `DurableWorkflow` workflow context and step outputs are typed `Types::JSONData`
  rather than `Types::Any`: values must be JSON-native, checked at append time
  rather than silently coerced to Strings on the way back out.

## [0.1.0] - 2024-09-27

- Initial release
