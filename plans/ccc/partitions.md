# CCC: Consumer Group Support (Store-Level Primitives)

## Context

CCC has a flat, globally-ordered message log with automatic key-pair indexing. To process messages in parallel in the background (like Sourced's workers/reactors), we need per-consumer-group, per-partition offset tracking.

A **partition** is defined by one or more attribute names (e.g., `[:course_name, :user_id]`). Each unique combination of those attribute values forms a partition instance. The key CCC insight:

- **Partition identity** (AND): a partition `(course_name=Algebra, user_id=joe)` is discovered from messages that have ALL those attributes (e.g. `UserJoinedCourse`). Messages with fewer attributes (e.g. `CourseCreated` with only `course_name`) do not create partitions — they are included when *fetching* for a partition.
- **Message fetch** (conditional AND): for a given partition, each message is matched against the *intersection* of the partition's attributes and the message's own declared payload attributes. See "Fetch Semantics" below.
- **ConsistencyGuard**: `claim_next` returns a guard for optimistic concurrency, enabling deciders to detect concurrent writes at append time.

Store-level primitives only — consumer DSL and worker/dispatcher come later.

## Fetch Semantics: Conditional AND

Pure OR fetching ("return messages where `course_name=Algebra` OR `user_id=joe`") is **incorrect** because it pulls in unrelated messages.

**Example of the problem with pure OR:**

Given partition `(course_name=Algebra, user_id=joe)`:
- `CourseCreated(course_name: Algebra)` — only has `course_name` → match on `course_name=Algebra` → **correct**
- `UserJoined(user_id: joe, course_name: Algebra)` — has both → must match BOTH → **correct**
- `UserJoined(user_id: jake, course_name: Algebra)` — has both → must match BOTH → `user_id=jake ≠ joe` → **excluded** (but pure OR would include it!)
- `UserJoined(user_id: joe, course_name: History)` — has both → must match BOTH → `course_name=History ≠ Algebra` → **excluded** (but pure OR would include it!)

**The correct rule:**

> For each message, match against ALL of the partition's attributes that the message has. If a message has 1 of 2 partition attributes, match on that 1. If it has 2 of 2, match on both.

In SQL terms: for each message, count how many of the partition's key_pairs it matches, count how many of the partition's attribute *names* appear in the message's key_pairs (regardless of value), and include the message only if those counts are equal.

**Implementation approach:**

Each partition has N key_pairs (e.g., `course_name=Algebra` and `user_id=joe`). Each key_pair has an attribute name. For a candidate message:

1. `matched_count` = how many of the partition's key_pairs this message is joined to (via `ccc_message_key_pairs`)
2. `relevant_count` = how many of the partition's attribute *names* this message has ANY value for

Include the message iff `matched_count = relevant_count` (i.e., for every partition attribute the message knows about, it has the right value).

**SQL for fetch:**

```sql
SELECT DISTINCT m.position, m.message_id, m.message_type, m.payload, m.metadata, m.created_at
FROM ccc_messages m
WHERE m.position > :last_position
  AND m.message_type IN (:handled_types)
  AND EXISTS (
    SELECT 1 FROM ccc_message_key_pairs mkp
    WHERE mkp.message_position = m.position
      AND mkp.key_pair_id IN (:partition_key_pair_ids)
  )
  AND (
    SELECT COUNT(*) FROM ccc_message_key_pairs mkp
    WHERE mkp.message_position = m.position
      AND mkp.key_pair_id IN (:partition_key_pair_ids)
  ) = (
    SELECT COUNT(DISTINCT kp_part.name)
    FROM ccc_message_key_pairs mkp2
    JOIN ccc_key_pairs kp_msg ON mkp2.key_pair_id = kp_msg.id
    JOIN ccc_key_pairs kp_part ON kp_part.id IN (:partition_key_pair_ids)
      AND kp_part.name = kp_msg.name
    WHERE mkp2.message_position = m.position
  )
ORDER BY m.position ASC
```

Note: DISTINCT is required due to a SQLite optimizer behavior where EXISTS with `IN (...)` can produce duplicate rows via index semi-join optimization.

**Walk-through:**

| Message | Partition attrs present | Matched key_pairs | Left=Right? | Included? |
|---------|------------------------|-------------------|-------------|-----------|
| `CourseCreated(course_name=Algebra)` | course_name (1) | course_name=Algebra (1) | 1=1 | Yes |
| `UserJoined(user_id=joe, course_name=Algebra)` | course_name, user_id (2) | both (2) | 2=2 | Yes |
| `UserJoined(user_id=jake, course_name=Algebra)` | course_name, user_id (2) | course_name=Algebra only (1) | 1!=2 | No |
| `CourseClosed(course_name=Algebra)` | course_name (1) | course_name=Algebra (1) | 1=1 | Yes |
| `UserRegistered(user_id=joe)` | user_id (1) | user_id=joe (1) | 1=1 | Yes |
| `UserJoined(user_id=joe, course_name=History)` | course_name, user_id (2) | user_id=joe only (1) | 1!=2 | No |

## ConsistencyGuard from claim_next

`claim_next` returns a `ConsistencyGuard` alongside the messages. This enables deciders to do optimistic concurrency checks at append time: "fail if any new messages matching these conditions appeared since I read."

**Guard conditions are built from `Message.to_conditions`** — each message class knows its own declared payload attributes and only generates conditions for attributes it actually has. This avoids nonsensical conditions (e.g. `CourseCreated × user_id`) while still covering all `handled_types` for conflict detection.

```ruby
# Message class API
CourseCreated.payload_attribute_names  # => [:course_name]
CourseCreated.to_conditions(course_name: 'Algebra', user_id: 'joe')
# => [QueryCondition('course.created', 'course_name', 'Algebra')]
# user_id is ignored — CourseCreated doesn't have it

UserJoinedCourse.to_conditions(course_name: 'Algebra', user_id: 'joe')
# => [QueryCondition('user.joined_course', 'course_name', 'Algebra'),
#     QueryCondition('user.joined_course', 'user_id', 'joe')]
```

`payload_attribute_names` is cached at define time (frozen array stored on the class).

Guard conditions are built from `handled_types` (not just the fetched messages) so the guard covers message types that haven't appeared yet but would be conflicts (e.g. `CourseClosed`).

## Schema

Add three tables to `install!`:

```sql
CREATE TABLE IF NOT EXISTS ccc_consumer_groups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'active',
  error_context TEXT,
  retry_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ccc_offsets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  consumer_group_id INTEGER NOT NULL REFERENCES ccc_consumer_groups(id) ON DELETE CASCADE,
  partition_key TEXT NOT NULL,
  last_position INTEGER NOT NULL DEFAULT 0,
  claimed INTEGER NOT NULL DEFAULT 0,
  claimed_at TEXT,
  claimed_by TEXT,
  UNIQUE(consumer_group_id, partition_key)
);

-- Maps each offset to its constituent key_pairs (supports composite partitions)
CREATE TABLE IF NOT EXISTS ccc_offset_key_pairs (
  offset_id INTEGER NOT NULL REFERENCES ccc_offsets(id) ON DELETE CASCADE,
  key_pair_id INTEGER NOT NULL REFERENCES ccc_key_pairs(id),
  PRIMARY KEY (offset_id, key_pair_id)
);
```

- `partition_key` TEXT: canonical serialized string for uniqueness (e.g. `"course_name:Algebra|user_id:joe"`, sorted by attribute name)
- `ccc_offset_key_pairs`: join table mapping offsets to their key_pairs — enables message queries via SQL joins
- Single-attribute partitions: one row per offset in join table. Composite: N rows.

## API

```ruby
# Lifecycle
store.register_consumer_group(group_id)
store.consumer_group_active?(group_id)
store.stop_consumer_group(group_id)
store.start_consumer_group(group_id)
store.reset_consumer_group(group_id)   # deletes all offsets

# Core processing
store.claim_next(group_id, partition_by:, handled_types:, worker_id:)
# partition_by: String or Array — e.g. 'device_id' or ['course_name', 'user_id']
# → { offset_id:, key_pair_ids:, partition_key:, partition_value:, messages: [...], guard: } or nil

store.ack(group_id, offset_id:, position:)
store.release(group_id, offset_id:)
```

`partition_by` and `handled_types` are passed each call (not persisted), matching Sourced's pattern.

`partition_value` is a Hash: `{ 'course_name' => 'Algebra', 'user_id' => 'joe' }`.

`guard` is a `ConsistencyGuard` ready to pass to `store.append(events, guard:)`.

## Claim Flow

### 1. Bootstrap offsets

Discover unique partition tuples via AND self-joins (one per partition attribute) — messages must have ALL attributes to define a partition:

```sql
-- For partition_by: ['course_name', 'user_id']
-- Finds tuples like (Algebra, joe), (Physics, jake), etc.
SELECT kp0.id AS kp_id_0, kp0.value AS val_0,
       kp1.id AS kp_id_1, kp1.value AS val_1
FROM ccc_messages m
JOIN ccc_message_key_pairs mkp0 ON m.position = mkp0.message_position
JOIN ccc_key_pairs kp0 ON mkp0.key_pair_id = kp0.id AND kp0.name = 'course_name'
JOIN ccc_message_key_pairs mkp1 ON m.position = mkp1.message_position
JOIN ccc_key_pairs kp1 ON mkp1.key_pair_id = kp1.id AND kp1.name = 'user_id'
GROUP BY kp0.id, kp1.id
```

No type filter — partitions are discovered from any message with all the attributes. For each tuple: INSERT OR IGNORE into `ccc_offsets` + `ccc_offset_key_pairs`.

### 2. Find and claim (inside brief transaction)

Find the next unclaimed partition with pending messages. Uses OR semantics for detection (any matching key_pair has messages beyond `last_position`). False positives are harmless — the partition is claimed, fetched, gets no results after conditional AND filtering, and is released.

```sql
SELECT o.id AS offset_id, o.partition_key, o.last_position,
       MIN(m.position) AS next_position
FROM ccc_offsets o
JOIN ccc_offset_key_pairs okp ON o.id = okp.offset_id
JOIN ccc_message_key_pairs mkp ON okp.key_pair_id = mkp.key_pair_id
JOIN ccc_messages m ON mkp.message_position = m.position
WHERE o.consumer_group_id = :cg_id
  AND o.claimed = 0
  AND m.position > o.last_position
  AND m.message_type IN (:handled_types)
GROUP BY o.id
ORDER BY next_position ASC
LIMIT 1
```

Then claim: `UPDATE ccc_offsets SET claimed=1, claimed_at=now, claimed_by=worker_id WHERE id=? AND claimed=0`

### 3. Fetch messages (outside transaction, read-only)

**Conditional AND semantics** — see "Fetch Semantics" section above for the full SQL and walk-through.

### 4. Build ConsistencyGuard

For each `handled_type`, look up the message class via `Message.registry` and call `klass.to_conditions(**partition_attrs)`. This only creates conditions for attributes the class actually declares. `last_position` is the position of the last fetched message.

### 5. Ack / Release

```sql
-- ack: advance offset + release claim
UPDATE ccc_offsets SET last_position=?, claimed=0, claimed_at=NULL, claimed_by=NULL
  WHERE consumer_group_id=? AND id=?

-- release: release claim without advancing (for error recovery)
UPDATE ccc_offsets SET claimed=0, claimed_at=NULL, claimed_by=NULL
  WHERE consumer_group_id=? AND id=?
```

## Message Class API

```ruby
# Defined on CCC::Message subclasses:

# Returns frozen array of payload attribute names (cached at define time)
CourseCreated.payload_attribute_names  # => [:course_name]

# Builds QueryConditions for attributes this class actually has
CourseCreated.to_conditions(course_name: 'Algebra', user_id: 'joe')
# => [QueryCondition('course.created', 'course_name', 'Algebra')]
```

## Private Helper Methods

| Method | Purpose |
|--------|---------|
| `bootstrap_offsets(cg_id, partition_by)` | AND self-joins to discover tuples, create offset + key_pair rows |
| `find_and_claim_partition(cg_id, handled_types, worker_id)` | OR-join to find unclaimed offset with pending messages, claim it |
| `fetch_partition_messages(key_pair_ids, last_position, handled_types)` | Conditional AND query, return PositionedMessages |
| `build_partition_key(partition_by, values)` | Build canonical string: `"attr1:v1\|attr2:v2"` (sorted) |

## Files Modified

| File | Change |
|------|--------|
| `lib/sourced/ccc/message.rb` | Add `EMPTY_ARRAY`, `payload_attribute_names` (cached at define time), `to_conditions` |
| `lib/sourced/ccc/store.rb` | Add 3 tables to `install!`, update `installed?` + `clear!`, add `ACTIVE`/`STOPPED` constants, add all lifecycle + claim/ack/release methods with guard |
| `spec/sourced/ccc/message_spec.rb` | Tests for `payload_attribute_names` and `to_conditions` |
| `spec/sourced/ccc/store_spec.rb` | Tests for all new methods |

## Tests

### Consumer Group Lifecycle
1. `register_consumer_group` creates row with active status
2. `register_consumer_group` is idempotent
3. `consumer_group_active?` returns true/false for active/stopped/nonexistent
4. `stop/start_consumer_group` toggle status
5. `reset_consumer_group` deletes all offsets

### claim_next (single attribute partition)
6. Bootstraps offsets for new partitions
7. Returns nil when no pending messages
8. Returns messages for next unclaimed partition with correct shape (including guard)
9. Returns multiple pending messages for same partition
10. Skips claimed partitions — second worker gets different partition
11. Returns nil when all partitions claimed
12. Respects handled_types filter
13. Only returns messages after last_position
14. Returns nil for stopped consumer group
15. Prioritizes partition with earliest pending message
16. Bootstraps newly appeared partitions on subsequent calls
17. Returns guard with conditions only for key_names each type actually has
18. Guard can be used for optimistic concurrency on append
19. Guard detects concurrent conflicting writes

### claim_next (composite partition — conditional AND fetch semantics)
20. Bootstraps composite partitions (only messages with ALL attributes create partitions)
21. Fetch returns messages with single partition attribute (e.g., `CourseCreated(course_name)` for partition `(course_name, user_id)`)
22. Different composite partitions can be claimed in parallel
23. Messages with ALL partition attributes must match ALL values — `UserJoined(user_id=jake, course_name=Algebra)` excluded from partition `(course_name=Algebra, user_id=joe)`
24. Messages with ALL partition attributes matching are not duplicated in results
25. Messages with partial attributes matching wrong value are excluded — `UserJoined(user_id=joe, course_name=History)` excluded from partition `(course_name=Algebra, user_id=joe)`
26. Guard conditions only for key_names each message type actually has
27. Guard detects concurrent writes in composite partition

### ack / release
28. ack advances offset and releases claim
29. After ack, subsequent claim skips processed messages
30. release releases claim without advancing
31. After release, same partition re-claimed with same messages

### clear!
32. Clears consumer groups, offsets, and offset_key_pairs

### Message class methods
33. `payload_attribute_names` returns attribute names for defined message class
34. `payload_attribute_names` returns empty array for bare message class
35. `to_conditions` returns conditions only for attributes the message class has
36. `to_conditions` returns conditions for all matching attributes
37. `to_conditions` returns empty array when no attributes match

## Usage Example

```ruby
# Message definitions
CourseCreated = CCC::Message.define('course.created') do
  attribute :course_name, String
end

UserRegistered = CCC::Message.define('user.registered') do
  attribute :user_id, String
  attribute :name, String
end

UserJoinedCourse = CCC::Message.define('user.joined_course') do
  attribute :course_name, String
  attribute :user_id, String
end

CourseClosed = CCC::Message.define('course.closed') do
  attribute :course_name, String
end

# Register consumer group
store.register_consumer_group('enrollment-decider')

# Append events
store.append(CourseCreated.new(payload: { course_name: 'Algebra' }))
store.append(UserRegistered.new(payload: { user_id: 'joe', name: 'Joe' }))
store.append(UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' }))
store.append(UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'jake' }))

# Claim partition (Algebra, joe) — discovered from UserJoinedCourse
result = store.claim_next('enrollment-decider',
  partition_by: ['course_name', 'user_id'],
  handled_types: ['course.created', 'user.registered', 'user.joined_course', 'course.closed'],
  worker_id: 'w-1')

result[:messages]
# => [CourseCreated(course_name: Algebra),                       <- 1 attr, matches
#     UserRegistered(user_id: joe),                              <- 1 attr, matches
#     UserJoinedCourse(course_name: Algebra, user_id: joe)]      <- 2 attrs, both match
# NOT: UserJoinedCourse(course_name: Algebra, user_id: jake)     <- 2 attrs, user_id != joe

result[:guard].conditions
# => [QueryCondition('course.created',      'course_name', 'Algebra'),        <- CourseCreated has course_name
#     QueryCondition('user.registered',     'user_id',     'joe'),            <- UserRegistered has user_id (not course_name)
#     QueryCondition('user.joined_course',  'course_name', 'Algebra'),        <- UserJoinedCourse has both
#     QueryCondition('user.joined_course',  'user_id',     'joe'),
#     QueryCondition('course.closed',       'course_name', 'Algebra')]        <- CourseClosed has course_name

# Decider processes messages, produces events
events = decider.decide(result[:messages])

# Append with guard — raises ConcurrentAppendError if conflicts
store.append(events, guard: result[:guard])

# Ack on success
store.ack('enrollment-decider',
  offset_id: result[:offset_id],
  position: result[:messages].last.position)
```

## Verification

```bash
bundle exec rspec spec/sourced/ccc/
# 88 examples, 0 failures
```
