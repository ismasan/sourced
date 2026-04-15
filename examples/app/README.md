# CCC Demo: Student Enrolment

A demo app exercising CCC's core features: sync deciders with optimistic concurrency, multi-entity context validation, uniqueness checks, and an async projector for read models.

## Setup

```bash
cd examples/ccc_app
bundle install
```

## Running

```bash
bundle exec falcon host
```

The app starts at `http://localhost:9292`.

## Domain

- **CourseDecider** -- enforces course name uniqueness (partitioned by `course_name`)
- **EnrolmentDecider** -- validates course exists, no duplicate students, max 20 per course (partitioned by `course_id`)
- **CourseCatalogProjector** -- async read model updated by background workers (partitioned by `course_id`)

Deciders run synchronously in the request (via the resource endpoints) or asynchronously via the generic `/commands` endpoint. The projector runs in the background via CCC's Dispatcher, so reads are eventually consistent.

## IRB

```bash
irb -r ./domain
```

```ruby
CourseApp.setup!
# Now you can use CCC.load, CCC.store.append, etc.
```

## Endpoints

### Generic command endpoint

```
POST /commands
```

Accepts any registered CCC message type. The command is appended to the store and processed asynchronously by background workers. Returns **202** on success.

| Field | Type | Required |
|-------|------|----------|
| `type` | string | yes |
| `payload` | object | yes |

```bash
curl -X POST http://localhost:9292/commands \
  -H 'Content-Type: application/json' \
  -d '{"type": "courses.create", "payload": {"course_id": "abc-123", "course_name": "Algebra"}}'
```

```json
{"id": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "type": "courses.create"}
```

Invalid payload returns **422** with field-level errors:

```bash
curl -X POST http://localhost:9292/commands \
  -H 'Content-Type: application/json' \
  -d '{"type": "courses.enrol", "payload": {"course_id": "", "student_id": ""}}'
```

```json
{"payload": {"course_id": "must be present", "student_id": "must be present"}}
```

Unknown message type returns **422**:

```json
{"error": "Unknown message type: foo.bar"}
```

### List courses

```
GET /
```

Returns the course catalog (populated by the async projector).

```bash
curl http://localhost:9292/
```

```json
[
  {"course_id": "abc-123", "course_name": "Algebra", "student_count": 2}
]
```

### Create a course

```
POST /courses
```

| Field | Type | Required |
|-------|------|----------|
| `course_name` | string | yes |

```bash
curl -X POST http://localhost:9292/courses \
  -H 'Content-Type: application/json' \
  -d '{"course_name": "Algebra"}'
```

```json
{"course_id": "abc-123", "course_name": "Algebra"}
```

Duplicate name returns **422**:

```bash
curl -X POST http://localhost:9292/courses \
  -H 'Content-Type: application/json' \
  -d '{"course_name": "Algebra"}'
```

```json
{"error": "Course 'Algebra' already exists"}
```

### Course detail

```
GET /courses/:id
```

```bash
curl http://localhost:9292/courses/abc-123
```

```json
{
  "course_id": "abc-123",
  "course_name": "Algebra",
  "students": ["stu-1", "stu-2"],
  "student_count": 1
}
```

Returns **404** if the course hasn't been projected yet or doesn't exist.

### Enrol a student

```
POST /courses/:id/enrolments
```

| Field | Type | Required |
|-------|------|----------|
| `student_id` | string | yes |

```bash
curl -X POST http://localhost:9292/courses/abc-123/enrolments \
  -H 'Content-Type: application/json' \
  -d '{"student_id": "stu-1"}'
```

```json
{"course_id": "abc-123", "student_id": "stu-1"}
```

Errors return **422**:

- Non-existent course: `{"error": "Course 'abc-123' does not exist"}`
- Duplicate student: `{"error": "Student 'stu-1' is already enrolled"}`
- Course full: `{"error": "Course is full (max 20 students)"}`

Concurrent modification returns **409**:

```json
{"error": "Concurrent modification — please retry"}
```

## Full walkthrough (resource endpoints)

```bash
# Create a course
curl -s -X POST http://localhost:9292/courses \
  -H 'Content-Type: application/json' \
  -d '{"course_name": "Algebra"}' | jq .
# Note the course_id from the response

# List courses (may take a moment for the projector to catch up)
curl -s http://localhost:9292/ | jq .

# Enrol students (replace <course_id> with the actual ID)
curl -s -X POST http://localhost:9292/courses/<course_id>/enrolments \
  -H 'Content-Type: application/json' \
  -d '{"student_id": "student-1"}' | jq .

curl -s -X POST http://localhost:9292/courses/<course_id>/enrolments \
  -H 'Content-Type: application/json' \
  -d '{"student_id": "student-2"}' | jq .

# View course detail
curl -s http://localhost:9292/courses/<course_id> | jq .

# Try duplicate name (422)
curl -s -X POST http://localhost:9292/courses \
  -H 'Content-Type: application/json' \
  -d '{"course_name": "Algebra"}' | jq .

# Try duplicate enrolment (422)
curl -s -X POST http://localhost:9292/courses/<course_id>/enrolments \
  -H 'Content-Type: application/json' \
  -d '{"student_id": "student-1"}' | jq .
```

## Full walkthrough (generic /commands endpoint)

```bash
# Create a course (async — processed by background workers)
curl -s -X POST http://localhost:9292/commands \
  -H 'Content-Type: application/json' \
  -d '{"type": "courses.create", "payload": {"course_id": "abc-123", "course_name": "Algebra"}}' | jq .

# Enrol a student
curl -s -X POST http://localhost:9292/commands \
  -H 'Content-Type: application/json' \
  -d '{"type": "courses.enrol", "payload": {"course_id": "abc-123", "student_id": "student-1"}}' | jq .

# List courses (may take a moment for workers to process)
curl -s http://localhost:9292/ | jq .

# Invalid payload (422)
curl -s -X POST http://localhost:9292/commands \
  -H 'Content-Type: application/json' \
  -d '{"type": "courses.create", "payload": {"course_id": "", "course_name": ""}}' | jq .

# Unknown type (422)
curl -s -X POST http://localhost:9292/commands \
  -H 'Content-Type: application/json' \
  -d '{"type": "nope", "payload": {}}' | jq .
```
