# Order Service with Transactional Outbox Pattern

An order service in Rust that uses the **transactional outbox pattern** to guarantee a database write and a message publish always happen together — no dual-write gap, no lost events.

Built with **Rust**, **Axum**, **sqlx**, **PostgreSQL**, and **RabbitMQ**.

---

## The problem this solves

Every service that writes to a database and publishes to a message broker has a hidden reliability gap:

```
1. INSERT INTO orders ...         // succeeds
2. publish("order.created")       // process crashes here
```

The order exists. The fulfilment service never hears about it. The inconsistency is silent and permanent — no error, no alert, no retry. This is the **dual-write problem**.

The outbox pattern closes this gap by making the message a database row. Both writes happen in the same transaction:

```
1. INSERT INTO orders ...         
2. INSERT INTO outbox_messages ...   one transaction — both or neither
3. Poller reads outbox -> publishes to RabbitMQ (separate process)
```

Once the transaction commits, the message is guaranteed to eventually be published — even if the poller crashes repeatedly.

---

## Guarantees

- **Atomic dual write** — an order can never exist without a corresponding outbox row, and vice versa
- **At-least-once delivery** — if an order commits, its message will eventually reach RabbitMQ
- **Concurrent poller safety** — `FOR UPDATE SKIP LOCKED` ensures concurrent pollers never claim the same message simultaneously
- **Crash recovery** — a TTL-based lock (`locked_until`) means a message claimed by a crashed poller is automatically reclaimed after 30 seconds

> **Note:** This pattern guarantees at-least-once delivery, not exactly-once. Consumers must be idempotent.

---

## Architecture


```mermaid
flowchart TD
    Client["Client (HTTP)"] -->|"POST /orders"| Handler["HTTP Handler"]

    subgraph OrderService["Order Service"]
        direction TB
        Handler --> Txn["Single Transaction"]
        Txn --> Orders["INSERT INTO orders"]
        Txn --> Outbox["INSERT INTO outbox_messages"]

        Poller["Background Poller (every 2s)"] -->|"SELECT ... FOR UPDATE SKIP LOCKED"| Claim["Claim pending messages"]
        Claim --> Publish["Publish to RabbitMQ"]
        Publish --> Mark["UPDATE status = 'published'"]
    end

    Publish --> RabbitMQ["RabbitMQ"]
    RabbitMQ --> Downstream["Downstream Services"]
```

---

## How it Works

### Happy path — new order

```mermaid
sequenceDiagram
    participant Client
    participant Handler as HTTP Handler
    participant DB as PostgreSQL
    participant Poller as Background Poller
    participant MQ as RabbitMQ

    Client->>+Handler: POST /orders
    Handler->>+DB: BEGIN TRANSACTION
    Handler->>DB: INSERT INTO orders
    Handler->>DB: INSERT INTO outbox_messages (status='pending')
    Handler->>DB: COMMIT
    DB-->>-Handler: committed
    Handler-->>-Client: 201 Created (order_id)

    Note over Poller,MQ: Runs independently every 2s

    Poller->>+DB: SELECT ... FOR UPDATE SKIP LOCKED
    DB-->>-Poller: batch of pending messages
    Poller->>+MQ: publish order.created
    MQ-->>-Poller: ack (publisher confirm)
    Poller->>DB: UPDATE status = 'published'
```

### Crash recovery — poller restarts after failure

```mermaid
sequenceDiagram
    participant DB as PostgreSQL
    participant Poller as Background Poller
    participant MQ as RabbitMQ

    Poller->>+DB: SELECT ... FOR UPDATE SKIP LOCKED
    DB-->>-Poller: batch claimed (locked_until = now + 30s)
    Poller-xPoller:  crash before publish

    Note over Poller,DB: locked_until expires after 30s

    Poller->>+DB: SELECT ... FOR UPDATE SKIP LOCKED
    DB-->>-Poller: same message reclaimed
    Poller->>+MQ: publish order.created
    MQ-->>-Poller: ack (publisher confirm)
    Poller->>DB: UPDATE status = 'published'
```

---

## Database Schema

```mermaid
erDiagram
    orders {
        uuid id PK
        uuid customer_id
        bigint amount
        text status
        timestamptz created_at
    }
    outbox_messages {
        uuid id PK
        text event_type
        jsonb payload
        text aggregate_id
        text status
        int attempts
        timestamptz locked_until
        text last_error
        timestamptz published_at
        timestamptz created_at
    }
    orders ||--|| outbox_messages : "produces"
```

- **`amount BIGINT`** — money stored as cents, never as a float
- **`CHECK (amount > 0)`** — invalid amounts rejected at the database level
- **`payload JSONB`** — message content stored verbatim, no recomputation on replay
- **`locked_until TIMESTAMPTZ`** — TTL-based lock; expired locks are automatically reclaimed by the next poller tick
- **`FOR UPDATE SKIP LOCKED`** — concurrent pollers skip rows already claimed, preventing duplicate processing
- **`outbox_pending_idx`** — partial index on `status = 'pending'` keeps polling fast as the table grows

---

## API

### Create an order

```
POST /orders
Content-Type: application/json
```

```json
{
  "customer_id": "a0000000-0000-0000-0000-000000000001",
  "amount": 2999
}
```

**Response `201`**

```json
{
  "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

`amount` is in cents. `2999` = £29.99.

The response returns as soon as the transaction commits. The `order.created` event is published to RabbitMQ asynchronously by the background poller.

---

## Running Locally

### Prerequisites

- [Rust](https://rustup.rs/)
- [Docker](https://www.docker.com/)
- `sqlx-cli`: `cargo install sqlx-cli --no-default-features --features postgres`

### With Docker Compose (full stack)

```bash
docker compose --profile app up --build
```

Services:
- **postgres** — port `5433`
- **rabbitmq** — port `5672` (AMQP), port `15672` (management UI at `http://localhost:15672`, guest/guest)
- **app** — port `3000`

### For local development

```bash
# 1. Start infrastructure
docker compose up -d postgres rabbitmq

# 2. Copy and configure environment
cp .env.example .env
```

`.env`:
```
DATABASE_URL=postgres://postgres:password@localhost:5433/order-service-outbox
AMQP_URL=amqp://guest:guest@localhost:5672/%2f
RUST_LOG=order_service_with_outbox_pattern=debug
```

```bash
# 3. Run migrations
sqlx migrate run

# 4. Run the service
cargo run
```

The HTTP server starts on `http://localhost:3000`. The background poller starts automatically.

---

## Running Tests

The tests require both Postgres and RabbitMQ running.

```bash
# Start infrastructure
docker compose up -d postgres rabbitmq

# Run migrations (first time only)
sqlx migrate run

# Run tests
cargo test
```

Tests use `serial_test` to run sequentially and truncate all tables between runs — each test starts with a clean database.

### Test coverage

| Test | What it verifies |
|---|---|
| `create_order_writes_both_rows_atomically` | An order and its outbox message are written in one transaction — both exist or neither does |
| `the_dual_write_gap_is_visible_naively_and_closed_by_outbox` | Demonstrates the gap a naive implementation creates, then proves the outbox pattern closes it |
| `poller_publishes_order_message_with_correct_payload` | End-to-end: order created → poller picks it up → message published to RabbitMQ → status updated |
| `concurrent_pollers_never_claim_same_message` | 3 concurrent pollers claim 10 messages — no message is claimed twice (`FOR UPDATE SKIP LOCKED`) |
| `publish_failure_marks_message_failed_and_leaves_order_intact` | A failed publish marks the message `failed` with the error reason — the order is unchanged |

---

## Project Structure

```
src/
  main.rs       — HTTP server + poller startup, graceful shutdown
  db.rs         — All database queries (orders + outbox)
  poller.rs     — Background polling loop with configurable batch + TTL
  publisher.rs  — RabbitMQ publisher with publisher confirms
  types.rs      — Domain types (OrderId, Money, OutboxMessage, ...)
  error.rs      — OutboxError enum + HTTP response mapping
migrations/
  001_schema.sql
tests/
  outbox_tests.rs
```

---

## Key Design Decisions

### `FOR UPDATE SKIP LOCKED` — concurrent poller safety

The poller claims messages using a single atomic `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)` query:

```sql
UPDATE outbox_messages
SET    status       = 'processing',
       attempts     = attempts + 1,
       locked_until = now() + ($1 || ' seconds')::interval
WHERE  id IN (
    SELECT id FROM outbox_messages
    WHERE  status = 'pending'
    AND    (locked_until IS NULL OR locked_until < now())
    ORDER  BY created_at
    LIMIT  $2
    FOR UPDATE SKIP LOCKED
)
RETURNING id, event_type, payload, aggregate_id, attempts, created_at
```

`SKIP LOCKED` means concurrent pollers skip rows already claimed by another instance — no message is ever processed twice simultaneously.

### TTL-based lock — crash recovery

If the poller crashes after claiming a batch but before marking messages published, `locked_until` expires after 30 seconds. The next poller tick picks up the same messages and retries — no manual intervention needed.

This is why consumers must be idempotent — a message may be delivered more than once.

### Graceful shutdown

The HTTP server listens for `SIGINT`, signals the poller via a `watch::channel`, and the poller finishes its current batch before exiting. No messages are abandoned mid-batch.

---

## Environment Variables

| Variable | Description | Example |
|---|---|---|
| `DATABASE_URL` | Postgres connection string | `postgres://postgres:password@localhost:5433/order-service-outbox` |
| `AMQP_URL` | RabbitMQ connection string | `amqp://guest:guest@localhost:5672/%2f` |
| `RUST_LOG` | Log level filter | `order_service_with_outbox_pattern=debug` |

Copy `.env.example` to `.env` to get started.

---

## Tech Stack

| | |
|---|---|
| Language | Rust |
| HTTP framework | [Axum](https://github.com/tokio-rs/axum) 0.7 |
| Database | PostgreSQL 16 |
| Database driver | [sqlx](https://github.com/launchbadge/sqlx) 0.7 (compile-time query verification) |
| Message broker | RabbitMQ 4 |
| AMQP client | [lapin](https://github.com/amqp-rs/lapin) |
| Async runtime | [Tokio](https://tokio.rs) |
| Logging | [tracing](https://github.com/tokio-rs/tracing) + tracing-subscriber (JSON) |
| Container | Docker (multi-stage, distroless runtime) |
