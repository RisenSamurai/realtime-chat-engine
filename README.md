# 🚀 Real-Time Chat & Matchmaking Engine

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)
[![Architecture: Clean](https://img.shields.io/badge/Architecture-Clean%20%2F%20Hexagonal-blueviolet?style=flat-square)](#architecture)
[![Observability](https://img.shields.io/badge/Observability-OpenTelemetry%20%2B%20slog-orange?style=flat-square)](#observability--reliability)
[![Tests: Passing](https://img.shields.io/badge/Tests-Passing-brightgreen?style=flat-square)](#testing--verification)

> **High-performance, production-ready Go backend engine** featuring real-time WebSockets, distributed Redis Pub/Sub, bounded worker pools with graceful drain, vector search matchmaking via PostgreSQL `pgvector`, and zero-leak distributed rate limiting.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [System Architecture](#-system-architecture)
- [Key Engineering Highlights](#-key-engineering-highlights)
  - [1. Real-Time WebSockets & Slow Consumer Protection](#1-real-time-websockets--slow-consumer-protection)
  - [2. Bounded Worker Pool & Graceful Shutdown](#2-bounded-worker-pool--graceful-shutdown)
  - [3. Two-Stage Matchmaking Retrieval (pgvector + Heuristic)](#3-two-stage-matchmaking-retrieval-pgvector--heuristic)
  - [4. Distributed Atomic Rate Limiter (Redis Pipelining)](#4-distributed-atomic-rate-limiter-redis-pipelining)
  - [5. Concurrency Limiter & Lock-Free Slot Pool](#5-concurrency-limiter--lock-free-slot-pool)
- [Project Structure](#-project-structure)
- [Tech Stack & Dependencies](#-tech-stack--dependencies)
- [Getting Started](#-getting-started)
- [Testing & Verification](#-testing--verification)

---

## 🌟 Overview

Modern real-time applications must balance high message throughput with resource isolation and predictable memory usage. This project isolates and demonstrates battle-tested Go concurrency and infrastructure patterns:

- **Prevents Slow Consumer Head-of-Line Blocking**: Slow mobile or throttled clients cannot exhaust server goroutines or memory.
- **Zero-Loss Graceful Termination**: Server drain cycles guarantee in-flight database persistence before exit signals (`SIGINT`/`SIGTERM`) finish.
- **Sub-Millisecond Multi-Stage Recommendations**: Hybrid vector similarity combined with relational criteria and custom weight distributions.
- **Fail-Safe Rate Limiting**: Distributed quota enforcement that never leaks transient keys into Redis on network hiccups.

---

## 🏗 System Architecture

```mermaid
flowchart TD
    subgraph Clients["Clients & Gateways"]
        ClientA["WebSocket Client A"]
        ClientB["WebSocket Client B (Slow Consumer)"]
    end

    subgraph Server["Go Real-Time Server Engine"]
        Router["Gin HTTP / WS Router"]
        RL["Distributed Rate Limiter<br/>(Redis Pipeline)"]
        SLog["Structured slog Middleware"]

        subgraph RoomHandler["Room WebSocket Handler"]
            IDOR["IDOR Security Validator"]
            Heartbeat["Heartbeat Ping/Pong (30s)"]
            SlowProtect["Non-blocking Client Queue<br/>(Slow Consumer Drop Warning)"]
        end

        subgraph WorkerPool["Bounded RoomWorkerPool"]
            PersistQueue["Persist Queue (1000 buffer)"]
            PersistWorkers["8x Persistence Workers"]
            AIQueue["AI Trigger Queue (100 buffer)"]
            AIWorkers["4x AI Workers"]
        end

        subgraph MatchingEngine["Matchmaking Engine"]
            ErrGroup["errgroup.WithContext (Parallel Fetch)"]
            PGVector["SQL + pgvector Distance (<=>)"]
            Heuristic["In-Memory Trait Scoring"]
            BatchInsert["pgx.Batch Insert"]
        end
    end

    subgraph DataPlane["Data Plane"]
        RedisPubSub[("Redis Pub/Sub<br/>room:{id}")]
        RedisKV[("Redis KV / Rate Limit")]
        Postgres[("PostgreSQL + pgvector<br/>(pgxpool + OpenTelemetry)")]
    end

    ClientA -->|HTTP / WS| Router
    ClientB -->|HTTP / WS| Router
    Router --> SLog --> RL
    RL --> RedisKV
    RL --> IDOR --> RoomHandler

    RoomHandler -->|Publish Message| RedisPubSub
    RedisPubSub -->|Fan-Out Frame| SlowProtect --> ClientA
    RedisPubSub -->|Fan-Out Frame| SlowProtect -->|Frame Drop if Buffer Full| ClientB

    RoomHandler -->|EnqueuePersist (Non-blocking)| PersistQueue
    PersistQueue --> PersistWorkers -->|Persist Message| Postgres
    PersistWorkers -->|EnqueueAI| AIQueue --> AIWorkers

    MatchingEngine --> ErrGroup
    ErrGroup --> Postgres
    PGVector --> Heuristic --> BatchInsert --> Postgres
```

---

## 💎 Key Engineering Highlights

### 1. Real-Time WebSockets & Slow Consumer Protection
*File:* [`internal/chat/room_ws.go`](internal/chat/room_ws.go)

A common vulnerability in real-time chat engines is the **Slow Consumer** problem: if a client's network connection degrades or pauses TCP ACKs, a blocking write to that client's socket stalls the broadcaster, causing message buffers to grow uncontrollably until the server crashes with Out-Of-Memory (OOM).

**Solution implemented:**
- Dedicated bounded per-connection buffer channel (`clientWriteCh := make(chan []byte, 64)`).
- The Redis subscriber reads incoming room broadcasts and performs a **non-blocking write** (`select-default`).
- If a client's buffer fills up, lagging frames are dropped with structured warning logs, preserving overall system stability:
```go
select {
case clientWriteCh <- []byte(msg.Payload):
default:
    h.logger.WarnContext(wsCtx, "slow consumer detected on websocket, dropped frame",
        slog.String("user_id", userID),
        slog.String("room_id", roomID.String()),
    )
}
```
- Includes 30-second ping/pong heartbeats to reap dead TCP sockets and strict frame size limits (`SetReadLimit(8192)`).

---

### 2. Bounded Worker Pool & Graceful Shutdown
*File:* [`internal/chat/worker_pool.go`](internal/chat/worker_pool.go)

Database writes for chat history should never block the WebSocket read/write loops.

**Solution implemented:**
- **Separate Bounded Queues**: Message persistence (`persistQueue`) and asynchronous background triggers (`aiQueue`) are decoupled into isolated worker pools.
- **Graceful Draining**: When `Stop()` is called (during SIGINT/SIGTERM), workers stop accepting new inputs, switch to drain mode with dedicated context timeouts, and flush remaining items before exiting:
```go
case <-ctx.Done():
    // Drain remaining items before quitting
    for {
        select {
        case event := <-pool.persistQueue:
            drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
            _ = pool.repo.SaveMessage(drainCtx, event)
            drainCancel()
        default:
            return
        }
    }
```

---

### 3. Two-Stage Matchmaking Retrieval (pgvector + Heuristic)
*File:* [`internal/matching/service.go`](internal/matching/service.go)

A hybrid recommendation engine tailored for low latency and high quality matches:
1. **Relational & Vector Stage (SQL)**: Uses PostgreSQL's `pgvector` cosine similarity operator (`<=>`) inside a Common Table Expression (CTE) combined with hard relational filtering (game overlap, negative match proposal status exclusions).
2. **Parallel Fetching**: Executes source user profile and persona configuration queries concurrently using `golang.org/x/sync/errgroup`.
3. **In-Memory Trait Scoring**: Calculates non-linear psychological penalties (tilt resistance, toxicity tolerance) and gaming schedule overlap.
4. **Batch Persistence**: Inserts computed proposals in bulk using `pgx.Batch`, avoiding N+1 roundtrips.
5. **Modern Slice Sorting**: Leverages Go 1.21+ `slices.SortFunc` for zero-allocation sorting.

---

### 4. Distributed Atomic Rate Limiter (Redis Pipelining)
*File:* [`internal/middleware/rate_limiter.go`](internal/middleware/rate_limiter.go)

A distributed sliding/fixed-window rate limiter designed to withstand process restarts without leaving orphan keys:
- **Redis Pipelining**: Executes `INCR` and `EXPIRE` together in a single pipeline roundtrip.
- **Cascading Identification**: Gracefully falls back across `Authenticated UserID -> Bearer Token Prefix -> Client IP`.
- **RFC Standard Headers**: Sends `X-RateLimit-Limit` and `X-RateLimit-Remaining` with zero allocations.

---

### 5. Concurrency Limiter & Lock-Free Slot Pool
*Files:* [`pkg/concurrency/limiter.go`](pkg/concurrency/limiter.go) & [`pkg/concurrency/limiter_test.go`](pkg/concurrency/limiter_test.go)

A high-performance concurrency throttle based on buffered channel semaphores:
- Exposes `Acquire(ctx)`, non-blocking `TryAcquire()`, and `Do(ctx, fn)`.
- Respects `context.Context` cancellation and deadline timeouts.
- Tested under rigorous concurrent loads using `sync/atomic` and `CompareAndSwap` assertions to guarantee zero capacity leaks.

---

## 📁 Project Structure

```text
.
├── cmd/
│   └── server/
│       └── main.go                 # Application entrypoint & graceful shutdown
├── internal/
│   ├── chat/
│   │   ├── models.go               # Domain entities, DTOs & repository contracts
│   │   ├── room_ws.go              # WebSocket handler, Redis PubSub & slow consumer protection
│   │   └── worker_pool.go          # Bounded worker pool with drain logic & backpressure
│   ├── cron/
│   │   └── room_cleaner.go         # Periodic inactivity cleaner with context cancellation
│   ├── database/
│   │   └── postgres.go             # pgxpool connection, migrations & OpenTelemetry tracing
│   ├── matching/
│   │   └── service.go              # 2-stage matchmaking (pgvector + errgroup + heuristic)
│   └── middleware/
│       ├── logger.go               # Structured slog middleware with latency tracking
│       └── rate_limiter.go         # Redis pipeline rate limiter with cascading identifiers
├── pkg/
│   ├── concurrency/
│   │   ├── limiter.go              # Channel-based concurrency slot limiter
│   │   └── limiter_test.go         # Concurrent unit tests verifying bounds and timeouts
│   └── httputil/
│       └── proxy.go                # Generic S2S JSON reverse proxy with type-safe binding
├── go.mod
├── go.sum
├── LICENSE
└── README.md
```

---

## 🛠 Tech Stack & Dependencies

| Area | Technologies | Rationale |
| :--- | :--- | :--- |
| **Language** | Go 1.24+ | Modern standard library (`log/slog`, `slices`, `sync/atomic`) |
| **HTTP Framework** | [Gin Gonic](https://github.com/gin-gonic/gin) | High throughput, minimal memory overhead |
| **WebSockets** | [coder/websocket](https://github.com/coder/websocket) | Idiomatic context-aware Go WebSockets (replaces gorilla) |
| **Database Driver** | [jackc/pgx/v5](https://github.com/jackc/pgx/v5) | High performance native PostgreSQL connection pooling & batching |
| **Vector Search** | PostgreSQL `pgvector` | Native relational + embedding cosine distance queries (`<=>`) |
| **Caching / PubSub**| [go-redis/v9](https://github.com/redis/go-redis/v9) | Pipelined operations, distributed Pub/Sub |
| **Observability** | [exaring/otelpgx](https://github.com/exaring/otelpgx), `log/slog` | OpenTelemetry distributed tracing & structured JSON logging |
| **Concurrency** | `golang.org/x/sync/errgroup` | Structured concurrency & error propagation |

---

## 🚀 Getting Started

### Prerequisites
- **Go 1.24+**
- **PostgreSQL 15+** with the `pgvector` extension
- **Redis 7+**

### Installation

```bash
# Clone the repository
git clone https://github.com/RisenSamurai/realtime-chat-engine.git
cd realtime-chat-engine

# Download dependencies
go mod download
```

### Environment Configuration

Create a `.env` file in the project root:

```env
PORT=8080
APP_ENV=development

# Database
DB_USER=postgres
DB_PASS=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=chat_db
DB_MAX_CONNS=25

# Redis
REDIS_ADDR=localhost:6379

# Security
ALLOWED_ORIGINS=localhost:*,127.0.0.1:*
```

### Running the Application

```bash
# Build and run the server
go run cmd/server/main.go
```

---

## 🧪 Testing & Verification

Run the test suite with race detection enabled:

```bash
# Execute unit & concurrency tests
go test -v -race ./...
```

Sample test output:
```text
=== RUN   TestLimiter_Capacity
--- PASS: TestLimiter_Capacity (0.16s)
=== RUN   TestLimiter_Timeout
--- PASS: TestLimiter_Timeout (0.05s)
PASS
ok      github.com/RisenSamurai/realtime-chat-engine/pkg/concurrency    1.119s
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
