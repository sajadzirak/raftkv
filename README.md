# RaftKV — A Raft-Based Distributed Key-Value Store

A from-scratch implementation of the Raft consensus algorithm in Go, with a
replicated key-value store built on top. Built as a graduate-school
application project to demonstrate practical understanding of distributed
consensus, concurrent systems design, and fault-tolerant testing — not as a
port of any existing reference implementation (including MIT 6.5840's
labs, which cover the same algorithm).

## Overview

RaftKV implements the core of the Raft protocol as described in
*"In Search of an Understandable Consensus Algorithm"* (Ongaro & Ousterhout) [Paper Link](https://raft.github.io/raft.pdf),
and layers a simple key-value store on top of it as a replicated state
machine. The project's goal was depth over breadth: a single-cluster,
non-sharded system, but one whose safety and liveness properties are
verified through deliberate fault-injection testing rather than assumed.

## Architecture

The system is split into three layers, each with a single responsibility:

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   KVStore   │ ◄── │     Raft     │ ◄── │   Network   │
│ (state      │     │ (consensus:  │     │ (simulated  │
│  machine)   │     │  election,   │     │  RPC +      │
│             │     │  replication,│     │  fault      │
│             │     │  persistence)│     │  injection) │
└─────────────┘     └──────────────┘     └─────────────┘
```

- **Network** is a fully in-memory, in-process simulation of an unreliable
  network (see [Design Decisions](#design-decisions)). It knows nothing
  about Raft — its only job is routing calls between peers and, on demand,
  dropping or delaying them.
- **Raft** implements leader election, log replication, and persistence.
  It knows nothing about key-value semantics — it replicates opaque
  `Command` values and exposes an `ApplyHandler` interface so any state
  machine can be plugged in underneath it.
- **KVStore** is the replicated state machine: a `map[string]string`
  guarded by its own mutex, driven entirely by committed entries that
  `Raft` hands it via the apply hook.

Every node in the cluster owns its own independent `Raft` instance and its
own independent `KVStore` instance. Convergence across nodes is never
assumed — it's the thing the test suite verifies.

## Features Implemented

- **Leader election** — randomized election timeouts, term-based safety
  rules, majority-vote quorum, the log up-to-date check from §5.4.1.
- **Log replication** — `AppendEntries` with the full consistency check
  (`prevLogIndex`/`prevLogTerm`), conflict detection and truncation,
  `nextIndex`/`matchIndex` tracking with idempotent, monotonic updates
  under concurrent/out-of-order RPC replies.
- **Commit safety** — the leader only directly commits log entries from
  its *own current term*, with earlier-term entries committed only
  transitively once a current-term entry reaches majority (the Figure 8
  safety rule — see [Design Decisions](#design-decisions)).
- **Persistence** — `currentTerm`, `votedFor`, and the log are durably
  written to disk before a node relies on them being safe (e.g., before
  granting a vote or acknowledging replicated entries), so a crashed node
  can restart and rejoin the cluster without violating safety.
- **Replicated key-value store** — `Put`, `Get`, `Delete`, built on top of
  Raft's `Start()`/apply pipeline, with client-facing commit confirmation
  (poll-until-applied, with a term check to detect a command being
  silently superseded by a leadership change).
- **Fault-injection test harness** — simulated network partitions
  (bidirectional node isolation), simulated crash-and-restart, and
  automated safety/liveness assertions (no split-brain, bounded
  re-election time, log and KV-state convergence after recovery).

## Known Limitations / Scope Decisions

These were deliberate scope cuts made under a real time budget, not
oversights — each is something I identified, reasoned about, and chose to
defer rather than rush:

- **No log compaction / snapshotting.** The log grows unboundedly. A real
  deployment would need `InstallSnapshot` and periodic compaction; this
  was the first thing cut when time ran short, in favor of getting the
  core algorithm and testing right.
- **No Pre-Vote extension.** A node that's long-partitioned keeps
  incrementing its term while isolated (since nothing stops it from timing
  out and re-electing itself indefinitely). On rejoining, this inflated
  term can force an otherwise-healthy leader to step down and trigger an
  unnecessary re-election — the "disruptive server" problem described in
  §4.2.3 of the paper. This was found via this project's own fault
  injection tests, not read about — the Pre-Vote extension (not part of
  the original paper) is the standard fix, and is noted here as future
  work rather than implemented.
- **Reads are not linearizable.** `Get` checks if the node *thinks* it's 
leader but doesn't verify this with a quorum before reading. A partitioned 
old leader can continue serving stale reads until it receives a message 
with a higher term. Real systems use ReadIndex (sending a no-op through 
the log) or lease-based reads; out of scope here.

- **No cluster membership changes.** Cluster size is fixed at
  construction; joint consensus for adding/removing nodes was not
  implemented.
- **Persistence uses full-file rewrite**, not an append-only log — simpler
  and correct, but O(log size) per write rather than O(1). Fine at test
  scale, not production-scale.
- **The test harness's simulated "crash"** replaces a node's `Raft`
  instance reference and its `Network` registration, but does not
  explicitly halt the original instance's background goroutines (no
  `Stop()`/lifecycle mechanism was built). In rare timing windows this
  could allow a "zombie" pre-crash instance to still emit stray RPCs.
  Noted as a gap rather than fixed, given the time available.

## Testing

```bash
go test -race -v ./...
go test -race -count=5 ./...   # repeat to catch timing-sensitive flakiness
```

The suite includes:

- `TestLeaderElection` — verifies a single leader is elected, survives
  leader isolation with a strictly higher term on re-election, and that
  the original leader correctly steps down to Follower on rejoining.
- `TestLeaderElectionVariousSizes` — quorum/majority math checked at 3,
  5, and 7-node cluster sizes.
- `TestLogReplication` — commands replicate to the reachable majority; an
  isolated follower correctly starts empty and converges to the leader's
  exact log after being restored.
- `TestStartRejectsNonLeader` — client-facing safety check.
- `TestCrashRecovery` — a node's persisted state survives a simulated
  crash and process replacement, and the recovered node resumes live
  participation (not just static state — it picks up new entries
  committed after its restart).
- `TestKVConvergence` — after a sequence of `Put`/`Delete` operations
  spanning a follower isolation/recovery cycle, every node's independent
  `KVStore` map is byte-for-byte identical.
- `TestGetRejectsOnNonLeader` — client-facing safety check for reads.

All tests pass consistently under `-race` across repeated runs.

Development itself relied heavily on `go test -race` and `go run -race`:
several real concurrency bugs were only found this way, including two
unprotected concurrent map accesses in the network simulation layer (one
of which was actively causing an intermittent, hard-to-diagnose liveness
failure — "no leader found" — that looked at first like a Raft-level bug
but turned out to be corrupted reads in the fault-injection layer itself).

## Design Decisions Worth Noting

**In-process goroutine simulation instead of real sockets.** Each Raft
node runs as a goroutine, and the `Network` type routes calls directly
between them (with optional injected delay/drop), rather than using real
TCP connections. This was a deliberate choice over `net/rpc` sockets: it
makes fault injection (partitions, message loss, delay) fully
deterministic and controllable from the test harness, at the cost of not
exercising a real network stack.

**Coarse-grained locking.** Each `Raft` node is guarded by a single
mutex covering all of its state, rather than fine-grained per-field locks
or a fully channel-owned single-goroutine design (the alternative
considered). Given the project's scope, this was the right tradeoff:
Raft's critical sections are short, and a single lock is far easier to
reason about correctly than either fine-grained locking (more deadlock
surface) or a fully message-passing architecture (would have required
restructuring every RPC handler to route through a single owning
goroutine). This choice was deliberate, not a default.

**The commit-safety rule was the hardest correctness issue in the
project.** Early on, it's tempting to think "commit once a majority has
the entry" — but a leader can only safely *directly* advance its commit
index for entries from its own current term (Figure 8 in the paper);
earlier-term entries are only committed transitively as a side effect.
Getting this wrong would silently violate Raft's core safety guarantee
without necessarily showing up in casual testing. This project's test
suite doesn't have a dedicated adversarial test for this specific
scenario (it requires orchestrating a leader change mid-replication,
which was judged not worth the time investment for this scope) — it's
implemented correctly per the paper's rule and covered by code review
reasoning, but flagged here explicitly as an analytically-verified rather
than empirically-stress-tested property.

**`nextIndex`/`matchIndex` updates are idempotent and monotonic, not
relative increments.** Because multiple `AppendEntries` calls to the same
peer can be in flight concurrently (from overlapping heartbeat ticks and
immediate replication triggers) and can complete out of order, updating
these fields with `+=` on each reply is unsafe — a late, stale reply can
double-count or regress a value that a newer reply already advanced. Both
fields are instead updated via `max(newValue, currentValue)`, derived
directly from each reply's own request parameters rather than from
whatever the field currently holds.

## How to Run

```bash
go run main/main.go
```

Adjust `main.go` for demo scenarios, or run the test suite directly for
verification (see [Testing](#testing) above).