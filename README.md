# Observation Archive Collector

A small FastAPI service that collects per-station/per-day observation archives
from a directory-driven source, with two-level priority scheduling, pause/resume,
preemption, restart-safe progress and verify-before-download semantics.

## Components

| Service | Port | Purpose |
| --- | --- | --- |
| `collector` | 8000 | Collection API + OpenAPI docs at `/docs` |
| `source` | 9000 | Local mock observation source with strong ETags, Range, revisions, latency and fault injection |

## Run with Docker Compose

```bash
docker compose up --build
```

- API docs / demo entry point: <http://localhost:8000/docs>
- Root descriptor: <http://localhost:8000/> lists all endpoints
- Scheduler overview: <http://localhost:8000/scheduler>
- Source admin state: <http://localhost:9000/admin/state>

Docker is not required for the tests; they start both ASGI apps on loopback
ports in-process.

A scripted end-to-end demonstration (normal collection → urgent preemption →
automatic resume → manual pause → revision switch → verified download):

```bash
python scripts/demo.py
```

Local (non-Docker) run:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn mock_source.app:app --port 9000 &
SOURCE_BASE_URL=http://127.0.0.1:9000 COLLECTOR_DATA_DIR=./data \
  uvicorn app.main:app --port 8000
python scripts/demo.py
```

`POST /tasks` returns HTTP 201 for new tasks; an existing station+date returns
the current task (its original priority is kept).

## Priority scheduling

There are two priorities — `normal` (default) and `urgent` — and **at most one
collection task runs at any moment**; every other eligible task waits in a
single queue (urgent FIFO first, then normal FIFO).

- `POST /tasks` accepts `{"station", "date", "priority": "normal"|"urgent"}`;
  omitting `priority` creates a normal task.
- When an **urgent** task starts while a **normal** task is running, the normal
  download is paused immediately: its generation is fenced, its partial
  fragments on disk are retained, and it is returned to the queue with the
  reason `queued: paused to make room for urgent task …`.
- The urgent task starts right away. Once it **completes, fails, or is manually
  paused**, the scheduler automatically resumes the preempted normal task from
  its retained fragments (with `Range` + `If-Range`).
- An urgent task never preempts another urgent task; urgent tasks run FIFO.
- A **manual** pause (on a running or queued task) is sticky: such a task is
  never picked up automatically and only returns to work via an explicit
  `start`/`resume` call.
- Every task view carries `priority` and `queued_reason` (populated whenever the
  task is `queued`, otherwise `null`); `GET /scheduler` shows the running slot,
  the ordered queue and the next task to run.
- All scheduling decisions (enqueue, preempt, dispatch, release) are serialized
  under one in-process lock, so interleaved start/pause/refresh calls can never
  create two running tasks. A stale worker round — preempted, paused or
  superseded — is fenced by its generation: its late chunks/responses can
  neither rewrite the new fragments nor reclaim the slot.
- Restarts keep the queue and slot consistent: running tasks become resumable
  pauses, queued tasks stay queued, and with `AUTO_RESUME=true` only
  restart-interrupted and already-queued work is replayed — manual pauses stay
  paused. The in-memory slot is rebuilt empty and the single-slot invariant is
  re-established before any worker starts.

## API

| Method & path | Meaning |
| --- | --- |
| `POST /tasks` `{station,date,priority?}` | Create (or return the existing) task; one task per station+date, default priority `normal` |
| `POST /tasks/{id}/start` | Enqueue/start a queued/paused/failed task; urgent callers preempt a running normal task |
| `POST /tasks/{id}/pause` | Manual pause; returns only after in-flight writes can no longer land; never auto-resumed |
| `POST /tasks/{id}/resume` | Explicit resume from durable progress (alias of start) |
| `POST /tasks/{id}/refresh` | Re-read the directory for a completed task and re-collect if it moved; queues behind the slot |
| `GET /tasks`, `GET /tasks/{id}` | List / inspect tasks (state, priority, queued_reason, bytes, percent, expected vs verified size & SHA-256, error) |
| `GET /scheduler` | Single-slot overview: running task, ordered queue with reasons, next task id |
| `GET /tasks/{id}/events` | Append-only event log for the task (including `preempted` events) |
| `GET /tasks/{id}/package` | Download the archive; `409` unless a verified package exists |

`GET /tasks` accepts `?state=running|paused|completed|failed|queued`.
A refresh waiting in the queue keeps its previously verified package
downloadable; the package is withdrawn only when the new transfer actually
starts, and re-published after the new revision verifies.

## Correctness guarantees

1. **Directory is authoritative.** Each attempt reads size and SHA-256 from the
   source directory; nothing is trusted from an earlier attempt unless the whole
   identity (URL + strong ETag + size + digest) still matches.
2. **Resume only within one version.** The part file name embeds a hash of the
   source identity. If the directory reports a changed revision, partial bytes
   and metadata are discarded before a fresh transfer starts, so bytes from two
   versions are never concatenated.
3. **Range + If-Range.** Resumes send `Range: bytes=N-` and `If-Range: <strong
   ETag>`. A mismatched ETag, a wrong `Content-Range`, or a 412/416 causes a
   re-read instead of an append. A server that ignores Range but returns the
   same ETag is handled by truncating and restarting that version from zero
   (never appending a full body to partial data).
4. **Pause is a generation fence.** Pause bumps `generation` transactionally
   before responding; every worker write is a compare-and-set on
   `(state='running', generation)`. Late chunks/responses therefore cannot
   advance progress, mark completion, or publish a package after the pause
   response.
5. **Restart-safe.** Running tasks are flipped to `paused` at startup; the part
   file and byte count survive. With `AUTO_RESUME=true` only tasks interrupted by
   the restart (and queued tasks) resume automatically — explicit user pauses are
   left alone.
6. **Verify before exposure.** Completion requires the downloaded file's length
   and SHA-256 to equal the directory entry's. The file is written to a temp
   name and atomically renamed; the task becomes `completed` only after the
   rename. Until then — and for failed tasks — `GET .../package` returns 409.
   The directory is re-read once more just before publishing so a mid-transfer
   revision swap is discarded, not published.
7. **Failure keeps the reason.** Failed tasks retain `error`; transient failures
   (timeouts, 5xx, network) keep their partial prefix and can be retried via
   `start`; terminal failures (bad metadata, repeated verification/source
   mismatch) delete partial data.
8. **One slot, generation-fenced scheduling.** Only one transfer runs at a time;
   urgent preemptions, manual pauses, refreshes and restarts all bump the
   generation before they return, so a late response from an old round can never
   rewrite new fragments, publish a package, or reclaim the transfer slot.
9. **Manual vs automatic pauses are distinguished.** Preempted normal tasks are
   queued with a reason and auto-resume when the urgent task releases the slot;
   operator pauses are sticky and require an explicit resume.
10. **Refresh preserves the verified package.** A queued refresh keeps serving
    the old verified package; verification is invalidated only when the new
    transfer starts, and the package is re-published only after the new revision
    passes size and SHA-256 verification.

## Source mock controls

```bash
# switch the revision served for a station/date (changes body, size, digest)
curl -X POST localhost:9000/admin/revision \
  -H 'content-type: application/json' \
  -d '{"station":"54511","date":"2026-11-17","revision":"v2"}'

# add 300 ms header delay and 10 ms per-chunk delay
curl -X POST localhost:9000/admin/delay \
  -H 'content-type: application/json' -d '{"seconds":0.3,"chunk_seconds":0.01}'

# simulate outage (503) or a corrupted body (honest digest, tampered bytes)
curl -X POST localhost:9000/admin/fault \
  -H 'content-type: application/json' -d '{"station":"54511","date":"2026-11-17","mode":"fail"}'
```

`mode` accepts `fail`, `corrupt` or `clear`. `GET /admin/state` also shows the
recent package requests including their `Range`/`If-Range` headers.

## Tests

```bash
pytest -q
```

The suite runs the real API and the real mock source on loopback ports and
covers: full verified flow, duplicate task creation, pause with a late response
(progress/completion must not move), restart recovery with a real `Range`
resume, revision change mid-pause (old bytes discarded), corruption → failure
with no downloadable package, outage → failure with reason → successful retry,
refresh replacing a package on revision change, a source that ignores Range
(clean restart, no append), a pause issued before a delayed response arrives,
and input validation.

`tests/test_scheduling.py` additionally covers the two-level scheduler:
single-slot FIFO with queue reasons, urgent preemption of a running normal task
with retained fragments and automatic `Range` resume, urgent-vs-urgent FIFO (no
preemption), manual pauses that never auto-resume, slot release after urgent
failure or manual pause, stale preempted-round responses that cannot rewrite the
new run, refresh queuing behind an occupied slot while the old package stays
downloadable, restart consistency with and without `AUTO_RESUME`, and dense
interleaved start/pause/resume hammering that can never produce two running
tasks.
