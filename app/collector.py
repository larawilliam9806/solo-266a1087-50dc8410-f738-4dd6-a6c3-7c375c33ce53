"""Download worker: resumable, ETag-guarded, verified by size and SHA-256.

Invariants enforced here
------------------------
* Every worker-side mutation is a compare-and-set from ``state='running'`` with
  the generation the worker started with.  Pausing (or a newer start) bumps the
  generation *before* the HTTP call returns, so a late chunk or late response
  can never advance, complete or overwrite a paused task.
* Resumption is only allowed against the exact source identity recorded in the
  database (URL + strong ETag + size + digest + revision).  If the directory
  reports a different identity, the old part file is discarded and a fresh
  transfer begins from the new directory entry.
* Before publishing, the directory is re-read: a package assembled from a
  revision that was superseded mid-transfer is discarded, never stitched.
* A package becomes downloadable only after size and SHA-256 both match the
  directory entry *and* the file is atomically renamed into place; only then is
  the task marked completed.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from .config import Settings
from .models import CatalogEntry
from .store import Store, utcnow

log = logging.getLogger("collector.worker")


class Resync(Exception):
    """The source changed underneath us; re-read the directory and restart."""


class FatalSourceError(Exception):
    """The directory cannot be trusted (bad metadata, missing entry, ...)."""


class Interrupted(Exception):
    """The task was paused, superseded or shut down mid-transfer."""


class TransferError(Exception):
    """A transient or permanent HTTP failure."""


@dataclass
class RunningTask:
    task_id: str
    generation: int
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None


# Why a task has been placed in the queue.  The reason is stored durably so a
# restart can still explain why a task is waiting.
REASON_WAITING = "queued: waiting for an available transfer slot"
REASON_REFRESH = "queued: refresh requested; waiting for the transfer slot"
REASON_URGENT_AHEAD = "queued: {n} urgent task(s) ahead"
REASON_PREEMPTED = (
    "queued: paused to make room for urgent task {urgent}; "
    "partial fragments retained, will resume automatically"
)


class Collector:
    def __init__(
        self,
        settings: Settings,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.settings = settings
        self.store = Store(settings.database_path)
        settings.parts_dir.mkdir(parents=True, exist_ok=True)
        settings.packages_dir.mkdir(parents=True, exist_ok=True)
        self._runs: dict[str, RunningTask] = {}
        self._tasks: set[asyncio.Task] = set()
        self._client: httpx.AsyncClient | None = http_client
        self._owns_client = http_client is None
        # The scheduler is a single-slot, in-process arbiter.  Every decision
        # (enqueue, preempt, dispatch, release) runs under this lock, so
        # interleaved start/pause/refresh requests can never observe or create
        # two running tasks.
        self._sched_lock = asyncio.Lock()
        self._slot: str | None = None
        # Tasks that should surface a custom "resume" event when the scheduler
        # picks them up (survives only within a process, like the workers do).
        self._resume_note: set[str] = set()
        # Set while the process is shutting down: a worker whose transfer is torn
        # down then reports an interruption (resumable), not a failure.
        self._shutting_down = False

    # -- lifecycle -------------------------------------------------------
    async def startup(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.settings.http_timeout, connect=5.0),
                follow_redirects=True,
                headers={"User-Agent": "observation-collector/1.0"},
            )
        # Recover from the live table: after a graceful shutdown workers already
        # moved their own rows to 'paused', while a hard crash leaves 'running'
        # rows that this call normalizes.  Read the current state rather than a
        # snapshot taken before recovery.
        self._shutting_down = False
        self._recover_interrupted()
        if self.settings.auto_resume:
            # Tasks interrupted mid-transfer (now paused with the restart marker)
            # and tasks already sitting in the queue resume automatically.  An
            # operator's explicit pause is respected and never auto-started.
            candidates: list[str] = []
            for row in self.store.list():
                if row["state"] == "queued":
                    candidates.append(row["id"])
                elif row["state"] == "paused" and (row["error"] or "").startswith(
                    "interrupted by process"
                ):
                    candidates.append(row["id"])
            for task_id in dict.fromkeys(candidates):
                row = self.store.get(task_id)
                if row and row["state"] in {"paused", "queued"}:
                    await self.start(task_id)
        else:
            self._refresh_queue_reasons()

    async def shutdown(self) -> None:
        self._shutting_down = True
        for run in list(self._runs.values()):
            run.cancel.set()
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=10)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        # Whatever was running when the process stopped is resumable rather than
        # lost; the in-memory slot dies with the process.
        running = [row["id"] for row in self.store.list("running")]
        self._recover_interrupted(running)
        self._slot = None
        try:
            self.store.close()
        except Exception:  # noqa: BLE001 - shutdown must not mask the real error
            log.warning("could not close the SQLite connection", exc_info=True)
        if self._owns_client and self._client:
            await self._client.aclose()
            self._client = None

    def _recover_interrupted(self, task_ids: list[str] | None = None) -> None:
        """Normalize rows left ``running`` by a dead process into resumable pauses.

        Workers already convert their own tasks during a graceful shutdown, so
        this mainly covers hard crashes where no worker got the chance to run.
        Only ``running`` rows are touched: queued tasks must remain queued.
        """
        rows = (
            (self.store.get(task_id) for task_id in task_ids)
            if task_ids is not None
            else self.store.list("running")
        )
        for row in rows:
            if row is None or row["state"] != "running":
                continue
            self.store.cas_state(
                row["id"],
                "running",
                "paused",
                expected_generation=row["generation"],
                generation=row["generation"] + 1,
                error="interrupted by process restart; resumable",
                queued_reason=None,
            )
            self.store.event(row["id"], "interrupted", "resumable after restart")

    # -- public operations ----------------------------------------------
    def create_task(self, task_id: str, station: str, date: str, priority: str = "normal") -> None:
        self.store.insert_task(task_id, station, date, priority)

    async def start(self, task_id: str) -> dict[str, str]:
        """Enqueue a task for the single transfer slot and try to schedule.

        Urgent callers preempt a running *normal* task: its worker is fenced off
        by a generation bump and its partial fragments are retained while the
        urgent task takes the slot.  A manually paused task is only ever moved
        back into the queue by this explicit call, never by the scheduler.
        """
        async with self._sched_lock:
            row = self.get_row(task_id)
            state = row["state"]
            if state == "completed":
                return {"state": "completed", "detail": "already completed"}
            if state == "running":
                return {"state": "running", "detail": "already running"}

            if state == "failed":
                # Retrying a terminal failure starts from zero; transient
                # failures keep their valid partial prefix for a Range resume.
                row_part = Path(row["part_path"]) if row["part_path"] else None
                if not row_part or not row_part.exists() or row["downloaded_bytes"] <= 0:
                    self._discard_parts(task_id)
                    self.store.update(task_id, downloaded_bytes=0, part_path=None)

            urgent = row["priority"] == "urgent"
            preempted: str | None = None

            if self._slot is not None and urgent:
                holder = self.store.get(self._slot)
                if holder is not None and holder["state"] == "running" and holder["priority"] != "urgent":
                    preempted = await self._preempt_locked(holder, task_id)

            if state == "queued":
                # Already queued: keep any specific reason (preemption, pending
                # refresh) and fall through to dispatch — the task may now be the
                # head of the queue after a preemption.
                if not (row["queued_reason"] or "").startswith(
                    ("queued: paused to make room", "queued: refresh")
                ):
                    self.store.set_queued_reason(task_id, REASON_WAITING)
            else:
                self._enqueue_locked(task_id, state)

            await self._dispatch_locked()
            current = self.store.get(task_id)
            if current["state"] == "running":
                return {"state": "running", "detail": "started"}
            if preempted:
                return {
                    "state": "queued",
                    "detail": f"queued; urgent slot occupied, preempted normal task {preempted}",
                }
            return {"state": "queued", "detail": current["queued_reason"] or REASON_WAITING}

    async def refresh(self, task_id: str) -> dict[str, str]:
        """Re-read the directory for a completed task; collect again if it moved.

        Refresh goes through the same single slot as every other transfer, so a
        refresh requested while another task runs waits in the queue instead of
        opening a second concurrent download.
        """
        async with self._sched_lock:
            row = self.get_row(task_id)
            if row["state"] != "completed":
                return {"state": row["state"], "detail": "refresh requires a completed task"}
            generation = row["generation"] + 1
            if not self.store.cas_state(
                task_id,
                "completed",
                "queued",
                expected_generation=row["generation"],
                generation=generation,
                error=None,
                attempts=row["attempts"] + 1,
                # The verified package stays exposed while the refresh is queued;
                # the verification columns are only cleared when the transfer
                # actually starts (in _dispatch_locked).
                queued_reason=REASON_REFRESH,
            ):
                return {"state": "unknown", "detail": "state changed concurrently"}
            self.store.event(task_id, "refresh", "re-reading source directory")
            await self._dispatch_locked()
            current = self.store.get(task_id)
            if current["state"] == "running":
                return {"state": "running", "detail": "refreshing"}
            return {"state": "queued", "detail": current["queued_reason"] or REASON_REFRESH}

    async def pause(self, task_id: str) -> dict[str, str]:
        """Operator pause.  Manually paused tasks are never auto-resumed."""
        async with self._sched_lock:
            row = self.get_row(task_id)
            state = row["state"]
            if state == "paused":
                return {"state": "paused", "detail": "already paused"}
            if state == "completed":
                return {"state": "completed", "detail": "already completed"}
            if state == "failed":
                return {"state": "failed", "detail": "task failed; reason retained"}

            # Bump the generation *first*.  Even if a response is in flight right
            # now, its generation guard will reject every subsequent write, so a
            # late chunk from an old round can never rewrite the new fragments.
            moved = self.store.cas_state(
                task_id,
                state,
                "paused",
                expected_generation=row["generation"],
                generation=row["generation"] + 1,
                error=None if state == "running" else row["error"],
                queued_reason=None,
            )
            if not moved:
                current = self.store.get(task_id)
                return {
                    "state": current["state"] if current else "unknown",
                    "detail": "state changed concurrently",
                }
            self._resume_note.discard(task_id)

            was_holder = self._slot == task_id
            run = self._runs.get(task_id)
            if run:
                run.cancel.set()

            if state == "running":
                # Release the slot before awaiting so dispatch decisions stay
                # serialized; then wait (bounded) for the worker to stop writing.
                if was_holder:
                    self._slot = None
                if run and run.task:
                    await asyncio.wait({run.task}, timeout=5)
                row = self.store.get(task_id)
                # A transfer that finished between the CAS and the cancel has
                # completed legitimately; report it as such rather than lying.
                if row and row["state"] == "completed":
                    await self._dispatch_locked()
                    return {"state": "completed", "detail": "completed before pause took effect"}
                if row and row["state"] == "paused" and row["part_path"]:
                    part = Path(row["part_path"])
                    if part.exists():
                        # Now that no writer remains, sync the reported progress
                        # with the durable bytes on disk.
                        self.store.update(
                            task_id,
                            downloaded_bytes=min(part.stat().st_size, row["expected_size"] or 0),
                        )
            await self._dispatch_locked()
            self.store.event(task_id, "pause_requested", "manual pause; not auto-resumed")
            return {"state": "paused", "detail": "paused manually; progress retained"}

    def get_row(self, task_id: str):
        row = self.store.get(task_id)
        if row is None:
            raise KeyError(task_id)
        return row

    # -- scheduler -------------------------------------------------------
    def _enqueue_locked(self, task_id: str, previous_state: str) -> None:
        """Move a paused/failed task back into the queue under a new generation."""
        row = self.store.get(task_id)
        generation = row["generation"] + 1
        if not self.store.cas_state(
            task_id,
            previous_state,
            "queued",
            expected_generation=row["generation"],
            generation=generation,
            error=None,
            attempts=row["attempts"] + 1,
            queued_reason=REASON_WAITING,
        ):
            current = self.store.get(task_id)
            raise RuntimeError(
                f"could not enqueue {task_id}; state now {current['state'] if current else 'gone'}"
            )
        if previous_state == "paused":
            self._resume_note.add(task_id)

    async def _preempt_locked(self, holder, urgent_task_id: str) -> str:
        """Pause a running normal task so an urgent task can take the slot.

        The preempted task keeps its part file and is returned to the queue with
        a reason; the scheduler restarts it automatically once the urgent task
        releases the slot.  Its generation is bumped, fencing the old worker so
        its late responses can neither rewrite fragments nor reclaim the slot.
        """
        task_id = holder["id"]
        moved = self.store.cas_state(
            task_id,
            "running",
            "queued",
            expected_generation=holder["generation"],
            generation=holder["generation"] + 1,
            queued_reason=REASON_PREEMPTED.format(urgent=urgent_task_id),
        )
        if not moved:
            return ""
        run = self._runs.get(task_id)
        if run:
            run.cancel.set()
        # The slot is vacated immediately; the urgent caller acquires it in the
        # following dispatch.  Wait for the old worker to drain so it cannot keep
        # writing to its part file after the urgent transfer has started.
        self._slot = None
        if run and run.task:
            await asyncio.wait({run.task}, timeout=5)
        row = self.store.get(task_id)
        if row and row["part_path"]:
            part = Path(row["part_path"])
            if part.exists():
                self.store.update(
                    task_id,
                    downloaded_bytes=min(part.stat().st_size, row["expected_size"] or 0),
                )
        self.store.event(
            task_id,
            "preempted",
            f"slot taken by urgent task {urgent_task_id}; fragments retained",
        )
        return task_id

    async def _dispatch_locked(self) -> None:
        """Give the single slot to the highest-priority queued task, if free."""
        # Forget an in-memory slot whose holder is no longer running (e.g. a
        # worker that exited without the normal release path).
        if self._slot is not None:
            holder = self.store.get(self._slot)
            if holder is None or holder["state"] != "running":
                self._slot = None
        if self._slot is not None:
            self._refresh_queue_reasons()
            return
        candidates = self.store.list_queued_ordered()
        if not candidates:
            return
        chosen = candidates[0]
        generation = chosen["generation"] + 1
        note = "resume" if chosen["id"] in self._resume_note else "started"
        dispatch_fields: dict[str, Any] = {"queued_reason": None}
        if (chosen["queued_reason"] or "").startswith("queued: refresh"):
            # The transfer for a refresh starts now: invalidate the previously
            # published verification so the old package can no longer be served
            # before the new revision has been verified.
            dispatch_fields.update(verified_size=None, verified_sha256=None)
        if not self.store.cas_state(
            chosen["id"],
            "queued",
            "running",
            expected_generation=chosen["generation"],
            generation=generation,
            **dispatch_fields,
        ):
            # Someone else changed the task concurrently; retry on the next call.
            return
        self._resume_note.discard(chosen["id"])
        self._slot = chosen["id"]
        self.store.event(
            chosen["id"],
            note,
            f"priority={chosen['priority']} attempt {chosen['attempts'] + 1}",
        )
        self._spawn(chosen["id"], generation)
        self._refresh_queue_reasons()

    def _refresh_queue_reasons(self) -> None:
        """Explain, for every waiting task, exactly why it is not running."""
        queued = self.store.list_queued_ordered()
        for row in queued:
            current = row["queued_reason"] or ""
            # Specific explanations (preemption, pending refresh) stay durable;
            # only generic waiting reasons are recomputed.
            if current.startswith(("queued: paused to make room", "queued: refresh")):
                continue
            if self._slot is None:
                reason = REASON_WAITING
            else:
                holder = self.store.get(self._slot)
                if holder is not None and holder["priority"] == "urgent":
                    reason = REASON_URGENT_AHEAD.format(
                        n=sum(1 for q in queued if q["priority"] == "urgent")
                    )
                else:
                    reason = REASON_WAITING
            if reason != row["queued_reason"]:
                self.store.set_queued_reason(row["id"], reason)

    def _spawn(self, task_id: str, generation: int) -> None:
        run = RunningTask(task_id=task_id, generation=generation)
        self._runs[task_id] = run
        task = asyncio.create_task(self._run(run), name=f"collect:{task_id}")
        run.task = task
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # -- worker ----------------------------------------------------------
    async def _run(self, run: RunningTask) -> None:
        try:
            await self._collect(run)
        except Interrupted:
            log.info("task %s interrupted; progress kept", run.task_id)
            if self._shutting_down:
                # Teardown must leave a resumable task, never a stale 'running'.
                self._mark_shutdown_paused(run)
        except FatalSourceError as exc:
            self._fail(run, str(exc), retryable=False)
        except asyncio.CancelledError:
            if self._shutting_down:
                self._mark_shutdown_paused(run)
            raise
        except TransferError as exc:
            # Timeout / network / 5xx: the part file is a valid prefix, so keep it
            # and report the failure reason.  A later resume continues with Range.
            self._fail(run, str(exc), retryable=True)
        except (httpx.HTTPError, OSError, ValueError) as exc:
            self._fail(run, f"{type(exc).__name__}: {exc}", retryable=False)
        except Exception as exc:  # noqa: BLE001 - the worker must never vanish
            log.exception("unexpected failure for %s", run.task_id)
            self._fail(run, f"unexpected error: {type(exc).__name__}: {exc}", retryable=False)
        finally:
            if self._runs.get(run.task_id) is run:
                self._runs.pop(run.task_id, None)
            await self._release_slot(run)

    def _mark_shutdown_paused(self, run: RunningTask) -> None:
        """Transition a still-running task to a resumable pause at shutdown."""
        row = self.store.get(run.task_id)
        if row is None or row["state"] != "running" or row["generation"] != run.generation:
            return
        part = Path(row["part_path"]) if row["part_path"] else None
        keep = part.stat().st_size if part and part.exists() else 0
        self.store.cas_state(
            run.task_id,
            "running",
            "paused",
            expected_generation=run.generation,
            generation=run.generation + 1,
            error="interrupted by process shutdown; resumable",
            downloaded_bytes=min(keep, row["expected_size"] or keep),
            queued_reason=None,
        )
        self.store.event(run.task_id, "interrupted", "resumable after restart")

    async def _release_slot(self, run: RunningTask) -> None:
        """Vacate the slot when its holder exits and hand it to the queue.

        Only the live generation's worker can release the slot.  A preempted or
        paused generation whose worker drains late no longer holds the slot
        (preemption/pause cleared it first), so a stale round can never reclaim
        the slot or starve the task that legitimately replaced it.
        """
        async with self._sched_lock:
            if self._slot != run.task_id:
                return
            row = self.store.get(run.task_id)
            if row is not None and row["state"] == "running" and row["generation"] == run.generation:
                # The worker died without moving the state: make it resumable.
                if self._shutting_down:
                    self._mark_shutdown_paused(run)
                else:
                    self.store.cas_state(
                        run.task_id,
                        "running",
                        "failed",
                        expected_generation=run.generation,
                        error="worker exited without completing",
                        queued_reason=None,
                    )
            self._slot = None
            if not self._shutting_down:
                await self._dispatch_locked()

    def _fail(self, run: RunningTask, message: str, retryable: bool) -> None:
        row = self.store.get(run.task_id)
        stale_package = Path(row["package_path"]) if row and row["package_path"] else None
        if retryable:
            part = Path(row["part_path"]) if row and row["part_path"] else None
            keep = part.stat().st_size if part and part.exists() else 0
            moved = self.store.cas_state(
                run.task_id,
                "running",
                "failed",
                expected_generation=run.generation,
                error=message,
                downloaded_bytes=keep,
                queued_reason=None,
            )
        else:
            moved = self.store.cas_state(
                run.task_id,
                "running",
                "failed",
                expected_generation=run.generation,
                error=message,
                part_path=None,
                package_path=None,
                downloaded_bytes=0,
                queued_reason=None,
            )
        if moved:
            if not retryable:
                # Terminal failure: no half package and nothing that could be
                # mistaken for resumable data.
                self._discard_parts(run.task_id)
                if stale_package:
                    stale_package.unlink(missing_ok=True)
            self.store.event(
                run.task_id,
                "failed",
                message if not retryable else f"{message} (progress retained, resumable)",
            )

    def _alive(self, run: RunningTask) -> bool:
        if run.cancel.is_set():
            return False
        row = self.store.get(run.task_id)
        return bool(
            row and row["state"] == "running" and row["generation"] == run.generation
        )

    def _update(self, run: RunningTask, **fields: Any) -> None:
        if not self.store.update_guarded(run.task_id, run.generation, **fields):
            raise Interrupted()

    def _note(self, run: RunningTask, event: str, detail: str | None = None) -> None:
        if self._alive(run):
            self.store.event(run.task_id, event, detail)

    async def _collect(self, run: RunningTask) -> None:
        limit = max(1, self.settings.max_source_resyncs)
        for _ in range(limit):
            try:
                await self._attempt(run)
                return
            except Resync as exc:
                if not self._alive(run):
                    raise Interrupted() from exc
                log.warning("source changed for %s: %s", run.task_id, exc)
                row = self.store.get(run.task_id)
                if not self.store.update_guarded(
                    run.task_id,
                    run.generation,
                    resyncs=(row["resyncs"] if row else 0) + 1,
                    error=None,
                ):
                    raise Interrupted() from exc
                self.store.event(run.task_id, "source_changed", str(exc))
        raise FatalSourceError(
            f"source kept changing or failed verification after {limit} re-reads"
        )

    # -- directory -------------------------------------------------------
    async def _read_directory(self, run: RunningTask, station: str, date: str) -> CatalogEntry:
        assert self._client is not None
        url = f"{self.settings.source_base_url}/directory/{station}/{date}"
        try:
            response = await self._client.get(url)
        except httpx.HTTPError as exc:
            raise TransferError(f"directory request failed: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 404:
            raise FatalSourceError("directory entry not found (404)")
        if response.status_code != 200:
            raise TransferError(f"directory returned HTTP {response.status_code}")
        try:
            payload = response.json()
            entries = payload["entries"]
            if not isinstance(entries, list) or not entries:
                raise ValueError("empty entries")
            raw = next(
                (e for e in entries if str(e.get("name", "")).endswith(".tar.gz")),
                entries[0],
            )
            entry = CatalogEntry(
                station=station,
                date=date,
                url=raw.get("url") or f"/packages/{station}/{date}.tar.gz",
                size=int(raw["size"]),
                sha256=raw["sha256"],
                etag=raw["etag"],
                revision=payload.get("revision") or raw.get("revision"),
                updated_at=payload.get("updated_at"),
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise FatalSourceError(f"malformed directory entry: {exc}") from exc
        if entry.size < 0:
            raise FatalSourceError("directory reports negative size")
        return entry

    def _absolute(self, url: str) -> str:
        if url.startswith(("http://", "https://")):
            return url
        return f"{self.settings.source_base_url}/{url.lstrip('/')}"

    def _identity_matches(self, row, entry: CatalogEntry) -> bool:
        return bool(
            row["expected_sha256"] == entry.sha256
            and row["expected_size"] == entry.size
            and row["source_etag"] == entry.etag
            and row["source_url"] == self._absolute(entry.url)
        )

    def _part_path(self, run: RunningTask, entry: CatalogEntry) -> Path:
        # The identity is embedded in the filename, so bytes written for one
        # version can never be mistaken for another version's partial data.
        tag = hashlib.sha256(
            f"{entry.etag}|{entry.size}|{entry.sha256}".encode()
        ).hexdigest()[:16]
        return self.settings.parts_dir / f"{run.task_id}-{tag}.part"

    # -- one attempt -----------------------------------------------------
    async def _attempt(self, run: RunningTask) -> None:
        row = self.store.get(run.task_id)
        if row is None or not self._alive(run):
            raise Interrupted()
        station, date = row["station"], row["date"]

        entry = await self._read_directory(run, station, date)
        if not self._alive(run):
            raise Interrupted()

        part = self._part_path(run, entry)
        same_identity = self._identity_matches(row, entry)
        if not same_identity:
            # Never stitch two revisions: drop partial data and stale metadata.
            self._discard_parts(run.task_id)
            self._update(
                run,
                source_url=self._absolute(entry.url),
                source_revision=entry.revision,
                source_etag=entry.etag,
                expected_size=entry.size,
                expected_sha256=entry.sha256,
                downloaded_bytes=0,
                part_path=str(part),
                verified_size=None,
                verified_sha256=None,
                error=None,
            )
            log.info("dropped stale partial for %s (revision %s)", run.task_id, entry.revision)
        else:
            self._update(
                run,
                source_revision=entry.revision,
                expected_size=entry.size,
                expected_sha256=entry.sha256,
                part_path=str(part),
            )
            self._discard_parts(run.task_id, keep=part)

        # Re-read the database for the authoritative published path.
        row = self.store.get(run.task_id)
        package = Path(row["package_path"]) if row["package_path"] else None
        if same_identity and package and package.exists():
            if self._verify(package, entry):
                # Refresh found the same revision already on disk: no transfer.
                self._publish_from_existing(run, entry, package)
                return

        offset = part.stat().st_size if part.exists() else 0
        if offset > entry.size:
            part.unlink(missing_ok=True)
            offset = 0
        if offset == entry.size and offset > 0:
            if self._verify(part, entry):
                self._publish(run, entry, part)
                return
            part.unlink(missing_ok=True)
            offset = 0

        self._update(run, downloaded_bytes=offset)
        if offset:
            self._note(run, "resume", f"from byte {offset} of {entry.size}")
        else:
            self._note(run, "download", f"0 of {entry.size}")

        await self._download(run, entry, part, offset)

    async def _download(self, run: RunningTask, entry: CatalogEntry, part: Path, offset: int) -> None:
        assert self._client is not None
        url = self._absolute(entry.url)
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        if offset:
            headers["If-Range"] = entry.etag

        try:
            async with self._client.stream("GET", url, headers=headers) as response:
                if response.status_code in (412, 416):
                    raise Resync(f"source precondition rejected (HTTP {response.status_code})")
                if response.status_code not in (200, 206):
                    raise TransferError(f"package returned HTTP {response.status_code}")

                response_etag = response.headers.get("etag")
                if not response_etag:
                    raise Resync("source omitted ETag on package response")
                if response_etag != entry.etag:
                    raise Resync(f"ETag changed during request ({response_etag} != {entry.etag})")
                if offset and response.status_code == 200:
                    # The server ignored Range but confirmed the same ETag.  It is
                    # safe (and simpler) to restart this version from byte zero.
                    log.info("source ignored Range for %s; restarting from 0", run.task_id)
                    offset = 0
                    self._update(run, downloaded_bytes=0)
                    self._note(run, "range_ignored", "restarted from byte 0")

                if response.status_code == 206:
                    content_range = response.headers.get("content-range", "")
                    if not content_range.startswith(f"bytes {offset}-"):
                        raise Resync(f"unexpected Content-Range: {content_range!r}")
                    total = content_range.rsplit("/", 1)[-1]
                    if total.isdigit() and int(total) != entry.size:
                        raise Resync(f"Content-Range total {total} != directory size {entry.size}")

                # A 200 for a ranged request means the server ignored Range; the
                # ETag already matched, so truncate and write this version anew.
                mode = "wb" if offset == 0 else "ab"
                written = offset
                with open(part, mode) as handle:
                    async for chunk in response.aiter_bytes(self.settings.chunk_size):
                        if not self._alive(run):
                            self._flush(handle)
                            # Record the durable byte count only for the live
                            # generation; a paused task's stored progress is never
                            # advanced by a response that arrived late.
                            self.store.update_guarded(
                                run.task_id, run.generation,
                                downloaded_bytes=part.stat().st_size,
                            )
                            raise Interrupted()
                        handle.write(chunk)
                        written += len(chunk)
                        if written > entry.size:
                            self._flush(handle)
                            raise Resync("downloaded more than directory size")
                        self._update(run, downloaded_bytes=written)
                    self._flush(handle)
        except httpx.HTTPError as exc:
            raise TransferError(f"package request failed: {type(exc).__name__}: {exc}") from exc

        if not self._alive(run):
            raise Interrupted()
        actual = part.stat().st_size
        if actual != entry.size:
            raise Resync(f"body size {actual} != directory size {entry.size}")

        # Confirm the directory still advertises the identity we just fetched,
        # so a revision swapped in mid-transfer is never published.
        if not self._alive(run):
            raise Interrupted()
        latest = await self._read_directory(run, entry.station, entry.date)
        if (latest.sha256, latest.size, latest.etag) != (entry.sha256, entry.size, entry.etag):
            raise Resync("directory changed while downloading")
        self._publish(run, entry, part)

    @staticmethod
    def _flush(handle) -> None:
        handle.flush()
        os.fsync(handle.fileno())

    def _verify(self, path: Path, entry: CatalogEntry) -> bool:
        try:
            if path.stat().st_size != entry.size:
                return False
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
            return digest.hexdigest() == entry.sha256
        except OSError:
            return False

    def _publish(self, run: RunningTask, entry: CatalogEntry, part: Path) -> None:
        if not self._verify(part, entry):
            part.unlink(missing_ok=True)
            raise Resync("size/SHA-256 verification failed against directory entry")
        if not self._alive(run):
            raise Interrupted()
        self._publish_from_existing(run, entry, part)

    def _publish_from_existing(self, run: RunningTask, entry: CatalogEntry, part: Path) -> None:
        row = self.store.get(run.task_id)
        station, date = row["station"], row["date"]
        final = self.settings.packages_dir / f"{station}-{date}-{entry.sha256[:12]}.tar.gz"
        fd, tmp_name = tempfile.mkstemp(prefix=".publish-", dir=self.settings.packages_dir)
        tmp = Path(tmp_name)
        try:
            with open(part, "rb") as src, os.fdopen(fd, "wb") as dst:
                while chunk := src.read(1024 * 1024):
                    dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(tmp, final)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

        moved = self.store.cas_state(
            run.task_id,
            "running",
            "completed",
            expected_generation=run.generation,
            part_path=None,
            package_path=str(final),
            downloaded_bytes=entry.size,
            verified_size=entry.size,
            verified_sha256=entry.sha256,
            error=None,
            queued_reason=None,
            completed_at=utcnow(),
        )
        if not moved:
            # A pause (or a newer generation) won the race: remove the artifact
            # and keep the part file so the task is never falsely completed and
            # never exposes an unverified/half package.
            final.unlink(missing_ok=True)
            raise Interrupted()

        if part != final:
            part.unlink(missing_ok=True)
        for old in self.settings.packages_dir.glob(f"{station}-{date}-*.tar.gz"):
            if old != final:
                old.unlink(missing_ok=True)
        self.store.event(run.task_id, "verified", f"sha256={entry.sha256} size={entry.size}")

    def _discard_parts(self, task_id: str, keep: Path | None = None) -> None:
        for stale in self.settings.parts_dir.glob(f"{task_id}-*.part"):
            if keep is None or stale != keep:
                stale.unlink(missing_ok=True)
