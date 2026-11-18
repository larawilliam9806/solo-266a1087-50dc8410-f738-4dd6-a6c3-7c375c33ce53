"""Two-level priority scheduling: single slot, preemption, consistent state.

Covers the rainstorm scenario: background (normal) re-collection occupies the
only transfer slot; an urgent QC request pauses it, keeps the partial fragments,
runs first, and the normal task resumes automatically afterwards.  Manually
paused tasks must never auto-resume, and interleaved start/pause/refresh and
restarts must keep queue and running states consistent.
"""

from __future__ import annotations

import asyncio
import hashlib
import time

import httpx
import pytest

from app.collector import Collector
from app.config import Settings
from app.main import create_app
from mock_source.app import app as source_app, state as source_state
from tests.conftest import free_port, run_server


class Stack:
    def __init__(self, source_url: str, api_url: str):
        self.source_url = source_url
        self.api_url = api_url


async def _wait(client: httpx.AsyncClient, api: str, task_id: str, states: set[str], timeout: float = 60):
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] in states:
            return view
        await asyncio.sleep(0.05)
    raise AssertionError(f"timeout waiting for {states}; last={view}")


async def _wait_until(
    client: httpx.AsyncClient, api: str, task_id: str, predicate, timeout: float = 15
) -> dict:
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if predicate(view):
            return view
        await asyncio.sleep(0.03)
    raise AssertionError(f"predicate not met; last={view}")


async def _settle(client: httpx.AsyncClient, api: str, seconds: float = 0.8) -> None:
    await asyncio.sleep(seconds)


@pytest.fixture
async def stack(tmp_path):
    source_state.revisions.clear()
    source_state.delay_seconds = 0.0
    source_state.chunk_delay_seconds = 0.0
    source_state.failing.clear()
    source_state.corrupting.clear()
    source_state.no_range.clear()
    source_state.requests.clear()

    source_port = free_port()
    api_port = free_port()
    settings = Settings(
        source_base_url=f"http://127.0.0.1:{source_port}",
        data_dir=tmp_path / "data",
        http_timeout=15,
        chunk_size=8 * 1024,
        max_source_resyncs=3,
        auto_resume=False,
    )
    collector = Collector(settings)
    api_app = create_app(settings, collector)
    async with run_server(source_app, source_port):
        async with run_server(api_app, api_port):
            async with httpx.AsyncClient(timeout=30) as client:
                yield Stack(f"http://127.0.0.1:{source_port}", f"http://127.0.0.1:{api_port}"), client, settings, collector


async def _create(client, api, station, date, priority=None):
    payload = {"station": station, "date": date}
    if priority is not None:
        payload["priority"] = priority
    resp = await client.post(f"{api}/tasks", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


async def test_default_priority_is_normal_and_queue_reason_shown(stack):
    st, client, _s, _c = stack
    api = st.api_url
    view = await _create(client, api, "P1", "2026-12-01")
    assert view["priority"] == "normal"
    assert view["state"] == "queued"
    assert view["queued_reason"] and "slot" in view["queued_reason"]

    urgent = await _create(client, api, "P2", "2026-12-01", "urgent")
    assert urgent["priority"] == "urgent"

    # Invalid priority is rejected at the boundary.
    bad = await client.post(
        f"{api}/tasks", json={"station": "P3", "date": "2026-12-01", "priority": "flash"}
    )
    assert bad.status_code == 422


async def test_only_one_task_runs_at_a_time_others_queue_with_reasons(stack):
    st, client, _s, _c = stack
    api = st.api_url
    await client.post(
        f"{st.source_url}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05}
    )
    first = await _create(client, api, "S1", "2026-12-02")
    second = await _create(client, api, "S2", "2026-12-02")
    third = await _create(client, api, "S3", "2026-12-02")
    await client.post(f"{api}/tasks/{first['id']}/start")
    await client.post(f"{api}/tasks/{second['id']}/start")
    await client.post(f"{api}/tasks/{third['id']}/start")
    await _settle(client, api)

    running = [t for t in (await client.get(f"{api}/tasks")).json() if t["state"] == "running"]
    queued = [t for t in (await client.get(f"{api}/tasks")).json() if t["state"] == "queued"]
    assert len(running) == 1
    assert running[0]["id"] == first["id"]
    assert {t["id"] for t in queued} == {second["id"], third["id"]}
    assert all(t["queued_reason"] for t in queued)

    overview = (await client.get(f"{api}/scheduler")).json()
    assert overview["max_concurrent_tasks"] == 1
    assert len(overview["running"]) == 1
    assert [t["id"] for t in overview["queue_position"]] == [second["id"], third["id"]]

    # FIFO: the second task runs as soon as the first finishes.
    done = await _wait(client, api, first["id"], {"completed", "failed"})
    assert done["state"] == "completed"
    await _wait_until(client, api, second["id"], lambda v: v["state"] == "running")
    await _wait(client, api, second["id"], {"completed", "failed"})
    await _wait(client, api, third["id"], {"completed", "failed"})
    final = (await client.get(f"{api}/tasks")).json()
    assert all(t["state"] == "completed" for t in final)
    assert all(t["queued_reason"] is None for t in final)


async def test_urgent_task_preempts_running_normal_and_resumes_it_after(stack):
    st, client, settings, _c = stack
    api = st.api_url
    await client.post(
        f"{st.source_url}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05}
    )
    normal = await _create(client, api, "N1", "2026-12-03", "normal")
    await client.post(f"{api}/tasks/{normal['id']}/start")
    running = await _wait_until(client, api, normal["id"], lambda v: v["state"] == "running")
    assert running["downloaded_bytes"] == 0
    progressed = await _wait_until(
        client, api, normal["id"], lambda v: v["downloaded_bytes"] > 100_000
    )
    bytes_before = progressed["downloaded_bytes"]

    urgent = await _create(client, api, "U1", "2026-12-03", "urgent")
    await client.post(f"{api}/tasks/{urgent['id']}/start")

    # The urgent task takes over; the normal task is back in the queue with a
    # preemption reason and its partial fragments retained.
    await _wait_until(client, api, urgent["id"], lambda v: v["state"] == "running")
    normal_view = await _wait_until(
        client, api, normal["id"], lambda v: v["state"] == "queued"
    )
    assert "urgent" in (normal_view["queued_reason"] or "")
    assert "resume" in (normal_view["queued_reason"] or "")
    assert normal_view["downloaded_bytes"] >= bytes_before
    parts = list(settings.parts_dir.glob(f"{normal['id']}-*.part"))
    assert parts and parts[0].stat().st_size > 0, "preempted fragments must be retained"

    overview = (await client.get(f"{api}/scheduler")).json()
    assert overview["running"][0]["id"] == urgent["id"]
    assert overview["queue_position"][0]["id"] == normal["id"]

    # Urgent completes first; the normal task auto-resumes without any API call.
    urgent_done = await _wait(client, api, urgent["id"], {"completed", "failed"})
    assert urgent_done["state"] == "completed", urgent_done
    resumed = await _wait_until(client, api, normal["id"], lambda v: v["state"] == "running")
    assert resumed["generation"] > normal_view["generation"]
    normal_done = await _wait(client, api, normal["id"], {"completed", "failed"}, timeout=90)
    assert normal_done["state"] == "completed", normal_done

    # Both packages are independently verifiable.
    for task in (urgent, normal):
        done = await client.get(f"{api}/tasks/{task['id']}")
        pkg = await client.get(f"{api}/tasks/{task['id']}/package")
        assert pkg.status_code == 200
        assert hashlib.sha256(pkg.content).hexdigest() == done.json()["verified_sha256"]

    events = [e["event"] for e in (await client.get(f"{api}/tasks/{normal['id']}/events")).json()]
    assert "preempted" in events
    assert "resume" in events


async def test_urgent_does_not_preempt_another_urgent_and_goes_first(stack):
    st, client, _s, _c = stack
    api = st.api_url
    await client.post(
        f"{st.source_url}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05}
    )
    urgent_a = await _create(client, api, "UA", "2026-12-04", "urgent")
    normal = await _create(client, api, "NB", "2026-12-04", "normal")
    urgent_b = await _create(client, api, "UC", "2026-12-04", "urgent")

    await client.post(f"{api}/tasks/{urgent_a['id']}/start")
    await client.post(f"{api}/tasks/{normal['id']}/start")
    await client.post(f"{api}/tasks/{urgent_b['id']}/start")
    await _settle(client, api)

    running = [t for t in (await client.get(f"{api}/tasks")).json() if t["state"] == "running"]
    assert len(running) == 1 and running[0]["id"] == urgent_a["id"]
    overview = (await client.get(f"{api}/scheduler")).json()
    # Urgent FIFO ahead of the normal task.
    assert [t["id"] for t in overview["queue_position"]] == [urgent_b["id"], normal["id"]]
    waiting_b = next(t for t in overview["queue_position"] if t["id"] == urgent_b["id"])
    assert "urgent" in (waiting_b["queued_reason"] or "")

    await _wait(client, api, urgent_a["id"], {"completed", "failed"})
    await _wait_until(client, api, urgent_b["id"], lambda v: v["state"] == "running")
    # Normal must not start while the second urgent is running.
    normal_view = await client.get(f"{api}/tasks/{normal['id']}")
    assert normal_view.json()["state"] == "queued"
    await _wait(client, api, urgent_b["id"], {"completed", "failed"})
    await _wait_until(client, api, normal["id"], lambda v: v["state"] == "running")
    await _wait(client, api, normal["id"], {"completed", "failed"})
    assert all(
        t["state"] == "completed" for t in (await client.get(f"{api}/tasks")).json()
    )


async def test_manually_paused_task_is_never_auto_resumed(stack):
    st, client, _s, _c = stack
    api = st.api_url
    await client.post(
        f"{st.source_url}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05}
    )
    blocker = await _create(client, api, "BLK", "2026-12-05", "normal")
    paused_task = await _create(client, api, "MAN", "2026-12-05", "normal")
    await client.post(f"{api}/tasks/{blocker['id']}/start")
    await client.post(f"{api}/tasks/{paused_task['id']}/start")
    await _settle(client, api)
    # It is waiting in the queue behind the blocker.
    view = (await client.post(f"{api}/tasks/{paused_task['id']}/pause")).json()
    assert view["state"] == "paused"
    assert view["queued_reason"] is None

    await _wait(client, api, blocker["id"], {"completed", "failed"})
    await _settle(client, api, 1.5)
    # The scheduler must not pick up a manual pause even though the slot is free.
    still = (await client.get(f"{api}/tasks/{paused_task['id']}")).json()
    assert still["state"] == "paused"

    # Explicit resume is required and then it runs.
    await client.post(f"{api}/tasks/{paused_task['id']}/resume")
    done = await _wait(client, api, paused_task["id"], {"completed", "failed"})
    assert done["state"] == "completed"


async def test_manual_pause_of_urgent_releases_slot_to_preempted_normal(stack):
    st, client, settings, _c = stack
    api = st.api_url
    await client.post(
        f"{st.source_url}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05}
    )
    normal = await _create(client, api, "N2", "2026-12-06", "normal")
    await client.post(f"{api}/tasks/{normal['id']}/start")
    await _wait_until(client, api, normal["id"], lambda v: v["downloaded_bytes"] > 50_000)

    urgent = await _create(client, api, "U2", "2026-12-06", "urgent")
    await client.post(f"{api}/tasks/{urgent['id']}/start")
    await _wait_until(client, api, urgent["id"], lambda v: v["state"] == "running")
    await _wait_until(client, api, normal["id"], lambda v: v["state"] == "queued")

    # Operator pauses the urgent job mid-flight: the preempted normal job must
    # take the slot back automatically.
    paused = (await client.post(f"{api}/tasks/{urgent['id']}/pause")).json()
    assert paused["state"] == "paused"
    await _wait_until(client, api, normal["id"], lambda v: v["state"] == "running")
    normal_done = await _wait(client, api, normal["id"], {"completed", "failed"}, timeout=90)
    assert normal_done["state"] == "completed"
    # The manually paused urgent task stays paused.
    urgent_view = (await client.get(f"{api}/tasks/{urgent['id']}")).json()
    assert urgent_view["state"] == "paused"


async def test_failed_urgent_task_still_releases_slot_to_normal(stack):
    st, client, _s, _c = stack
    source, api = st.source_url, st.api_url
    await client.post(
        f"{source}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.03}
    )
    normal = await _create(client, api, "N3", "2026-12-07", "normal")
    await client.post(f"{api}/tasks/{normal['id']}/start")
    await _wait_until(client, api, normal["id"], lambda v: v["downloaded_bytes"] > 50_000)

    urgent = await _create(client, api, "U3", "2026-12-07", "urgent")
    await client.post(f"{source}/admin/fault", json={"station": "U3", "date": "2026-12-07", "mode": "fail"})
    await client.post(f"{api}/tasks/{urgent['id']}/start")
    urgent_view = await _wait(client, api, urgent["id"], {"failed", "completed"})
    assert urgent_view["state"] == "failed"
    assert "503" in (urgent_view["error"] or "")

    # Slot released: normal auto-resumes and completes despite the urgent failure.
    await _wait_until(client, api, normal["id"], lambda v: v["state"] == "running")
    normal_done = await _wait(client, api, normal["id"], {"completed", "failed"}, timeout=90)
    assert normal_done["state"] == "completed"


async def test_preempted_task_resumes_with_range_and_verifies(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    await client.post(
        f"{source}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05}
    )
    normal = await _create(client, api, "N4", "2026-12-08", "normal")
    await client.post(f"{api}/tasks/{normal['id']}/start")
    await _wait_until(client, api, normal["id"], lambda v: v["downloaded_bytes"] > 150_000)

    urgent = await _create(client, api, "U4", "2026-12-08", "urgent")
    await client.post(f"{api}/tasks/{urgent['id']}/start")
    await _wait(client, api, urgent["id"], {"completed", "failed"})
    done = await _wait(client, api, normal["id"], {"completed", "failed"}, timeout=90)
    assert done["state"] == "completed", done

    state_info = (await client.get(f"{source}/admin/state")).json()
    ranged = [r for r in state_info["recent_requests"] if r["range"]]
    assert any(r["range"] and r["range"] != "bytes=0-" for r in ranged)
    assert all(r["if_range"] for r in ranged)


async def test_late_response_of_preempted_round_cannot_rewrite_new_run(stack):
    st, client, settings, _c = stack
    api = st.api_url
    await client.post(
        f"{st.source_url}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.06}
    )
    normal = await _create(client, api, "N5", "2026-12-09", "normal")
    await client.post(f"{api}/tasks/{normal['id']}/start")
    pre = await _wait_until(client, api, normal["id"], lambda v: v["downloaded_bytes"] > 100_000)
    gen_pre = pre["generation"]

    urgent = await _create(client, api, "U5", "2026-12-09", "urgent")
    await client.post(f"{api}/tasks/{urgent['id']}/start")
    queued = await _wait_until(client, api, normal["id"], lambda v: v["state"] == "queued")
    assert queued["generation"] == gen_pre + 1
    bytes_at_preempt = queued["downloaded_bytes"]

    # Let any late chunk of the preempted generation try to land.
    await _settle(client, api, 1.5)
    after = (await client.get(f"{api}/tasks/{normal['id']}")).json()
    assert after["state"] == "queued"
    assert after["generation"] == gen_pre + 1
    assert after["downloaded_bytes"] == bytes_at_preempt

    urgent_done = await _wait(client, api, urgent["id"], {"completed", "failed"})
    assert urgent_done["state"] == "completed"
    normal_done = await _wait(client, api, normal["id"], {"completed", "failed"}, timeout=90)
    assert normal_done["state"] == "completed"
    pkg = await client.get(f"{api}/tasks/{normal['id']}/package")
    assert hashlib.sha256(pkg.content).hexdigest() == normal_done["verified_sha256"]


async def test_refresh_waits_for_slot_and_recollects_under_scheduling(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    station, date = "REF", "2026-12-10"
    task = await _create(client, api, station, date)
    await client.post(f"{api}/tasks/{task['id']}/start")
    first = await _wait(client, api, task["id"], {"completed", "failed"})
    assert first["state"] == "completed"

    # Occupy the slot with another task, then ask for a refresh: it must queue.
    await client.post(f"{source}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05})
    blocker = await _create(client, api, "BLK2", date, "normal")
    await client.post(f"{api}/tasks/{blocker['id']}/start")
    await _wait_until(client, api, blocker["id"], lambda v: v["state"] == "running")

    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"}
    )
    refreshed = (await client.post(f"{api}/tasks/{task['id']}/refresh")).json()
    assert refreshed["state"] == "queued"
    assert "refresh" in (refreshed["queued_reason"] or "")
    # The completed package stays downloadable while the refresh is queued.
    during = await client.get(f"{api}/tasks/{task['id']}/package")
    assert during.status_code == 200

    await _wait(client, api, blocker["id"], {"completed", "failed"})
    second = await _wait(client, api, task["id"], {"completed", "failed"}, timeout=90)
    assert second["state"] == "completed"
    assert second["source_revision"] == "v2"
    assert second["verified_sha256"] != first["verified_sha256"]
    pkg = await client.get(f"{api}/tasks/{task['id']}/package")
    entry = (await client.get(f"{source}/directory/{station}/{date}")).json()["entries"][0]
    assert hashlib.sha256(pkg.content).hexdigest() == entry["sha256"]


async def test_interleaved_start_pause_refresh_keeps_single_slot_consistent(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    await client.post(
        f"{source}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.04}
    )
    # Four normal tasks + one urgent, hammered with interleaved lifecycle calls.
    tasks = [
        (await _create(client, api, f"IL{i}", "2026-12-13", "normal"))["id"] for i in range(4)
    ]
    urgent = (await _create(client, api, "ILU", "2026-12-13", "urgent"))["id"]

    async def hammer(tid: str):
        for _ in range(6):
            await client.post(f"{api}/tasks/{tid}/start")
            await asyncio.sleep(0.02)
            await client.post(f"{api}/tasks/{tid}/pause")
            await asyncio.sleep(0.02)
            await client.post(f"{api}/tasks/{tid}/resume")

    coros = [hammer(tid) for tid in tasks]
    coros.append(
        (lambda: client.post(f"{api}/tasks/{urgent}/start"))()  # noqa: E731
    )
    await asyncio.gather(*coros)

    # At every poll there is at most one running task, and queue reasons exist.
    for _ in range(40):
        rows = (await client.get(f"{api}/tasks")).json()
        assert len([t for t in rows if t["state"] == "running"]) <= 1
        for t in rows:
            if t["state"] == "queued":
                assert t["queued_reason"]
        if all(t["state"] in {"completed", "paused", "failed"} for t in rows) and not any(
            t["state"] == "running" for t in rows
        ):
            break
        await asyncio.sleep(0.1)

    # Explicitly resume anything left non-terminal and let the queue drain.
    for tid in tasks + [urgent]:
        await client.post(f"{api}/tasks/{tid}/start")
    for tid in tasks + [urgent]:
        done = await _wait(client, api, tid, {"completed", "failed"}, timeout=120)
        assert done["state"] == "completed", done

    overview = (await client.get(f"{api}/scheduler")).json()
    assert overview["running"] == [] and overview["queue_position"] == []
    for tid in tasks + [urgent]:
        pkg = await client.get(f"{api}/tasks/{tid}/package")
        assert pkg.status_code == 200


async def test_restart_keeps_queue_and_slot_consistent(stack):
    st, client, settings, collector = stack
    api = st.api_url
    await client.post(
        f"{st.source_url}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.05}
    )
    running_task = await _create(client, api, "R1", "2026-12-11", "normal")
    queued_task = await _create(client, api, "Q1", "2026-12-11", "normal")
    manual = await _create(client, api, "M1", "2026-12-11", "normal")
    await client.post(f"{api}/tasks/{running_task['id']}/start")
    await client.post(f"{api}/tasks/{queued_task['id']}/start")
    await _wait_until(client, api, running_task["id"], lambda v: v["downloaded_bytes"] > 50_000)
    await client.post(f"{api}/tasks/{manual['id']}/pause")

    # Restart the collector process against the same database (auto_resume off).
    await collector.shutdown()
    fresh = Collector(settings)
    await fresh.startup()
    try:
        states = {row["id"]: row for row in fresh.store.list()}
        assert states[running_task["id"]]["state"] == "paused"
        assert states[queued_task["id"]]["state"] == "queued"
        assert states[manual["id"]]["state"] == "paused"
        # Nothing is running in memory or in the database.
        assert fresh._slot is None
        assert not fresh.store.list("running")

        # Explicit starts after restart still honor the single slot.
        await fresh.start(queued_task["id"])
        await fresh.start(running_task["id"])
        await asyncio.sleep(0.5)
        running = fresh.store.list("running")
        assert len(running) == 1
        assert running[0]["id"] == queued_task["id"]
        done = await _wait(client, api, queued_task["id"], {"completed", "failed"})
        assert done["state"] == "completed"
        done2 = await _wait(client, api, running_task["id"], {"completed", "failed"}, timeout=90)
        assert done2["state"] == "completed"
        assert fresh.store.get(manual["id"])["state"] == "paused"
    finally:
        await fresh.shutdown()


async def test_restart_with_auto_resume_replays_queue_but_not_manual_pause(tmp_path):
    # A second in-process stack with AUTO_RESUME enabled, driven directly.
    from tests.conftest import free_port as _free
    from tests.conftest import run_server as _run

    source_state.revisions.clear()
    source_state.failing.clear()
    source_state.corrupting.clear()
    source_state.delay_seconds = 0.0
    source_state.chunk_delay_seconds = 0.02

    source_port = _free()
    settings = Settings(
        source_base_url=f"http://127.0.0.1:{source_port}",
        data_dir=tmp_path / "data-ar",
        http_timeout=15,
        chunk_size=8 * 1024,
        auto_resume=True,
    )
    collector = Collector(settings)
    app = create_app(settings, collector)
    async with _run(source_app, source_port):
        async with _run(app, _free()) as cm:
            api_port = cm.port
            async with httpx.AsyncClient(timeout=30) as client:
                api = f"http://127.0.0.1:{api_port}"
                t1 = await _create(client, api, "AR1", "2026-12-12")
                t2 = await _create(client, api, "AR2", "2026-12-12")
                t3 = await _create(client, api, "AR3", "2026-12-12")
                await client.post(f"{api}/tasks/{t1['id']}/start")
                await client.post(f"{api}/tasks/{t2['id']}/start")
                await _wait_until(client, api, t1["id"], lambda v: v["downloaded_bytes"] > 30_000)
                await client.post(f"{api}/tasks/{t3['id']}/pause")  # manual pause

                await collector.shutdown()
                fresh = Collector(settings)
                await fresh.startup()
                try:
                    # Interrupted + queued work replays; the manual pause stays.
                    await asyncio.sleep(0.4)
                    assert fresh.store.get(t3["id"])["state"] == "paused"
                    running = fresh.store.list("running")
                    assert len(running) == 1  # still the single-slot guarantee
                    ids = {t1["id"], t2["id"]}
                    # The two replayable tasks run one at a time and both finish.
                    first_done = await _wait(
                        client, api, running[0]["id"], {"completed", "failed"}, timeout=90
                    )
                    assert first_done["state"] == "completed"
                    remaining = (ids - {running[0]["id"]}).pop()
                    second_done = await _wait(client, api, remaining, {"completed", "failed"}, timeout=90)
                    assert second_done["state"] == "completed"
                    # The manual pause was never auto-resumed.
                    assert fresh.store.get(t3["id"])["state"] == "paused"
                    overview_running = [
                        t for t in (await client.get(f"{api}/tasks")).json() if t["state"] == "running"
                    ]
                    assert not overview_running
                finally:
                    await fresh.shutdown()
