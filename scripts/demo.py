#!/usr/bin/env python3
"""End-to-end demo against the Compose stack (or any base URLs).

Usage:
    python scripts/demo.py [--source http://localhost:9000] [--api http://localhost:8000]

The demo starts a background (normal) collection, shows an urgent QC task
preempting it with partial fragments retained, watches the normal task resume
automatically, then exercises pause/resume, a revision switch with refresh, and
verifies the downloadable package's size and SHA-256.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time

import httpx


def wait(client: httpx.Client, api: str, task_id: str, states: set[str], timeout: float = 90) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        view = client.get(f"{api}/tasks/{task_id}").json()
        if view["state"] in states:
            return view
        time.sleep(0.2)
    raise SystemExit(f"timeout waiting for {states}; last={view}")


def wait_until(client: httpx.Client, api: str, task_id: str, predicate, timeout: float = 30) -> dict:
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = client.get(f"{api}/tasks/{task_id}").json()
        if predicate(view):
            return view
        time.sleep(0.1)
    raise SystemExit(f"predicate not met; last={view}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="http://localhost:9000")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--station", default="54511")
    parser.add_argument("--date", default="2026-11-17")
    args = parser.parse_args()

    source, api = args.source.rstrip("/"), args.api.rstrip("/")
    with httpx.Client(timeout=30) as client:
        print("source:", client.get(f"{source}/healthz").json())
        print("api:   ", client.get(f"{api}/healthz").json())
        client.post(f"{source}/admin/reset")
        client.post(f"{source}/admin/delay", json={"seconds": 0.3, "chunk_seconds": 0.01})

        # 1) Background, normal-priority re-collection occupies the single slot.
        normal = client.post(
            f"{api}/tasks",
            json={"station": args.station, "date": args.date, "priority": "normal"},
        ).json()
        normal_id = normal["id"]
        print("normal task:", normal_id, normal["priority"], normal["state"])
        client.post(f"{api}/tasks/{normal_id}/start")
        wait_until(client, api, normal_id, lambda v: v["downloaded_bytes"] > 0)
        view = client.get(f"{api}/tasks/{normal_id}").json()
        print("normal downloading:", view["state"], view["downloaded_bytes"], "bytes")

        # 2) An urgent QC request arrives and preempts the normal transfer.
        urgent = client.post(
            f"{api}/tasks",
            json={"station": "54512", "date": args.date, "priority": "urgent"},
        ).json()
        urgent_id = urgent["id"]
        client.post(f"{api}/tasks/{urgent_id}/start")
        wait_until(client, api, urgent_id, lambda v: v["state"] == "running")
        preempted = wait_until(client, api, normal_id, lambda v: v["state"] == "queued")
        print("urgent task running:", urgent_id)
        print("normal preempted:", preempted["queued_reason"])
        print("scheduler:", client.get(f"{api}/scheduler").json()["queue_position"])

        urgent_done = wait(client, api, urgent_id, {"completed", "failed"})
        assert urgent_done["state"] == "completed", urgent_done
        print("urgent finished first:", urgent_done["verified_sha256"][:12])

        # 3) The normal task resumes automatically from its retained fragments.
        resumed = wait_until(client, api, normal_id, lambda v: v["state"] == "running")
        print("normal auto-resumed, generation now:", resumed["generation"])
        done = wait(client, api, normal_id, {"completed", "failed"})
        assert done["state"] == "completed", done
        package = client.get(f"{api}/tasks/{normal_id}/package")
        package.raise_for_status()
        digest = hashlib.sha256(package.content).hexdigest()
        assert digest == done["verified_sha256"], (digest, done["verified_sha256"])
        print(f"normal package verified: {len(package.content)} bytes")

        # 4) Manual pause is respected (never auto-resumed) until an explicit resume.
        client.post(f"{source}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.02})
        manual = client.post(
            f"{api}/tasks", json={"station": "54513", "date": args.date}
        ).json()
        client.post(f"{api}/tasks/{manual['id']}/start")
        wait_until(client, api, manual["id"], lambda v: v["downloaded_bytes"] > 0)
        paused = client.post(f"{api}/tasks/{manual['id']}/pause").json()
        print("manual pause:", paused["state"], "bytes:", paused["downloaded_bytes"])
        time.sleep(0.5)
        after = client.get(f"{api}/tasks/{manual['id']}").json()
        assert after["state"] == "paused", after
        client.post(f"{api}/tasks/{manual['id']}/resume")
        manual_done = wait(client, api, manual["id"], {"completed", "failed"})
        assert manual_done["state"] == "completed"

        # 5) Switch the source revision and refresh: old package is replaced.
        client.post(f"{source}/admin/delay", json={"seconds": 0.0, "chunk_seconds": 0.0})
        client.post(
            f"{source}/admin/revision",
            json={"station": args.station, "date": args.date, "revision": "v2"},
        )
        client.post(f"{api}/tasks/{normal_id}/refresh")
        done2 = wait(client, api, normal_id, {"completed", "failed"}, timeout=90)
        assert done2["state"] == "completed", done2
        assert done2["source_revision"] == "v2", done2
        assert done2["verified_sha256"] != done["verified_sha256"]
        package2 = client.get(f"{api}/tasks/{normal_id}/package")
        package2.raise_for_status()
        assert hashlib.sha256(package2.content).hexdigest() == done2["verified_sha256"]
        print("revision switch verified:", done2["source_revision"], done2["verified_sha256"][:12])
        print("events:", [e["event"] for e in client.get(f"{api}/tasks/{normal_id}/events").json()])
    return 0


if __name__ == "__main__":
    sys.exit(main())
