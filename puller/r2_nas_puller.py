#!/usr/bin/env python3
"""image2 R2 -> NAS puller.

Runs ON the NAS (e.g. a PVE box). Cloudflare Workers/R2 live in the cloud and cannot
reach into your home LAN, so the NAS PULLS: it lists images the worker has staged in R2,
downloads each, writes it durably to the NAS, then ACKs — and only on ack does the worker
delete the R2 copy. delete-after-ack: an image leaves R2 only once it is durably on the NAS,
so a failed/partial pull never loses data (it stays staged for the next pass).

Integrity: the download is accepted only if its byte length equals the catalog's e['size']
AND it is a PNG; the file is fsync'd (file + parent dir) BEFORE /ack so a power loss right
after ack cannot leave a torn file while R2 is already pruned. Idempotent: a re-run
re-downloads + overwrites the same path and re-acks; harmless.

env:
  IMG2_ENDPOINT   worker base URL, e.g. https://img2-counter.<acct>.workers.dev   (required)
  IMG2_TOKEN      the worker AUTH_TOKEN (Bearer)                                   (required)
  IMG2_DEST       NAS base dir to archive into, e.g. /mnt/pool/image2  (default /mnt/image2)
  IMG2_ONCE       if set (any value), run ONE drain pass and exit (for cron); else loop
  IMG2_INTERVAL   loop interval seconds when not IMG2_ONCE (default 300)

cron (every 5 min):
  */5 * * * *  IMG2_ENDPOINT=... IMG2_TOKEN=... IMG2_DEST=/mnt/pool/image2 IMG2_ONCE=1 /usr/bin/python3 /opt/r2_nas_puller.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

EP = os.environ.get("IMG2_ENDPOINT", "").rstrip("/")
TOK = os.environ.get("IMG2_TOKEN", "")
DEST = Path(os.environ.get("IMG2_DEST", "/mnt/image2")).resolve()
HDR = {"Authorization": f"Bearer {TOK}", "User-Agent": "img2-nas-puller/1.1"}

if not EP or not TOK:
    sys.exit("set IMG2_ENDPOINT and IMG2_TOKEN")


def call(method, path, body=None, raw=False, timeout=20, tries=3):
    """HTTP call with bounded retry/backoff for transient errors. Permanent HTTP errors
    (4xx) raise immediately so the caller can decide (e.g. 404/409 = skip, don't loop)."""
    last = None
    for attempt in range(1, tries + 1):
        req = urllib.request.Request(
            EP + path, method=method,
            data=json.dumps(body).encode() if body else None,
            headers={**HDR, **({"Content-Type": "application/json"} if body else {})},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read() if raw else json.loads(r.read())
        except urllib.error.HTTPError as ex:
            if 400 <= ex.code < 500:
                raise  # permanent — let caller handle (404 missing, 409 mismatch)
            last = ex
        except (urllib.error.URLError, TimeoutError, OSError) as ex:
            last = ex
        time.sleep(min(2 ** attempt, 10))  # transient backoff
    raise last


def _fsync_path(p: Path):
    fd = os.open(str(p), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def pull_one(e):
    r2key, catKey = e["r2key"], e["catKey"]
    expected = e.get("size")
    # confine strictly under DEST: server already sanitizes segments, but defend in depth.
    rel = Path(r2key)
    if rel.is_absolute() or ".." in rel.parts or not str(rel).startswith("img/") or "\x00" in r2key or "\n" in r2key:
        print(f"  ! refusing suspicious key {r2key!r}")
        return False
    dest = (DEST / rel).resolve()
    if not str(dest).startswith(str(DEST) + os.sep):  # symlink / traversal escape guard
        print(f"  ! resolved path escapes DEST: {dest}")
        return False

    try:
        data = call("GET", "/img?key=" + urllib.parse.quote(r2key, safe=""), raw=True, timeout=120)
    except urllib.error.HTTPError as ex:
        # 404 = object already pruned/archived (e.g. an /ack that crashed after the status flip);
        # nothing to fetch. Leave the row; if it's genuinely wedged, it's visible in /audit.
        print(f"  ! /img {ex.code} for {r2key} — skipping")
        return False

    if len(data) < 100 or data[:4] != b"\x89PNG":
        print(f"  ! not a valid PNG ({len(data)}B) for {r2key}, leaving staged")
        return False
    if expected is not None and len(data) != expected:  # truncated/short download -> do NOT ack
        print(f"  ! size mismatch {len(data)}!={expected} for {r2key}, leaving staged for retry")
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())          # durable bytes before we publish
        if tmp.stat().st_size != len(data):
            print(f"  ! tmp size wrong for {dest}, aborting")
            return False
        os.replace(tmp, dest)             # atomic publish
        _fsync_path(dest.parent)          # durable rename before we let the worker prune R2
    except OSError as ex:
        print(f"  ! write failed for {dest}: {ex}")
        return False
    finally:
        if tmp.exists():
            try: tmp.unlink()
            except OSError: pass

    r = call("POST", "/ack", {"catKey": catKey, "r2key": r2key, "nas_path": str(dest)}, timeout=20)
    if not (isinstance(r, dict) and r.get("ok")):
        print(f"  ! /ack not ok for {r2key}: {r} — image is on NAS but R2 not pruned (retried next pass)")
        return False
    print(f"  ✓ {r2key} -> {dest} ({len(data)} B) acked+pruned")
    return True


def one_pass():
    """Drain: keep pulling until /pending returns nothing new (handles >limit backlogs)."""
    total = 0
    while True:
        res = call("GET", "/pending?limit=500", timeout=20)
        pend = res.get("pending", [])
        if not pend:
            break
        done = 0
        for e in pend:
            try:
                if pull_one(e):
                    done += 1
            except Exception as ex:  # noqa: BLE001 — one bad item must not kill the pass
                print(f"  ! error on {e.get('r2key')}: {ex}")
        total += done
        if done == 0:  # no progress (all left staged) -> stop to avoid a hot spin
            break
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] archived {total} this pass")
    return total


def main():
    DEST.mkdir(parents=True, exist_ok=True)
    if os.environ.get("IMG2_ONCE"):
        one_pass()
        return
    interval = int(os.environ.get("IMG2_INTERVAL", "300"))
    print(f"puller loop: every {interval}s, DEST={DEST}")
    while True:
        try:
            one_pass()
        except Exception as ex:  # noqa: BLE001
            print(f"pass error: {ex}")
        time.sleep(interval)


if __name__ == "__main__":
    main()
