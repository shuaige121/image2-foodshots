#!/usr/bin/env python3
"""
image2 — parallel gpt-image-2 generation via Codex's built-in `image_gen` tool,
with usage-aware auto concurrency.

Why this design:
  * gpt-image-2 is only reachable through Codex's built-in `image_gen` tool,
    which runs on the Codex *subscription* auth (plan: ChatGPT Pro here). It does
    NOT need OPENAI_API_KEY (unset on this machine; the CLI fallback
    `~/.codex/skills/.system/imagegen/scripts/image_gen.py` would fail).
  * `codex exec --json` drives the model headlessly so it calls `image_gen`.
  * The `--json` stream does NOT report the saved path; instead each run's
    `thread.started.thread_id` == the output dir name under
    `$CODEX_HOME/generated_images/<thread_id>/`. We locate the PNG by thread_id
    and never trust the model's self-reported path (it sometimes hallucinates).
  * Parallelism is clean because each concurrent `codex exec` gets its own
    thread_id -> its own output dir -> zero collision.

Quota / usage:
  * ChatGPT Pro = effectively unlimited image creation (subject to anti-abuse
    guardrails). There is NO published "images/day" number.
  * The real, locally-readable budget is the Codex subscription's rolling
    rate-limit windows. Every session rollout carries a `token_count.rate_limits`
    snapshot from the server: `primary` (5h window) + `secondary` (7d window),
    each with `used_percent` and `resets_at`, plus `rate_limit_reached_type`.
  * `--usage` prints the latest snapshot. Auto concurrency reads it to decide
    serial vs parallel and to back off when the window is nearly spent.

Reference images (img2img):
  * `--ref FILE` (repeatable) or per-job JSONL field `ref` attaches local image
    file(s) to the codex prompt via `codex exec -i`. The built-in image_gen then
    uses them as style/composition references or edit targets (`--ref-mode`).
  * gpt-image-2 always uses high input fidelity for image inputs.
  * GOTCHA: `-i` is variadic in clap and swallows a following positional prompt,
    so the prompt is ALWAYS passed via stdin ("-"). Never leave stdin open when
    invoking codex exec yourself — it blocks forever on "Reading additional
    input from stdin..." (we pass input= so the pipe closes).

Cross-device counter:
  * Optional config at $CODEX_HOME/image2-counter.json (NOT git-synced):
    {"endpoint": "https://img2-counter.<sub>.workers.dev", "token": "...",
     "device": "mac"}. Each run reports this device's ABSOLUTE count for the
    local date (idempotent — no increment races); --usage shows the
    all-devices totals. Failures are silently ignored.

Usage:
  image2.py "a red apple on white" "a blue mug, studio shot"     # auto concurrency
  image2.py -p "a cat" -p "a dog" --n 2 --size square --quality high
  image2.py "same plating, new dish: ..." --ref /path/competitor.jpg
  image2.py --jobs jobs.jsonl --out-dir ./out --concurrency 4
  image2.py --usage                                              # usage + image counters
"""
import argparse
import concurrent.futures as cf
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

CODEX_HOME = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
GEN_DIR = CODEX_HOME / "generated_images"
SESS_DIR = CODEX_HOME / "sessions"
# Cross-device shared counter (Cloudflare Worker + KV). Config lives OUTSIDE
# synced folders: {"endpoint": "https://img2-counter.<sub>.workers.dev",
#                  "token": "...", "device": "mac"}
COUNTER_CFG = Path(os.environ.get("IMAGE2_COUNTER_CONFIG", str(CODEX_HOME / "image2-counter.json")))

# Auto-concurrency thresholds on the primary (5h) window used_percent.
SOFT_LIMIT = 70.0   # above this: cap parallelism to 2
HARD_LIMIT = 90.0   # above this (or throttled): go serial


def log(msg: str) -> None:
    try:
        print(msg, file=sys.stderr, flush=True)
    except UnicodeEncodeError:  # Windows cp936 console can't print ▶✔✘
        print(msg.encode(sys.stderr.encoding or "ascii", "replace").decode(
            sys.stderr.encoding or "ascii"), file=sys.stderr, flush=True)


def slugify(text: str, fallback: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip().lower()).strip("-")[:40].strip("-")
    return s or fallback


# ---------------------------------------------------------------- usage / rate limits

def _read_rate_limits_from_file(path: Path) -> dict | None:
    """Return the LAST token_count.rate_limits dict in a rollout file, or None."""
    found = None
    try:
        for line in path.read_text(errors="ignore").splitlines():
            if "rate_limits" not in line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            rl = obj.get("payload", {}).get("rate_limits") or obj.get("rate_limits")
            if rl:
                found = rl
    except OSError:
        return None
    return found


def rate_limits_for_thread(thread_id: str | None) -> dict | None:
    if not thread_id:
        return None
    hits = glob.glob(str(SESS_DIR / "**" / f"rollout-*{thread_id}*.jsonl"), recursive=True)
    for p in hits:
        rl = _read_rate_limits_from_file(Path(p))
        if rl:
            return rl
    return None


def latest_rate_limits() -> dict | None:
    """Most recent rate-limit snapshot across all recent rollouts."""
    files = sorted(SESS_DIR.glob("**/rollout-*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files[:12]:
        rl = _read_rate_limits_from_file(p)
        if rl:
            return rl
    return None


def _fmt_resets(resets_at) -> str:
    """'14:30 today, in 1h05m' — absolute local time + relative."""
    if not resets_at:
        return "?"
    try:
        ts = int(resets_at)
        secs = ts - int(time.time())
    except (TypeError, ValueError):
        return "?"
    when = time.strftime("%H:%M", time.localtime(ts))
    day = time.strftime("%m-%d", time.localtime(ts))
    today = time.strftime("%m-%d", time.localtime())
    abs_part = when if day == today else f"{day} {when}"
    if secs <= 0:
        return f"{abs_part} (now)"
    h, m = secs // 3600, (secs % 3600) // 60
    rel = f"{h}h{m:02d}m" if h else f"{m}m"
    return f"{abs_part} (in {rel})"


def count_images_since(since_epoch: float) -> int:
    """Count generated PNGs under generated_images/ newer than since_epoch.

    Counts every image produced through Codex image_gen on this machine
    (this script AND interactive Codex sessions), since both land there.
    """
    n = 0
    try:
        for d in GEN_DIR.iterdir():
            if not d.is_dir() or d.stat().st_mtime < since_epoch:
                continue
            for p in d.glob("*.png"):
                if p.stat().st_mtime >= since_epoch and p.stat().st_size > 1024:
                    n += 1
    except OSError:
        pass
    return n


def image_counters(rl: dict | None) -> str:
    """today / current-5h-window / 7d image counts + rough remaining estimate."""
    now = time.time()
    lt = time.localtime(now)
    midnight = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1))
    today = count_images_since(midnight)
    week = count_images_since(now - 7 * 86400)
    line = f"images: today {today} | last 7d {week}"
    p = (rl or {}).get("primary") or {}
    used, resets = p.get("used_percent"), p.get("resets_at")
    if used and resets:
        try:
            win_start = int(resets) - 5 * 3600
            in_win = count_images_since(win_start)
            if in_win > 0 and used > 0:
                # crude: assumes the window's used% is mostly image calls
                remaining = int(in_win * (100 - used) / used)
                line += f" | this 5h window {in_win} (≈{used/in_win:.1f}%/img → ~{remaining} more before reset)"
            else:
                line += f" | this 5h window {in_win}"
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return line


def _counter_cfg() -> dict | None:
    try:
        cfg = json.loads(COUNTER_CFG.read_text())
        return cfg if cfg.get("endpoint") and cfg.get("token") else None
    except (OSError, json.JSONDecodeError):
        return None


def _counter_call(method: str, path: str, body: dict | None = None) -> dict | None:
    """Best-effort call to the shared-counter Worker; None on any failure."""
    cfg = _counter_cfg()
    if not cfg:
        return None
    req = urllib.request.Request(
        cfg["endpoint"] + path, method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {cfg['token']}", "Content-Type": "application/json",
                 "User-Agent": "image2-skill/1.0"},  # CF blocks default Python-urllib UA with 403
    )
    try:
        with urllib.request.urlopen(req, timeout=6) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def counter_report() -> None:
    """Report THIS device's absolute count for today (idempotent, race-free)."""
    cfg = _counter_cfg()
    if not cfg:
        return
    lt = time.localtime()
    midnight = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1))
    _counter_call("POST", "/report", {
        "device": cfg.get("device", "unknown"),
        "date": time.strftime("%Y-%m-%d", lt),
        "count": count_images_since(midnight),
    })


def counter_summary() -> str | None:
    day = _counter_call("GET", f"/day?date={time.strftime('%Y-%m-%d')}")
    if not day:
        return None
    parts = ", ".join(f"{d} {n}" for d, n in sorted(day.get("devices", {}).items()))
    line = f"all devices today: {day.get('total', '?')}" + (f" ({parts})" if parts else "")
    week = _counter_call("GET", "/week")
    if week:
        line += f" | 7d all devices: {week.get('total', '?')}"
    return line


def cooldown_check() -> dict | None:
    """Read-only shared cooldown gate (GET /check); None if no counter configured."""
    return _counter_call("GET", "/check")


def cooldown_event(n: int = 1) -> None:
    """Record n generation events into the shared cooldown window (best-effort)."""
    dev = (_counter_cfg() or {}).get("device", "unknown")
    for _ in range(max(1, n)):
        _counter_call("POST", "/gen", {"device": dev})


def cooldown_line() -> str | None:
    st = cooldown_check()
    if not st:
        return None
    if st.get("in_cooldown"):
        mins = (st.get("cooldown_remaining_sec", 0) + 59) // 60
        return (f"⏳ SHARED COOLDOWN: {st.get('recent_in_window')} gens in last "
                f"{st.get('window_min')}min — wait ~{mins}min (until {st.get('cooldown_until_iso', '?')})")
    return f"cooldown gate: clear ({st.get('recent_in_window')}/{st.get('threshold')} in last {st.get('window_min')}min)"


import base64
import threading
import atexit

# ---------- generation lease (cross-device mutex) + R2 image staging ----------
_LEASE = {"id": None, "owned": False, "stop": None}


def lease_acquire(task: str, ttl: int = 900, wait_max: int = 1200) -> None:
    """Claim the single cross-device generation lease BEFORE a batch, so two devices never
    race the rotating OAuth refresh token. If another device holds it, wait (up to wait_max).
    If a parent (paced_run) already holds this device's lease, inherit it WITHOUT owning, so
    we never release someone else's in-flight lease. Best-effort: no counter -> no-op."""
    cfg = _counter_cfg()
    if not cfg:
        return
    held = os.environ.get("IMAGE2_LEASE_HELD")  # parent passed its lease -> don't re-claim/release
    if held:
        _LEASE["id"], _LEASE["owned"] = held, False
        return
    dev = cfg.get("device", "unknown")
    waited = 0
    while True:
        r = _counter_call("POST", "/claim", {"device": dev, "task": task[:120], "ttl": ttl})
        if r is None:
            return  # coordinator unreachable -> proceed uncoordinated (don't block real work)
        if r.get("granted"):
            _LEASE["id"] = r.get("lease_id")
            _LEASE["owned"] = not r.get("inherited")  # only release a lease we actually created
            if _LEASE["owned"]:
                _lease_start_heartbeat(ttl)
            return
        wait = min(20, max(5, int(r.get("wait_sec", 15))))
        if waited >= wait_max:
            log(f"  ⚠ waited {waited}s for the generation lease (held by {r.get('holder')}); proceeding")
            return
        log(f"  ⏳ generation lease held by {r.get('holder')} ({r.get('task', '')}) — waiting {wait}s")
        time.sleep(wait)
        waited += wait


def _lease_start_heartbeat(ttl: int) -> None:
    stop = threading.Event()
    _LEASE["stop"] = stop

    def beat():
        while not stop.wait(max(30, ttl // 3)):
            if not _LEASE["id"]:
                return
            _counter_call("POST", "/heartbeat", {"lease_id": _LEASE["id"], "ttl": ttl})

    threading.Thread(target=beat, daemon=True).start()


def lease_release() -> None:
    if _LEASE.get("stop"):
        _LEASE["stop"].set()
    if _LEASE.get("owned") and _LEASE.get("id"):
        _counter_call("POST", "/release", {"lease_id": _LEASE["id"]})
    _LEASE["id"], _LEASE["owned"], _LEASE["stop"] = None, False, None


atexit.register(lease_release)  # release on crash / SIGINT too (best-effort)


def upload_image(path: str, name: str, thread_id: str | None, prompt: str) -> None:
    """Stage one PNG into R2 via the worker (best-effort). The NAS puller archives it to the
    NAS and acks, after which the worker prunes the R2 copy (delete-after-ack)."""
    cfg = _counter_cfg()
    if not cfg:
        return
    try:
        data = Path(path).read_bytes()
    except OSError:
        return
    _counter_call("POST", "/upload", {
        "device": cfg.get("device", "unknown"),
        "thread_id": thread_id or "x",
        "name": name,
        "prompt": prompt[:2000],
        "date": time.strftime("%Y-%m-%d"),
        "png_b64": base64.b64encode(data).decode(),
    })


def format_usage(rl: dict | None) -> str:
    if not rl:
        return "usage: (no rate-limit data found yet)"
    p = rl.get("primary") or {}
    s = rl.get("secondary") or {}
    plan = rl.get("plan_type", "?")
    reached = rl.get("rate_limit_reached_type")
    line = (f"usage[{plan}]: 5h-window {p.get('used_percent','?')}% "
            f"(resets {_fmt_resets(p.get('resets_at'))}) | "
            f"7d-window {s.get('used_percent','?')}% "
            f"(resets {_fmt_resets(s.get('resets_at'))})")
    if reached:
        line += f"  ⚠ THROTTLED ({reached})"
    return line + "\n" + image_counters(rl)


def auto_concurrency(n_jobs: int, rl: dict | None) -> tuple[int, str]:
    """Decide serial vs parallel from job count and current usage."""
    if n_jobs <= 1:
        return 1, "single image → serial"
    base = n_jobs if n_jobs <= 3 else 4
    if rl:
        if rl.get("rate_limit_reached_type"):
            return 1, "currently throttled → serial"
        used = (rl.get("primary") or {}).get("used_percent", 0) or 0
        if used >= HARD_LIMIT:
            return 1, f"5h-window {used}% ≥ {HARD_LIMIT}% → serial"
        if used >= SOFT_LIMIT:
            return min(base, 2), f"5h-window {used}% ≥ {SOFT_LIMIT}% → cap 2"
    return base, f"{n_jobs} images → parallel x{base}"


# ---------------------------------------------------------------- generation

def build_prompt(spec: str, size: str | None, quality: str | None, forceful: bool,
                 refs: list[str] | None = None, ref_mode: str = "style") -> str:
    lines = []
    if refs:
        roles = {
            "style":       "STYLE AND COMPOSITION REFERENCE ONLY — match its camera angle, plating/arrangement, lighting mood, depth of field and overall look, but create a NEW subject/scene; do NOT reproduce it literally, do NOT copy logos, packaging, trade dress or any text from it",
            "edit":        "EDIT TARGET — preserve everything except what the spec asks to change",
            "subject":     "SUBJECT REFERENCE — keep this subject's identity/appearance, place it per the spec",
            "composition": "COMPOSITION REFERENCE ONLY — match framing, angle and layout; everything else (subject, palette, surfaces) should be new",
        }
        role = roles.get(ref_mode, roles["style"])
        for i in range(len(refs)):
            lines.append(f"Image {i+1} (attached): {role}.")
        lines.append("")
    lines += [
        "Render exactly ONE image by calling your built-in `image_gen` tool one time.",
        "", "Image spec:", spec.strip(),
    ]
    if size:
        lines.append(f"Aspect / size: {size}")
    if quality:
        lines.append(f"Quality: {quality}")
    lines += [
        "", "Hard rules:",
        "- Actually INVOKE the built-in `image_gen` tool to produce a real PNG file.",
        "- Do NOT write or run any code; do NOT use any CLI, python, or OPENAI_API_KEY.",
        "- Do NOT merely describe the image. Call image_gen exactly once.",
        "- After it returns, reply with just the word: DONE",
    ]
    if forceful:
        lines.insert(0, "IMPORTANT: last attempt did NOT generate an image. You MUST call image_gen now.\n")
    return "\n".join(lines)


def run_codex(prompt: str, reasoning: str, timeout: int, codex_bin: str, logf: Path,
              refs: list[str] | None = None) -> str | None:
    cmd = [
        codex_bin, "exec", "--json",
        "--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check",
        "-C", str(Path.home()), "-c", f"model_reasoning_effort={reasoning}",
    ]
    # -i is variadic in clap and would swallow a following positional prompt, so
    # attach refs one flag per file and ALWAYS pass the prompt via stdin ("-").
    for r in refs or []:
        cmd += ["-i", r]
    cmd.append("-")
    try:
        with open(logf, "wb") as lf:
            proc = subprocess.run(cmd, input=prompt.encode(), stdout=subprocess.PIPE,
                                  stderr=lf, timeout=timeout)
    except subprocess.TimeoutExpired:
        log(f"  [timeout {timeout}s] {logf.name}")
        return None
    except FileNotFoundError:
        log(f"  [error] codex binary not found: {codex_bin}")
        raise
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") == "thread.started" and obj.get("thread_id"):
            return obj["thread_id"]
    return None


def collect_outputs(thread_id: str) -> list[Path]:
    d = GEN_DIR / thread_id
    if not d.is_dir():
        return []
    return sorted(p for p in d.glob("*.png") if p.stat().st_size > 1024)


def was_silent_refusal(thread_id: str | None) -> bool:
    """True if the model finished WITHOUT ever invoking image_gen.

    Measured 2026-06-12: after ~15-20 images in a tight burst, the server
    silently disables image_gen (anti-abuse). The model then just replies
    "DONE" with zero image_generation_call events in the rollout, while the
    visible rate-limit windows still show ~0-1% — this rollout check is the
    only reliable signal. Content is NOT the cause (the same prompt succeeds
    again after a cooldown).
    """
    if not thread_id:
        return False
    hits = glob.glob(str(SESS_DIR / "**" / f"rollout-*{thread_id}*.jsonl"), recursive=True)
    for p in hits:
        try:
            text = Path(p).read_text(errors="ignore")
        except OSError:
            continue
        if '"image_generation_call"' in text:
            return False
        if '"task_complete"' in text or '"agent_message"' in text:
            return True
    return False


_AUTH_DEAD = False  # set when Codex auth is invalidated, to abort the batch fast (no retry storm)


def _is_auth_error(logf) -> bool:
    try:
        t = Path(logf).read_text(errors="ignore")
    except OSError:
        return False
    return ("refresh_token_invalidated" in t
            or ('"invalid_request_error"' in t and "token" in t)
            or '"code": "unauthorized"' in t)


def do_job(job: dict, args) -> dict:
    global _AUTH_DEAD
    name, spec = job["name"], job["prompt"]
    size = job.get("size", args.size)
    quality = job.get("quality", args.quality)
    refs = job.get("ref") or list(args.ref or [])
    if isinstance(refs, str):
        refs = [refs]
    ref_mode = job.get("ref_mode", args.ref_mode)
    out_dir = Path(args.out_dir)
    logdir = out_dir / ".logs"
    logdir.mkdir(parents=True, exist_ok=True)
    result = {"name": name, "prompt": spec, "ok": False, "files": [], "thread_id": None,
              "throttled": False, "refs": refs}

    missing = [r for r in refs if not Path(r).is_file()]
    if missing:
        log(f"  ✘ {name}: reference image(s) not found: {', '.join(missing)}")
        return result

    if _AUTH_DEAD:  # a sibling job already saw a dead token — don't pile on more requests
        result["auth_error"] = True
        return result

    attempts = args.retries + 1
    for attempt in range(1, attempts + 1):
        prompt = build_prompt(spec, size, quality, attempt > 1, refs, ref_mode)
        logf = logdir / f"{name}.attempt{attempt}.log"
        t0 = time.time()
        log(f"  ▶ {name} (attempt {attempt}/{attempts}{', refs=' + str(len(refs)) if refs else ''}) ...")
        thread_id = run_codex(prompt, args.reasoning, args.timeout, args.codex_bin, logf, refs)
        result["thread_id"] = thread_id
        if _is_auth_error(logf):
            _AUTH_DEAD = True
            result["auth_error"] = True
            log(f"  ✘ {name}: Codex auth invalidated (refresh_token_invalidated) — run `codex login`. "
                "Aborting batch, NOT retrying (avoids a request storm that trips the anti-abuse cooldown).")
            return result
        rl = rate_limits_for_thread(thread_id)
        if rl and rl.get("rate_limit_reached_type"):
            result["throttled"] = True
        pngs = collect_outputs(thread_id) if thread_id else []
        if not pngs and was_silent_refusal(thread_id):
            result["silent_refusal"] = True
            log(f"  ✘ {name}: image_gen was never invoked (server-side anti-abuse cooldown likely) "
                "— retrying immediately is useless; wait 30-60 min")
            return result
        if pngs:
            saved = []
            for i, src in enumerate(pngs):
                dest = out_dir / (f"{name}.png" if len(pngs) == 1 else f"{name}-{i+1}.png")
                if dest.exists() and not args.force:
                    k = 2
                    while (out_dir / f"{dest.stem}-v{k}{dest.suffix}").exists():
                        k += 1
                    dest = out_dir / f"{dest.stem}-v{k}{dest.suffix}"
                shutil.copy2(src, dest)
                saved.append(str(dest))
            result.update(ok=True, files=saved)
            cooldown_event(len(saved))  # record into shared cooldown window (best-effort)
            for sp in saved:  # stage each PNG into R2 for the NAS puller to archive (best-effort)
                upload_image(sp, Path(sp).stem, thread_id, spec)
            log(f"  ✔ {name} -> {', '.join(saved)}  ({time.time()-t0:.0f}s)")
            return result
        log(f"  ✘ {name} attempt {attempt} produced no image ({time.time()-t0:.0f}s)"
            + ("  [throttled]" if result["throttled"] else ""))
        if result["throttled"] and attempt < attempts:
            time.sleep(min(15 * attempt, 45))  # back off before retry
    return result


def load_jobs(args) -> list[dict]:
    raw: list[dict] = []
    if args.jobs:
        for ln in Path(args.jobs).read_text().splitlines():
            if ln.strip():
                raw.append(json.loads(ln))
    raw += [{"prompt": p} for p in args.prompt]
    raw += [{"prompt": p} for p in args.positional]
    if not raw:
        log("No prompts given. Pass prompts as args, -p, or --jobs file.")
        sys.exit(2)
    jobs, seen = [], {}
    for idx, item in enumerate(raw, 1):
        base = item.get("name") or slugify(item["prompt"], f"image2-{idx:02d}")
        variants = item.get("n", args.n)
        for v in range(1, variants + 1):
            nm = base if variants == 1 else f"{base}-v{v}"
            seen[nm] = seen.get(nm, 0) + 1
            if seen[nm] > 1:
                nm = f"{nm}-{seen[nm]}"
            jobs.append({k: item[k] for k in ("prompt", "size", "quality", "ref", "ref_mode") if k in item} | {"name": nm})
    return jobs


def main() -> None:
    ap = argparse.ArgumentParser(description="Parallel gpt-image-2 generation via Codex built-in image_gen.")
    ap.add_argument("positional", nargs="*", help="image prompts (one image each)")
    ap.add_argument("-p", "--prompt", action="append", default=[], help="image prompt (repeatable)")
    ap.add_argument("--jobs", help="JSONL file; each line {prompt, name?, size?, quality?, n?}")
    ap.add_argument("--out-dir", default="./image2-out", help="where final PNGs are copied (default ./image2-out)")
    ap.add_argument("--concurrency", default="auto",
                    help="'auto' (default; decides serial/parallel from count + current usage) or an integer")
    ap.add_argument("--n", type=int, default=1, help="variants per prompt (default 1)")
    ap.add_argument("--size", default=None, help='size hint, e.g. "square 1024x1024", "portrait 1024x1536", "4K landscape"')
    ap.add_argument("--quality", default=None, help="low | medium | high | auto")
    ap.add_argument("--ref", action="append", default=[],
                    help="reference image file attached to EVERY job via codex -i (repeatable); "
                         "per-job override: JSONL field 'ref' (string or list)")
    ap.add_argument("--ref-mode", default="style", choices=["style", "edit", "subject", "composition"],
                    help="how attached refs are labeled in the prompt (default style: "
                         "match look/composition but create a new subject, never copy literally)")
    ap.add_argument("--reasoning", default="low", help="codex model_reasoning_effort (default low = faster tool call)")
    ap.add_argument("--retries", type=int, default=1, help="retries if a job makes no image (default 1)")
    ap.add_argument("--timeout", type=int, default=360, help="per-job timeout seconds (default 360)")
    ap.add_argument("--force", action="store_true", help="overwrite existing output files")
    ap.add_argument("--codex-bin", default=os.environ.get("CODEX_BIN", "codex"))
    ap.add_argument("--usage", action="store_true", help="print latest Codex usage/rate-limits and exit")
    ap.add_argument("--dry-run", action="store_true", help="print planned jobs and exit")
    args = ap.parse_args()

    if args.usage:
        counter_report()  # freshen this device's number before reading totals
        print(format_usage(latest_rate_limits()))
        shared = counter_summary()
        if shared:
            print(shared)
        cl = cooldown_line()
        if cl:
            print(cl)
        return

    jobs = load_jobs(args)
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)

    gate = cooldown_check()
    if gate and gate.get("in_cooldown"):
        mins = (gate.get("cooldown_remaining_sec", 0) + 59) // 60
        log(f"⏳ SHARED COOLDOWN active: {gate.get('recent_in_window')} gens in last "
            f"{gate.get('window_min')}min across all devices — server likely to silently refuse. "
            f"Suggest waiting ~{mins}min (until {gate.get('cooldown_until_iso','?')}).")

    pre = latest_rate_limits()
    if str(args.concurrency).lower() == "auto":
        conc, why = auto_concurrency(len(jobs), pre)
    else:
        conc = max(1, min(int(args.concurrency), len(jobs)))
        why = f"manual x{conc}"

    log(f"image2: {len(jobs)} image(s) | concurrency={conc} ({why}) | out-dir={args.out_dir}")
    log("  " + format_usage(pre).replace("\n", "\n  "))
    for j in jobs:
        refs = j.get("ref") or args.ref
        tag = f" [+{len(refs) if isinstance(refs, list) else 1} ref]" if refs else ""
        log(f"  - {j['name']}{tag}: {j['prompt'][:70]}")
    if args.dry_run:
        log("[dry-run] not executing.")
        print(json.dumps({"jobs": jobs, "out_dir": args.out_dir, "concurrency": conc, "decision": why}, ensure_ascii=False, indent=2))
        return

    t0 = time.time()
    results: list[dict] = []
    lease_acquire(f"{len(jobs)} img -> {Path(args.out_dir).name}")  # cross-device mutex before generating
    try:
        with cf.ThreadPoolExecutor(max_workers=conc) as ex:
            futs = [ex.submit(do_job, j, args) for j in jobs]
            for fut in cf.as_completed(futs):
                results.append(fut.result())
    finally:
        lease_release()  # release on success, failure, or abort

    ok = [r for r in results if r["ok"]]
    bad = [r for r in results if not r["ok"]]
    throttled = any(r.get("throttled") for r in results)
    refused = [r for r in results if r.get("silent_refusal")]
    auth_dead = [r for r in results if r.get("auth_error")]
    results.sort(key=lambda r: r["name"])
    post = latest_rate_limits()
    counter_report()
    log("")
    log(f"Done: {len(ok)}/{len(results)} succeeded in {time.time()-t0:.0f}s")
    log("  " + format_usage(post).replace("\n", "\n  "))
    shared = counter_summary()
    if shared:
        log("  " + shared)
    if throttled:
        log("  ⚠ hit rate limiting — lower --concurrency or wait for the 5h window to reset.")
    if refused:
        log(f"  ⚠ {len(refused)} job(s) hit the SILENT image_gen cooldown (anti-abuse, invisible in "
            "usage%) — stop generating and retry in 30-60 min; do not burn retries now.")
    if auth_dead:
        log(f"  ⛔ {len(auth_dead)} job(s) hit a DEAD Codex token (refresh_token_invalidated) — run "
            "`codex login` to re-auth, then re-run. Batch was aborted early to avoid a request storm.")
    for r in bad:
        log(f"  FAILED: {r['name']} (thread={r['thread_id']}) — see {args.out_dir}/.logs/")
    print(json.dumps({
        "out_dir": str(Path(args.out_dir).resolve()),
        "concurrency": conc, "decision": why,
        "succeeded": len(ok), "failed": len(bad), "throttled": throttled,
        "silent_refusals": len(refused),
        "auth_errors": len(auth_dead),
        "usage": post, "results": results,
    }, ensure_ascii=False, indent=2))
    sys.exit(0 if not bad else 1)


if __name__ == "__main__":
    main()
