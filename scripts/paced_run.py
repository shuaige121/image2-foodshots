#!/usr/bin/env python3
# Cooldown-paced + cross-device-LEASED batch runner.
# Before each chunk it acquires the single generation lease on the worker (so two
# devices never run codex concurrently -> no OAuth refresh-token race), waits on the
# cooldown window, generates, then releases the lease.
# usage: paced_run.py <jobs.jsonl> <out_dir> <reps>
import json, os, sys, time, subprocess, urllib.request
JOBS, OUT, REPS = sys.argv[1], sys.argv[2], int(sys.argv[3])
os.makedirs(OUT, exist_ok=True)
IMG2 = os.path.expanduser("~/.claude/skills/image2/scripts/image2.py")
CFG = json.load(open(os.path.expanduser("~/.codex/image2-counter.json")))
EP, TOK, DEVICE = CFG["endpoint"], CFG["token"], CFG.get("device", "unknown")
LOG = open(f"{OUT}/progress.log", "a", buffering=1)
def log(m): LOG.write(f"{time.strftime('%H:%M:%S')} {m}\n")

def call(method, path, body=None):
    try:
        r = urllib.request.Request(EP + path, method=method,
            data=json.dumps(body).encode() if body else None,
            headers={"Authorization": f"Bearer {TOK}", "Content-Type": "application/json", "User-Agent": "paced/2"})
        return json.loads(urllib.request.urlopen(r, timeout=8).read())
    except Exception:
        return None

def recent():
    r = call("GET", "/check");  return r.get("recent_in_window", 0) if r else 0
def wait_clear():
    for _ in range(40):
        if recent() <= 2: return
        time.sleep(120)
def acquire_lease(task, ttl=900):
    for _ in range(80):  # wait up to ~20 min for the lock
        r = call("POST", "/claim", {"device": DEVICE, "task": task, "ttl": ttl})
        if r is None:  # worker unreachable -> proceed uncoordinated
            log("  (coordinator unreachable; proceeding without lease)"); return None
        if r.get("granted"):
            log(f"  🔒 lease acquired ({r.get('lease_id','')[:8]})"); return r.get("lease_id")
        log(f"  ⏳ lease held by {r.get('holder')} ({r.get('task')}) — wait {min(20,r.get('wait_sec',15))}s")
        time.sleep(min(20, max(5, r.get("wait_sec", 15))))
    log("  ⚠ waited >20min for lease; proceeding"); return None
def release_lease(lid):
    if lid: call("POST", "/release", {"lease_id": lid}); log("  🔓 lease released")

base = [json.loads(l) for l in open(JOBS) if l.strip()]
CHUNK = 9
work = []
for rep in range(1, REPS + 1):
    for j in base:
        nm = f"{j['name']}_rep{rep}"
        if not os.path.exists(f"{OUT}/{nm}.png"):
            work.append({"prompt": j["prompt"], "name": nm})
log(f"=== start: {len(work)} of {REPS*len(base)} ===")
i = 0
while i < len(work):
    chunk = work[i:i+CHUNK]
    # NOTE: paced_run no longer claims the lease itself. image2.py now self-claims the
    # cross-device mutex AND heartbeats it for the whole batch, so it's the single lease
    # authority — no nested lease, no TTL-expiry-mid-chunk gap. We only pace the cooldown here.
    wait_clear()
    jf = f"{OUT}/_chunk.jsonl"
    open(jf, "w").write("\n".join(json.dumps(c) for c in chunk) + "\n")
    log(f"chunk {i//CHUNK+1}: {[c['name'] for c in chunk]}")
    r = subprocess.run(["python3", IMG2, "--jobs", jf, "--out-dir", OUT, "--size", "square 1024x1024",
                        "--quality", "high", "--timeout", "480", "--concurrency", "3"], capture_output=True, text=True)
    if '"auth_errors": 0' not in (r.stdout or "") and '"auth_errors"' in (r.stdout or ""):
        log("  ⛔ dead Codex token detected — stopping; run `codex login`"); break
    log(f"  done; pngs now {len([x for x in os.listdir(OUT) if x.endswith('.png')])}")
    i += CHUNK
log("=== COMPLETE ===")
print("DONE", len([x for x in os.listdir(OUT) if x.endswith('.png')]))
