#!/usr/bin/env python3
"""e2b_stage_bench.py -- create / pause / resume benchmark for kuasar-sandbox.

This mirrors the CubeSandbox benchmark shape (create_bench.py +
dd_warm_stage_bench.py + *_stage_batch.py) but drives the E2B-compatible HTTP
API directly, so the same driver also runs against a CubeSandbox node when its
responses expose a sandbox state field.

Scenarios
  create        C concurrent creates; per-request wall and whole-batch wall,
                p50/p95, warm-up rounds discarded up front, retries counted.
  pause-resume  create C -> pause all -> optional page-cache warm -> resume all,
                two warm-up rounds discarded by default, one JSONL record per
                measured round with SIDs and wall times.
  analyze       parse a kuasar conductor journal/log by SID and print the
                per-phase breakdown (create/resume phases, artifact prepare,
                pause capture, launch total).

Flags shared by the scenarios (place them after the subcommand):
  --strict         one attempt per operation; the first error aborts the run.
  --max-retries N  additional attempts per operation (default 2 = 3 total).
  --append         allow appending to an existing --out/--ops-out file. Default
                   is to refuse, so two arms cannot silently share one file.
  --arm NAME       label recorded in samples and ops records (e.g. off|on).

Every attempt is written to the --ops-out JSONL, including failed ones, with
ok, attempt, phase (create|wait|pause|connect|restore) and err_class, so the
failure rate is visible instead of being smoothed away by retries.

Examples
  export E2B_API_URL=http://127.0.0.1:3000
  export E2B_API_KEY=e2b_...
  export E2B_TEMPLATE_ID=e2b-snp-...

  # create only, C=1 and C=10
  python3 e2b_stage_bench.py create -c 1 -n 10
  python3 e2b_stage_bench.py create -c 10 -n 5

  # full create -> pause -> resume at C=1,5,10 (needs local checkpoint dirs
  # for --warm; omit --warm for a pure remote/cold run)
  python3 e2b_stage_bench.py pause-resume --concurrency 1,5,10 -n 3 \
      --warm --warm-base-root /var/lib/sandbox \
      --out arm-off.jsonl --ops-out arm-off.ops.jsonl --arm off

  # negative control: the same binary with vsock_reset_before_freeze=on must
  # use a different out file (or --append) and its own arm label
  python3 e2b_stage_bench.py pause-resume --concurrency 1 -n 30 --strict \
      --out arm-on.jsonl --ops-out arm-on.ops.jsonl --arm on

  # phase breakdown after the run
  journalctl -o json --no-pager --since "-20 min" > journal.json
  python3 e2b_stage_bench.py analyze --log conductor.log --samples samples.jsonl
  python3 test/perf/stage_tree.py --ops arm-on.ops.jsonl --journal journal.json --strict
"""
import argparse
import json
import math
import os
import statistics
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

RETRIES = {"create": 0, "pause": 0, "resume": 0, "rounds": 0, "delete": 0}

# --strict: one attempt per operation, first error aborts the run. --max-retries
# is the number of *additional* attempts (the historical default of 3 total
# attempts is preserved by the default of 2).
STRICT = False
MAX_RETRIES = 2

# Per-operation wall-clock records (epoch ms) used by stage_tree.py to line
# client observations up with the node-side perf marks in the journal. Every
# attempt is recorded (ok=false included) so failures are not invisible.
OPS = []
OPS_OUT = ""
CUR_CONC = None
ARM = ""
APPEND = False
SUPPRESS_OPS = False


def attempt_budget():
    return 1 if STRICT else 1 + max(0, MAX_RETRIES)


def record_op(kind, sid, start_epoch_s, end_epoch_s, ok=True, attempt=1,
              err=None, phase=""):
    if SUPPRESS_OPS:
        return
    rec = {
        "op": kind,
        "sid": sid,
        "c": CUR_CONC,
        "start_ms": start_epoch_s * 1000.0,
        "end_ms": end_epoch_s * 1000.0,
        "dur_ms": (end_epoch_s - start_epoch_s) * 1000.0,
        "ok": bool(ok),
        "attempt": attempt,
    }
    if phase:
        rec["phase"] = phase
    if err is not None:
        rec["err_class"] = type(err).__name__
        rec["err"] = str(err)[:300]
    if ARM:
        rec["arm"] = ARM
    OPS.append(rec)


def flush_ops():
    if not OPS_OUT or not OPS:
        return
    path = Path(OPS_OUT)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for rec in OPS:
            f.write(json.dumps(rec) + "\n")
    OPS.clear()


def percentile(data, p):
    s = sorted(data)
    if not s:
        return 0.0
    k = int(math.ceil(len(s) * p / 100.0)) - 1
    return s[max(0, min(k, len(s) - 1))]


def fmt(v, w=10):
    return f"{v:>{w}.1f}"


class Api:
    def __init__(self, base, key, timeout):
        self.base = base.rstrip("/")
        self.key = key
        self.timeout = timeout

    def call(self, method, path, body=None, timeout=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method)
        # kuasar uses X-API-KEY; CubeSandbox accepts Bearer. Send both.
        req.add_header("X-API-KEY", self.key)
        req.add_header("Authorization", "Bearer " + self.key)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw.strip() else None


def sandbox_state(api, sid):
    sb = api.call("GET", "/sandboxes/" + sid)
    if not isinstance(sb, dict):
        return ""
    return str(sb.get("state") or "").lower()


def wait_state(api, sid, want, timeout):
    end = time.monotonic() + timeout
    last = ""
    while time.monotonic() < end:
        sb = api.call("GET", "/sandboxes/" + sid)
        if isinstance(sb, dict) and "state" not in sb:
            # Server does not expose lifecycle state (e.g. raw CubeSandbox
            # create/connect are synchronous); treat the call as ready.
            return
        last = sandbox_state(api, sid)
        if last == want:
            return
        time.sleep(0.05)
    raise TimeoutError(f"sandbox {sid} did not reach {want!r} (last={last!r})")


def create_one(api, tpl, timeout):
    last = None
    budget = attempt_budget()
    for attempt in range(1, budget + 1):
        t = time.monotonic()
        t_epoch = time.time()
        sid = ""
        phase = "create"
        try:
            body = {"templateID": tpl, "timeout": timeout}
            r = api.call("POST", "/sandboxes", body, timeout=max(120, timeout))
            sid = (r or {}).get("sandboxID") or (r or {}).get("sandbox_id")
            if not sid:
                raise RuntimeError("create returned no sandboxID")
            phase = "wait"
            wait_state(api, sid, "running", timeout)
        except Exception as exc:  # noqa: BLE001 - retry env flake like create_bench
            end_epoch = t_epoch + (time.monotonic() - t)
            record_op("create", sid, t_epoch, end_epoch, ok=False, attempt=attempt,
                      err=exc, phase=phase)
            last = exc
            if attempt >= budget:
                break
            RETRIES["create"] += 1
            print(f"  [create-retry{attempt}] {str(exc)[:90]}", file=sys.stderr, flush=True)
            time.sleep(1)
            continue
        end_epoch = t_epoch + (time.monotonic() - t)
        record_op("create", sid, t_epoch, end_epoch, ok=True, attempt=attempt)
        return (time.monotonic() - t) * 1000.0, sid
    raise last


def pause_one(api, sid, memory, timeout):
    last = None
    budget = attempt_budget()
    for attempt in range(1, budget + 1):
        t = time.monotonic()
        t_epoch = time.time()
        phase = "pause"
        try:
            api.call("POST", f"/sandboxes/{sid}/pause", {"memory": memory}, timeout=timeout)
            phase = "wait"
            wait_state(api, sid, "paused", timeout)
        except Exception as exc:  # noqa: BLE001
            end_epoch = t_epoch + (time.monotonic() - t)
            record_op("pause", sid, t_epoch, end_epoch, ok=False, attempt=attempt,
                      err=exc, phase=phase)
            last = exc
            if attempt >= budget:
                break
            if phase == "wait":
                # The pause request was accepted; repeating it could be a no-op
                # or a 409. Report the wait failure instead of duplicating it.
                break
            RETRIES["pause"] += 1
            print(f"  [pause-retry{attempt} {sid}] {str(exc)[:90]}",
                  file=sys.stderr, flush=True)
            time.sleep(1)
            continue
        end_epoch = t_epoch + (time.monotonic() - t)
        record_op("pause", sid, t_epoch, end_epoch, ok=True, attempt=attempt)
        return (time.monotonic() - t) * 1000.0
    raise last


def resume_one(api, sid, timeout):
    last = None
    budget = attempt_budget()
    for attempt in range(1, budget + 1):
        t = time.monotonic()
        t_epoch = time.time()
        # "connect" is the POST /connect admission; "restore" is the client-side
        # wait for the launch worker and VM restore. The guest restore ACK is
        # node-side and not separately observable from this API, so it is part
        # of the restore phase here.
        phase = "connect"
        try:
            api.call("POST", f"/sandboxes/{sid}/connect", {"timeout": timeout}, timeout=timeout)
            phase = "restore"
            wait_state(api, sid, "running", timeout)
        except Exception as exc:  # noqa: BLE001
            end_epoch = t_epoch + (time.monotonic() - t)
            record_op("resume", sid, t_epoch, end_epoch, ok=False, attempt=attempt,
                      err=exc, phase=phase)
            last = exc
            if attempt >= budget:
                break
            if phase == "restore":
                # /connect was accepted; re-sending it could duplicate the
                # restore. Record the restore-phase failure and stop retrying.
                break
            RETRIES["resume"] += 1
            print(f"  [resume-retry{attempt} {sid}] {str(exc)[:90]}",
                  file=sys.stderr, flush=True)
            time.sleep(1)
            continue
        end_epoch = t_epoch + (time.monotonic() - t)
        record_op("resume", sid, t_epoch, end_epoch, ok=True, attempt=attempt)
        return (time.monotonic() - t) * 1000.0
    raise last


def delete_one(api, sid):
    last = None
    budget = attempt_budget()
    for attempt in range(1, budget + 1):
        try:
            api.call("DELETE", "/sandboxes/" + sid, timeout=120)
            return
        except Exception as exc:  # noqa: BLE001 - cleanup is best effort
            last = exc
            RETRIES["delete"] += 1
            if attempt >= budget:
                break
            time.sleep(0.5)
    if STRICT:
        raise last
    print(f"  [delete-failed {sid}] {str(last)[:90]}", file=sys.stderr, flush=True)


def cleanup(api, sids):
    if not sids:
        return
    with ThreadPoolExecutor(max_workers=len(sids)) as pool:
        list(pool.map(lambda s: delete_one(api, s), sids))


def warm_checkpoints(base_root, sids):
    """Read each sandbox's checkpoint files to warm the host page cache.

    This is the kuasar analogue of CubeSandbox's ``dd`` prewarm over the
    pause-snapshot and template volume files.
    """
    root = Path(base_root)
    files = []
    for sid in sids:
        d = root / "sandboxes" / sid / "checkpoint"
        if not d.is_dir():
            continue
        for p in d.rglob("*"):
            if p.is_file():
                files.append(p)
    if not files:
        return 0, 0

    def read_one(p):
        try:
            with p.open("rb") as f:
                while f.read(4 << 20):
                    pass
            return p.stat().st_size
        except OSError:
            return 0

    total = 0
    with ThreadPoolExecutor(max_workers=min(16, len(files))) as pool:
        for size in pool.map(read_one, files):
            total += size
    return len(files), total


def create_batch(api, tpl, count, timeout):
    reqs, sids = [], []
    with ThreadPoolExecutor(max_workers=count) as pool:
        futs = [pool.submit(create_one, api, tpl, timeout) for _ in range(count)]
        for f in as_completed(futs):
            ms, sid = f.result()
            reqs.append(ms)
            sids.append(sid)
    return reqs, sids


def run_create_round(api, tpl, count, timeout):
    global CUR_CONC
    CUR_CONC = count
    t0 = time.monotonic()
    sids = []
    try:
        reqs, sids = create_batch(api, tpl, count, timeout)
        flush_ops()
    finally:
        wall_ms = (time.monotonic() - t0) * 1000.0
        cleanup(api, sids)
    return {"wall_ms": wall_ms, "reqs": reqs, "sids": sids}


def cmd_create(args, api):
    global SUPPRESS_OPS
    print(
        "# api=%s template=%s concurrency=%d rounds=%d settle=%.1fs"
        % (args.api, args.template, args.concurrency, args.rounds, args.settle_secs)
    )
    print(
        f"{'conc':>4}  {'op':>6}  {'wall_avg':>9} {'wall_p95':>9} {'per':>8}  |  "
        f"{'req_avg':>9} {'req_p50':>9} {'req_p95':>9} {'req_max':>9} {'n_req':>6}"
    )
    print("-" * 100)

    for _ in range(args.warmup_rounds):
        previous_suppress = SUPPRESS_OPS
        SUPPRESS_OPS = True
        try:
            run_create_round(api, args.template, args.concurrency, args.timeout)
            break
        except Exception as exc:  # noqa: BLE001
            if STRICT:
                raise
            RETRIES["rounds"] += 1
            print(f"  [warmup-retry] {str(exc)[:90]}", file=sys.stderr, flush=True)
            time.sleep(5)
        finally:
            SUPPRESS_OPS = previous_suppress
    time.sleep(args.settle_secs)

    walls, reqs = [], []
    done, attempts = 0, 0
    max_attempts = args.rounds if STRICT else args.rounds * 3
    while done < args.rounds and attempts < max_attempts:
        attempts += 1
        try:
            r = run_create_round(api, args.template, args.concurrency, args.timeout)
        except Exception as exc:  # noqa: BLE001
            if STRICT:
                raise
            RETRIES["rounds"] += 1
            print(f"  [round-retry] {str(exc)[:90]}", file=sys.stderr, flush=True)
            time.sleep(5)
            continue
        walls.append(r["wall_ms"])
        reqs.extend(r["reqs"])
        done += 1
        time.sleep(args.settle_secs)

    per = statistics.mean(walls) / args.concurrency if walls else 0.0
    print(
        f"{args.concurrency:>4}  {'create':>6}  "
        f"{fmt(statistics.mean(walls))} {fmt(percentile(walls, 95))} {fmt(per, 8)}  |  "
        f"{fmt(statistics.mean(reqs))} {fmt(statistics.median(reqs))} "
        f"{fmt(percentile(reqs, 95))} {fmt(max(reqs))} {len(reqs):>6}"
    )
    print(
        f"# retries: create={RETRIES['create']} rounds_retried={RETRIES['rounds']} "
        f"delete={RETRIES['delete']} (env flake, not a result metric)"
    )


def run_pr_round(api, tpl, count, args, warmup=False):
    global CUR_CONC, SUPPRESS_OPS
    CUR_CONC = count
    previous_suppress = SUPPRESS_OPS
    SUPPRESS_OPS = warmup
    t0 = time.monotonic()
    sids = []
    try:
        _, sids = create_batch(api, tpl, count, args.timeout)
        create_ms = (time.monotonic() - t0) * 1000.0

        t1 = time.monotonic()
        with ThreadPoolExecutor(max_workers=count) as pool:
            list(pool.map(lambda s: pause_one(api, s, args.memory, args.timeout), sids))
        pause_ms = (time.monotonic() - t1) * 1000.0

        warmed_files = warmed_bytes = 0
        if args.warm:
            warmed_files, warmed_bytes = warm_checkpoints(args.warm_base_root, sids)

        t2 = time.monotonic()
        with ThreadPoolExecutor(max_workers=count) as pool:
            list(pool.map(lambda s: resume_one(api, s, args.timeout), sids))
        resume_ms = (time.monotonic() - t2) * 1000.0
    finally:
        SUPPRESS_OPS = previous_suppress
        cleanup(api, sids)

    flush_ops()
    rec = {
        "c": count,
        "round": args.round_index,
        "sids": sids,
        "create_ms": create_ms,
        "pause_ms": pause_ms,
        "resume_ms": resume_ms,
        "warmed_files": warmed_files,
        "warmed_bytes": warmed_bytes,
        "memory": args.memory,
    }
    if ARM:
        rec["arm"] = ARM
    if warmup:
        print(
            "W C=%d round=%d create=%.1f pause=%.1f resume=%.1f (discarded)"
            % (count, args.round_index, create_ms, pause_ms, resume_ms),
            flush=True,
        )
        return rec
    with open(args.out, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
    print(
        "R C=%d round=%d create=%.1f pause=%.1f resume=%.1f warm=%d/%dB"
        % (count, args.round_index, create_ms, pause_ms, resume_ms, warmed_files, warmed_bytes),
        flush=True,
    )
    return rec


def cmd_pause_resume(args, api):
    concs = [int(x) for x in args.concurrency.split(",") if x.strip()]
    print(
        "# api=%s template=%s concurrency=%s rounds=%d warmup=%d memory=%s warm=%s strict=%s"
        % (args.api, args.template, concs, args.rounds, args.warmup_rounds,
           args.memory, args.warm, STRICT)
    )
    for count in concs:
        # Warm-up rounds are discarded: no samples, no ops records. They absorb
        # first-touch page cache and runner-pool effects before the measured
        # rounds, which matters for paired A/B runs.
        for rnd in range(args.warmup_rounds):
            args.round_index = rnd
            try:
                run_pr_round(api, args.template, count, args, warmup=True)
            except Exception as exc:  # noqa: BLE001
                if STRICT:
                    raise
                RETRIES["rounds"] += 1
                print(f"  [warmup-retry C={count} r={rnd}] {str(exc)[:120]}",
                      file=sys.stderr, flush=True)
            time.sleep(args.settle_secs)
        for rnd in range(args.rounds):
            args.round_index = rnd
            try:
                run_pr_round(api, args.template, count, args)
            except Exception as exc:  # noqa: BLE001
                if STRICT:
                    raise
                RETRIES["rounds"] += 1
                print(f"  [round-retry C={count} r={rnd}] {str(exc)[:120]}", file=sys.stderr, flush=True)
            time.sleep(args.settle_secs)

    recs = []
    if Path(args.out).exists():
        recs = [json.loads(line) for line in Path(args.out).read_text().splitlines() if line.strip()]
    print("\n# summary (wall, ms)")
    header = f"{'conc':>4}  {'create_avg':>10} {'pause_avg':>9} {'resume_avg':>10} {'rounds':>6}"
    if ARM:
        header = f"{'conc':>4}  {'arm':>6}  " + header.split("  ", 1)[1]
    print(header)
    for count in concs:
        xs = [r for r in recs if r.get("c") == count]
        if not xs:
            continue
        arms = sorted({r.get("arm", "") for r in xs})
        for arm in arms:
            subset = [r for r in xs if r.get("arm", "") == arm]
            prefix = f"{count:>4}  {arm:>6}  " if ARM else f"{count:>4}  "
            print(
                f"{prefix}{statistics.mean(r['create_ms'] for r in subset):>10.1f} "
                f"{statistics.mean(r['pause_ms'] for r in subset):>9.1f} "
                f"{statistics.mean(r['resume_ms'] for r in subset):>10.1f} {len(subset):>6}"
            )
    print(
        f"# retries: create={RETRIES['create']} pause={RETRIES['pause']} "
        f"resume={RETRIES['resume']} rounds_retried={RETRIES['rounds']} "
        f"delete={RETRIES['delete']} (env flake, not a result metric)"
    )


PHASE_RE = None


def parse_conductor_log(path):
    import re

    global PHASE_RE
    if PHASE_RE is None:
        PHASE_RE = {
            "sid": re.compile(r"\bsid=([0-9A-Za-z-]+)"),
            "phase": re.compile(r"kind=(\w+).*?\bphase=(\w+)\s+duration=([0-9.]+)ms"),
            "prepare": re.compile(r"artifact_prepare_handoff_duration=([0-9.]+)ms"),
            "capture": re.compile(r"pause capture completed.*?capture_ms=([0-9.]+)"),
            "total": re.compile(r"starting_total_duration=([0-9.]+)"),
        }
    phases = {}
    for line in Path(path).read_text(errors="replace").splitlines():
        m = PHASE_RE["sid"].search(line)
        if not m:
            continue
        sid = m.group(1)
        rec = phases.setdefault(sid, {})
        m = PHASE_RE["phase"].search(line)
        if m:
            kind, phase, dur = m.group(1), m.group(2), float(m.group(3))
            rec[f"{kind}.{phase}"] = dur
            continue
        m = PHASE_RE["prepare"].search(line)
        if m:
            rec["artifact_prepare"] = float(m.group(1))
            continue
        m = PHASE_RE["capture"].search(line)
        if m:
            rec["pause.capture"] = float(m.group(1))
            continue
        m = PHASE_RE["total"].search(line)
        if m:
            rec["launch.total"] = float(m.group(1))
    return phases


def cmd_analyze(args, api=None):
    phases = parse_conductor_log(args.log)
    sids = set(phases)
    meta = {}
    if args.samples and Path(args.samples).exists():
        for line in Path(args.samples).read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            for sid in rec.get("sids", []):
                meta[sid] = rec
                sids.add(sid)

    rows = []
    for sid in sids:
        rec = phases.get(sid, {})
        where = meta.get(sid, {})
        row = {"sid": sid, "c": where.get("c"), "round": where.get("round")}
        row.update(rec)
        rows.append(row)

    keys = sorted({k for r in rows for k in r if k not in ("sid", "c", "round")})
    print(f"# analyzed sids: {len(rows)} log={args.log} samples={args.samples or '-'}")
    print(f"{'phase':<32} {'n':>4} {'mean':>9} {'p50':>9} {'p95':>9}")
    print("-" * 70)
    for key in keys:
        vals = [r[key] for r in rows if key in r]
        if not vals:
            continue
        print(
            f"{key:<32} {len(vals):>4} {statistics.mean(vals):>9.1f} "
            f"{statistics.median(vals):>9.1f} {percentile(vals, 95):>9.1f}"
        )

    if args.out:
        Path(args.out).write_text(json.dumps(rows, indent=1) + "\n")
        print(f"# rows written to {args.out}")


def ensure_fresh(path, what):
    """Refuse to append a new run onto an existing samples/ops file.

    Reusing one path across arms silently mixes them in the summary; callers
    that really want to extend a file must pass --append.
    """
    if not path or APPEND:
        return
    p = Path(path)
    if p.exists() and p.stat().st_size > 0:
        raise SystemExit(
            "%s %s already exists and is non-empty; use a unique path per run/arm "
            "or pass --append" % (what, path))


def main():
    global OPS_OUT, STRICT, MAX_RETRIES, APPEND, ARM
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api", default=os.environ.get("E2B_API_URL", "http://127.0.0.1:3000"))
    p.add_argument("--key", default=os.environ.get("E2B_API_KEY", ""))
    p.add_argument("--template", default=os.environ.get("E2B_TEMPLATE_ID", ""))
    p.add_argument("--timeout", type=int, default=600, help="sandbox timeout / wait budget (s)")
    p.add_argument("--settle-secs", type=float, default=1.0)
    sub = p.add_subparsers(dest="cmd", required=True)

    # Options shared by every scenario. They are attached to the subcommands so
    # `... create --strict` works; pass them after the subcommand.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--strict", action="store_true",
                        help="fail on first error; one attempt per operation")
    common.add_argument("--max-retries", type=int, default=2,
                        help="additional attempts per operation (default 2 = 3 total)")
    common.add_argument("--append", action="store_true",
                        help="allow appending to an existing --out/--ops-out file")
    common.add_argument("--arm", default="", help="label recorded in samples/ops (e.g. off|on)")
    # Also accept the endpoint flags after the subcommand. SUPPRESS keeps the
    # top-level value when they are given in the usual (leading) position.
    common.add_argument("--api", default=argparse.SUPPRESS)
    common.add_argument("--key", default=argparse.SUPPRESS)
    common.add_argument("--template", default=argparse.SUPPRESS)
    common.add_argument("--timeout", type=int, default=argparse.SUPPRESS)
    common.add_argument("--settle-secs", type=float, default=argparse.SUPPRESS)

    c = sub.add_parser("create", parents=[common])
    c.add_argument("-c", "--concurrency", type=int, default=1)
    c.add_argument("-n", "--rounds", type=int, default=10)
    c.add_argument("--warmup-rounds", type=int, default=1)
    c.add_argument("--ops-out", default="", help="per-operation epoch-ms JSONL for stage_tree.py")
    c.set_defaults(func=cmd_create)

    pr = sub.add_parser("pause-resume", parents=[common])
    pr.add_argument("--concurrency", default="1,5,10")
    pr.add_argument("-n", "--rounds", type=int, default=3)
    pr.add_argument("--warmup-rounds", type=int, default=2,
                    help="full create/pause/resume rounds discarded before measurement")
    pr.add_argument("--memory", type=lambda v: v.lower() != "false", default=True)
    pr.add_argument("--warm", action="store_true")
    pr.add_argument("--warm-base-root", default="/var/lib/sandbox")
    pr.add_argument("--out", default="e2b-stage-samples.jsonl")
    pr.add_argument("--ops-out", default="", help="per-operation epoch-ms JSONL for stage_tree.py")
    pr.set_defaults(func=cmd_pause_resume)

    a = sub.add_parser("analyze", parents=[common])
    a.add_argument("--log", required=True)
    a.add_argument("--samples", default="")
    a.add_argument("--out", default="")
    a.set_defaults(func=cmd_analyze)

    args = p.parse_args()
    STRICT = args.strict
    MAX_RETRIES = max(0, args.max_retries)
    APPEND = args.append
    ARM = args.arm
    if getattr(args, "ops_out", ""):
        OPS_OUT = args.ops_out
    if args.cmd != "analyze":
        if args.ops_out:
            ensure_fresh(args.ops_out, "ops output")
        if args.cmd == "pause-resume":
            ensure_fresh(args.out, "samples output")
    if args.cmd != "analyze" and not args.template:
        p.error("no template: pass --template or set E2B_TEMPLATE_ID")
    if args.cmd != "analyze" and not args.key:
        p.error("no API key: pass --key or set E2B_API_KEY")
    api = Api(args.api, args.key, args.timeout) if args.cmd != "analyze" else None
    args.func(args, api)


if __name__ == "__main__":
    main()
