#!/usr/bin/env python3
"""restore_stress.py -- full-cycle stress and negative-control harness for the
design v3 "vsock transport reset before freeze" change.

One cycle is:

    create sandbox -> establish N host->guest exec connections -> pause
    -> read the snapshot's VSOCK state -> resume -> inspect the guest
    -> delete

The harness drives the E2B-compatible control plane
(POST /sandboxes, /pause, /connect, DELETE) and uses ``sandbox-ctl exec`` for
guest-side inspection. Node-side timing comes from the journal: the
KUASAR_PERF marks emitted when ``KUASAR_PERF=1`` is set on the units.

Marks consumed per cycle (missing marks are reported as "-", never as 0):

    snap.vsock_reset                          pause, duration (T)
    snap.vsock_reset_result                    pause, KV published/acked/timeout
    restore.dial.first_try                     resume, duration
    restore.dial                               resume, duration
    restore.dial_attempts                      resume, count
    restore.guest_ack                          resume, duration
    restore.snapshot_state                     resume, KV transport_reset_pending
                                               and local_port_last

Correctness assertions per cycle
  * snapshot state ``transport_reset_pending`` matches the arm expectation:
    off -> true (0006 staging/gate), on -> false. On the "on" arm a pending
    snapshot is counted as a fallback (published but not ACKed), which the
    design explicitly allows, and is reported separately.
  * no stale guest vsock row after resume. The predicate is exactly the one in
    design v3 section 5.3: a row in guest /proc/net/vsock whose local port is
    5000, peer CID is 2, peer port is in [0x40000000, local_port_last] and
    state is 01/02/03 is stale. CLOSE/CLOSE_WAIT rows (04/07/08/09) are not.
    ``local_port_last`` is read from the snapshot's state.json (the restored
    run dir's snap-state/state.json or the committed Snapshot S) and, when that
    carrier is unreadable, from the restore.snapshot_state mark, which is
    emitted from the same state.json bytes. The row created by the inspection
    exec itself is excluded (it is the highest host port at inspection time).
  * first-try dial success (restore.dial_attempts == 1 and no
    restore.dial_failed).
  * the restore did not exceed --restore-deadline-ms.

Guest inspection uses the flag set of this deployment (verified against
sandboxer/cmd/sandbox-ctl/exec.go; exec has no --base-root):

    sandbox-ctl exec --sandbox-id SID --run-root /run/sandbox/sandboxes \
        -- /bin/cat /proc/net/vsock

Output, under --out (a per-run directory):
    cycles.jsonl     one record per cycle, including assertion results
    summary.md       per-arm stats, quality counters and paired A/B (ABBA)
    journal.jsonl    the journal window captured for this run (unless
                     --journal-file was supplied)
    run.json         run parameters

Examples
  # negative control, same binary: off arm then on arm, 12 cycles each side
  python3 test/perf/restore_stress.py --arms off --cycles 12 --connections 2 \
      --template "$E2B_TEMPLATE_ID" --out /tmp/stress-off
  python3 test/perf/restore_stress.py --arms on --cycles 12 --connections 2 \
      --template "$E2B_TEMPLATE_ID" --out /tmp/stress-on

  # paired ABBA in one run; needs a per-cycle switch command (see README)
  python3 test/perf/restore_stress.py --arms off,on --cycles 16 \
      --arm-switch-cmd 'sudo /usr/local/bin/set-vsock-reset {arm}' --out /tmp/abba

The script only measures; it does not build, deploy, flip configuration or
commit anything. An arm switch that needs a runner-pool restart is the
operator's responsibility (--arm-switch-cmd exists for deployments that expose
a helper).
"""
import argparse
import glob
import json
import math
import os
import random
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HOST_CID = 2
GUEST_CONTROL_PORT = 5000
HOST_PORT_BASE = 0x40000000
STALE_STATES = {"01", "02", "03"}
MARK_SLACK_MS = 2000.0


# --------------------------------------------------------------------- helpers

def now_ms():
    return time.time() * 1000.0


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    k = int(math.ceil(len(ordered) * p / 100.0)) - 1
    return ordered[max(0, min(k, len(ordered) - 1))]


def fmt(value, width=9):
    return f"{'-':>{width}}" if value is None else f"{value:>{width}.2f}"


def as_bool01(value):
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "yes", "on", "1"):
            return 1
        if text in ("false", "no", "off", "0"):
            return 0
    return None


def bootstrap_ci(values, iterations=2000, seed=12345, alpha=0.05):
    """Bootstrap 95% CI of the median, fixed seed so reports are reproducible."""
    if len(values) < 3:
        return None
    rnd = random.Random(seed)
    n = len(values)
    medians = []
    for _ in range(iterations):
        medians.append(statistics.median(values[rnd.randrange(n)] for _ in range(n)))
    medians.sort()
    lo = medians[max(0, int(iterations * alpha / 2) - 1)]
    hi = medians[min(iterations - 1, int(iterations * (1 - alpha / 2)))]
    return lo, hi


# ------------------------------------------------------------------------- API

class Api:
    def __init__(self, base, key, timeout):
        self.base = base.rstrip("/")
        self.key = key
        self.timeout = timeout

    def call(self, method, path, body=None, timeout=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method)
        # kuasar uses X-API-KEY; CubeSandbox accepts Bearer. Send both, as the
        # stage benchmark does.
        req.add_header("X-API-KEY", self.key)
        req.add_header("Authorization", "Bearer " + self.key)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw.strip() else None
        except urllib.error.HTTPError as exc:
            detail = exc.read()[:300].decode("utf-8", "replace")
            raise RuntimeError("%s %s: HTTP %d %s" % (method, path, exc.code, detail)) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError("%s %s: %s" % (method, path, exc.reason)) from exc

    def create(self, template, timeout):
        body = {"templateID": template, "timeout": timeout}
        result = self.call("POST", "/sandboxes", body, timeout=max(120, timeout))
        sid = (result or {}).get("sandboxID") or (result or {}).get("sandbox_id")
        if not sid:
            raise RuntimeError("create returned no sandboxID")
        return sid

    def wait_state(self, sid, want, timeout):
        end = time.monotonic() + timeout
        last = ""
        while time.monotonic() < end:
            sandbox = self.call("GET", "/sandboxes/" + sid)
            if isinstance(sandbox, dict) and "state" not in sandbox:
                # Server does not expose lifecycle state; a returned object is
                # treated as ready (same rule as e2b_stage_bench.py).
                return
            last = str((sandbox or {}).get("state") or "").lower()
            if last == want:
                return
            time.sleep(0.05)
        raise TimeoutError("sandbox %s did not reach %r (last=%r)" % (sid, want, last))

    def pause(self, sid, memory, timeout):
        self.call("POST", "/sandboxes/%s/pause" % sid, {"memory": memory},
                  timeout=timeout)
        self.wait_state(sid, "paused", timeout)

    def resume(self, sid, timeout):
        self.call("POST", "/sandboxes/%s/connect" % sid, {"timeout": timeout},
                  timeout=timeout)
        self.wait_state(sid, "running", timeout)

    def delete(self, sid):
        self.call("DELETE", "/sandboxes/" + sid, timeout=120)


# --------------------------------------------------------------------- journal

def _decode_message(rec):
    msg = rec.get("MESSAGE")
    if isinstance(msg, bytes):
        msg = msg.decode("utf-8", "replace")
    return msg if isinstance(msg, str) else ""


def _perf_payload(text):
    pos = text.find("KUASAR_PERF ")
    if pos < 0:
        return None
    try:
        payload, _ = json.JSONDecoder().raw_decode(text[pos + len("KUASAR_PERF "):])
        return payload if isinstance(payload, dict) else None
    except json.JSONDecodeError:
        return None


def load_journal(path):
    """Parse `journalctl -o json`; tolerate plain short-iso lines too."""
    marks = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            payload = None
            ts_ms = None
            if raw.startswith("{"):
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    rec = None
                if rec is not None:
                    msg = _decode_message(rec)
                    ts = rec.get("__REALTIME_TIMESTAMP")
                    if ts:
                        try:
                            ts_ms = int(ts) / 1000.0
                        except (TypeError, ValueError):
                            ts_ms = None
                    if msg.startswith("KUASAR_PERF "):
                        payload = _perf_payload(msg)
            else:
                # `journalctl -o short-iso-precise` / application stderr
                payload = _perf_payload(raw)
            if payload is None:
                continue
            if ts_ms is not None and not payload.get("wall_ns"):
                payload["wall_ns"] = int(ts_ms * 1e6)
            payload["_ts_ms"] = ts_ms
            marks.append(payload)
    return marks


def group_marks(marks):
    sets = {}
    for mark in marks:
        rid = mark.get("rid")
        if not rid:
            continue
        # rid restarts in every sandbox-ctl child, so pid is part of the key.
        key = (rid, mark.get("pid"))
        item = sets.setdefault(key, {
            "comp": mark.get("comp"), "op": mark.get("op"),
            "sid": mark.get("sid"), "events": defaultdict(list),
        })
        if mark.get("sid"):
            item["sid"] = mark["sid"]
        item["events"][mark.get("evt")].append(mark)
    for item in sets.values():
        walls = [m.get("wall_ns") or 0 for recs in item["events"].values() for m in recs]
        item["first_wall_ns"] = min(walls) if walls else 0
        item["last_wall_ns"] = max(walls) if walls else 0
    return sets


def interval_value(mset, evt):
    recs = mset["events"].get(evt) or []
    for rec in recs:
        if rec.get("kind") == "interval" and isinstance(rec.get("off_ms"), (int, float)):
            return float(rec["off_ms"])
    offsets = [float(r["off_ms"]) for r in recs
               if r.get("kind") == "cumulative" and isinstance(r.get("off_ms"), (int, float))]
    if len(offsets) >= 2:
        return offsets[-1] - offsets[0]
    return None


def kv_value(recs, *names):
    for rec in recs:
        for name in names:
            if name in rec:
                return rec[name]
    return None


def attempt_count(recs):
    values = []
    for rec in recs:
        for name in ("attempts", "attempt", "count", "n", "value"):
            value = rec.get(name)
            if isinstance(value, (int, float)):
                values.append(int(value))
    if values:
        return max(values)
    return len(recs) if recs else None


class JournalQuery:
    """Attribute mark sets to one cycle by sid, falling back to its time window.

    restore.dial* marks currently come from a guestlink mark set without a sid
    (design v3 section 2.5 asks for that to be fixed). At C>1 several untagged
    sets can overlap the same window; the query then reports ``ambiguous``
    instead of borrowing another sandbox's number.
    """

    def __init__(self, sets):
        self.sets = list(sets)

    @staticmethod
    def _overlaps(mset, t0, t1, slack=MARK_SLACK_MS):
        lo = (t0 - slack) * 1e6
        hi = (t1 + slack) * 1e6
        if not mset["first_wall_ns"]:
            return False
        return mset["last_wall_ns"] >= lo and mset["first_wall_ns"] <= hi

    def _select(self, sid, t0, t1, evt, selectors):
        ambiguous = []
        for comp, op in selectors:
            candidates = []
            for mset in self.sets:
                if (mset.get("comp"), mset.get("op")) != (comp, op):
                    continue
                if not mset["events"].get(evt):
                    continue
                if not self._overlaps(mset, t0, t1):
                    continue
                if mset.get("sid") and sid and mset["sid"] != sid:
                    continue
                candidates.append(mset)
            if not candidates:
                continue
            tagged = [m for m in candidates if sid and m.get("sid") == sid]
            if tagged:
                candidates = tagged
            if len(candidates) == 1:
                mset = candidates[0]
                return mset, {
                    "source": "sid" if mset.get("sid") else "window",
                    "candidates": 1, "ambiguous": False,
                }
            ambiguous.append((comp, op, len(candidates)))
        if ambiguous:
            return None, {"source": "ambiguous", "ambiguous": True,
                          "candidates": sum(item[2] for item in ambiguous),
                          "detail": ambiguous}
        return None, {"source": "missing", "ambiguous": False, "candidates": 0}

    def interval(self, sid, t0, t1, evt, selectors):
        mset, meta = self._select(sid, t0, t1, evt, selectors)
        if mset is None:
            return None, meta
        return interval_value(mset, evt), meta

    def records(self, sid, t0, t1, evt, selectors):
        mset, meta = self._select(sid, t0, t1, evt, selectors)
        if mset is None:
            return [], meta
        return list(mset["events"].get(evt) or []), meta

    def cumulative(self, sid, t0, t1, evt, selectors):
        """Cumulative offset (ms) of a mark inside its own mark set."""
        mset, meta = self._select(sid, t0, t1, evt, selectors)
        if mset is None:
            return None, meta
        recs = mset["events"].get(evt) or []
        for rec in recs:
            if isinstance(rec.get("off_ms"), (int, float)):
                return float(rec["off_ms"]), meta
        return None, meta


# ----------------------------------------------------------- snapshot state

class StateUnavailable(RuntimeError):
    pass


def _resolved(path):
    try:
        return str(Path(path).resolve())
    except OSError:
        return str(path)


def _mtime(path):
    try:
        return Path(path).stat().st_mtime
    except OSError:
        return 0.0


def state_candidates(args, sid):
    """Paths that may carry the snapshot's state.json for this sandbox.

    The restored run directory holds the exact state.json bytes CH restored
    (`<run-root>/<sid>/snap-state/state.json`), so it is tried first. After a
    pause (before resume) that file does not exist yet, and the committed
    checkpoint Snapshot S is used instead.
    """
    items = []
    for raw in args.state_json or []:
        expanded = raw.replace("{sid}", sid)
        if any(ch in expanded for ch in "*?["):
            matches = glob.glob(expanded)
            matches.sort(key=_mtime, reverse=True)
            items.extend(matches)
        else:
            items.append(expanded)
    if args.run_root:
        items.append(str(Path(args.run_root) / sid / "snap-state" / "state.json"))
    if args.base_root:
        for checkpoint in (Path(args.base_root) / "sandboxes" / sid / "checkpoint",
                           Path(args.base_root) / sid / "checkpoint"):
            if not checkpoint.is_dir():
                continue
            items.append(str(checkpoint / "state.json"))
            for alias_name in (sid + ".snapshot", sid + ".bundle"):
                alias = checkpoint / alias_name
                if alias.exists():
                    items.append(str(alias))
            snaps = []
            for pattern in ("*.snapshot", "*.bundle"):
                try:
                    snaps.extend(checkpoint.glob(pattern))
                except OSError:
                    pass
            snaps.sort(key=_mtime, reverse=True)
            items.extend(str(p) for p in snaps[:3])
    seen = set()
    unique = []
    for item in items:
        key = _resolved(item)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def extract_state_json(path):
    """Return (state.json bytes, source label).

    A pause writes a Snapshot S whose layout is [sparse memory][ZIP]. The ZIP
    tail is appended, which Python's zipfile reads (it computes the prefix
    offset from the central directory). Tarstream/Manifest carriers may put a
    trailer after the ZIP and may be encrypted; those fail closed with a clear
    message and the operator can pass --state-json explicitly.
    """
    p = Path(path)
    if p.is_dir():
        p = p / "state.json"
    if not p.exists():
        raise StateUnavailable("missing %s" % p)
    if p.suffix.lower() == ".json":
        return p.read_bytes(), str(p)
    try:
        with zipfile.ZipFile(p) as archive:
            return archive.read("state.json"), "%s#state.json" % p
    except KeyError:
        raise StateUnavailable("%s: no state.json entry in the snapshot ZIP" % p)
    except zipfile.BadZipFile as exc:
        raise StateUnavailable(
            "%s: no readable appended snapshot ZIP (%s); this carrier may be a "
            "tarstream/Manifest wrapper or encrypted. Pass --state-json with a "
            "plain state.json (for example from the CH snapshot staging dir)."
            % (p, exc))
    except OSError as exc:
        raise StateUnavailable("%s: %s" % (p, exc))


def parse_vsock_state(state_bytes):
    """Find the vsock device state anywhere in the CH state.json tree.

    CH double-encodes each device state as a JSON string, so a plain recursive
    walk that also decodes object-looking strings is used rather than depending
    on the exact "__vsock" key.
    """
    try:
        root = json.loads(state_bytes)
    except json.JSONDecodeError as exc:
        raise StateUnavailable("state.json is not valid JSON: %s" % exc)

    found = []

    def walk(node, path):
        if isinstance(node, dict):
            if isinstance(node.get("local_port_last"), (int, float)):
                found.append((path, node))
            for key, value in node.items():
                if isinstance(value, str) and value.lstrip().startswith("{") \
                        and len(value) <= 4 * 1024 * 1024:
                    try:
                        walk(json.loads(value), path + [key])
                    except json.JSONDecodeError:
                        pass
                else:
                    walk(value, path + [key])
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, path + [str(index)])

    walk(root, [])
    if not found:
        return None
    found.sort(key=lambda item: (0 if any("vsock" in part.lower() for part in item[0]) else 1,
                                 len(item[0])))
    path, node = found[0]
    pending = node.get("transport_reset_pending", node.get("pending"))
    return {
        "local_port_last": int(node["local_port_last"]),
        "transport_reset_pending": as_bool01(pending),
        "device_path": "/".join(path),
    }


def read_snapshot_state(args, sid, timeout=None):
    errors = []
    deadline = time.monotonic() + max(0.0, args.state_timeout if timeout is None else timeout)
    while True:
        for candidate in state_candidates(args, sid):
            try:
                data, source = extract_state_json(candidate)
                state = parse_vsock_state(data)
                if state is None:
                    errors.append("%s: no local_port_last/vsock state found" % source)
                    continue
                state["source"] = source
                return state
            except StateUnavailable as exc:
                errors.append(str(exc))
        if time.monotonic() >= deadline:
            break
        time.sleep(0.2)
    raise StateUnavailable("; ".join(dict.fromkeys(errors)) or
                           "no state.json candidate; pass --state-json or --base-root")


# -------------------------------------------------------- /proc/net/vsock

def parse_proc_vsock(text):
    """Parse guest /proc/net/vsock into dict rows.

    Kernel format (net/vmw_vsock/af_vsock.c) is:
      Num RefCount Protocol Flags Type St Inode Port Peer_Port Peer_CID
    Header names are used when present; a positional fallback keeps kernels
    with a slightly different header spelling working. Rows that do not parse
    are counted, never silently dropped.
    """
    rows = []
    bad = 0
    lines = [line for line in text.splitlines() if line.strip()]
    header = None
    if lines and not lines[0].split()[0].endswith(":"):
        header = [token.lower().replace("_", "") for token in lines[0].split()]

    name_map = {
        "num": "num", "refcount": "refcount", "protocol": "protocol",
        "flags": "flags", "type": "type", "st": "state",
        "inode": "inode", "port": "local_port", "peerport": "peer_port",
        "peercid": "peer_cid",
    }
    for line in lines[1:] if header else lines:
        tokens = line.split()
        try:
            if header:
                values = {}
                for index, name in enumerate(header):
                    if index >= len(tokens):
                        break
                    mapped = name_map.get(name)
                    if mapped:
                        values[mapped] = tokens[index]
                row = {
                    "num": values.get("num", tokens[0]),
                    "state": values.get("state"),
                    "local_port": int(values["local_port"]),
                    "peer_port": int(values["peer_port"]),
                    "peer_cid": int(values["peer_cid"]),
                }
            else:
                # tokens[0] is "addr:", then refcount, protocol, flags, type,
                # state, inode, port, peer_port, peer_cid.
                if len(tokens) < 10:
                    bad += 1
                    continue
                row = {
                    "num": tokens[0],
                    "state": tokens[5],
                    "local_port": int(tokens[7]),
                    "peer_port": int(tokens[8]),
                    "peer_cid": int(tokens[9]),
                }
        except (KeyError, IndexError, ValueError):
            bad += 1
            continue
        rows.append(row)
    return rows, bad


def stale_rows(rows, local_port_last):
    """Apply the design v3 stale predicate exactly.

    Returns (stale, inspection, host_rows). The inspection exec itself opens a
    host-initiated connection to guest port 5000; it is the highest host port
    visible at inspection time because allocation is monotonic, so that row is
    excluded. A row in CLOSE/CLOSE_WAIT (state 04/07/08/09 or later) is not
    stale even when it is inside the port range.
    """
    all_host = [row for row in rows
                if row["local_port"] == GUEST_CONTROL_PORT
                and row["peer_cid"] == HOST_CID]
    inspection = max(all_host, key=lambda row: row["peer_port"]) if all_host else None
    host_rows = [row for row in all_host
                 if HOST_PORT_BASE <= row["peer_port"] <= local_port_last]
    stale = [row for row in host_rows
             if row is not inspection and row.get("state") in STALE_STATES]
    return stale, inspection, host_rows


# ------------------------------------------------------------- sandbox-ctl

def ctl_exec(args, sid, argv, timeout=60, stdin=None):
    cmd = [args.sandbox_ctl, "exec", "--sandbox-id", sid,
           "--run-root", args.run_root]
    if stdin is not None:
        cmd.append("--stdin")
    cmd.extend(["--"])
    cmd.extend(argv)
    return subprocess.run(cmd, input=stdin, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True, timeout=timeout)


def inspect_guest_vsock(args, sid):
    proc = ctl_exec(args, sid, ["/bin/cat", "/proc/net/vsock"],
                    timeout=args.exec_timeout)
    if proc.returncode != 0:
        raise RuntimeError("sandbox-ctl exec /bin/cat /proc/net/vsock failed "
                           "(rc=%d): %s" % (proc.returncode,
                                            (proc.stderr or "").strip()[:300]))
    rows, bad = parse_proc_vsock(proc.stdout)
    return rows, bad, proc.stdout


class Connections:
    """Keep N host->guest exec sessions open across pause/resume.

    Each ``sandbox-ctl exec --stdin`` session is a host-initiated vsock
    connection to guest port 5000, i.e. exactly the row shape the stale
    predicate looks for. ``echo READY`` confirms the session is live before the
    pause; stdin stays open (cat blocks) until stop().
    """

    def __init__(self, args, sid):
        self.args = args
        self.sid = sid
        self.procs = []

    def start(self, count):
        ready = 0
        errors = []
        for index in range(count):
            cmd = [self.args.sandbox_ctl, "exec", "--sandbox-id", self.sid,
                   "--run-root", self.args.run_root, "--stdin", "--",
                   "/bin/sh", "-c", "echo READY; exec /bin/cat"]
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, text=True)
            self.procs.append(proc)
            line, finished = _read_line(proc.stdout, self.args.conn_timeout)
            if not finished or line.strip() != "READY":
                errors.append("connection %d did not become ready (line=%r)" %
                              (index, (line or "")[:80]))
                continue
            ready += 1
        if errors:
            self.stop()
            raise RuntimeError("; ".join(errors))
        return ready

    def stop(self):
        for proc in self.procs:
            try:
                if proc.stdin:
                    proc.stdin.close()
            except OSError:
                pass
            try:
                proc.terminate()
            except OSError:
                pass
        for proc in self.procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except OSError:
                    pass
        self.procs = []


def _read_line(stream, timeout):
    box = {}

    def reader():
        try:
            box["line"] = stream.readline()
        except Exception as exc:  # noqa: BLE001 - surfaced by the caller
            box["err"] = exc

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    thread.join(timeout)
    return box.get("line", ""), not thread.is_alive() or "line" in box


# ------------------------------------------------------------------- cycle

def switch_arm(args, arm, state):
    with state["lock"]:
        if state.get("arm") == arm:
            return 0.0
        started = time.monotonic()
        if args.arm_switch_cmd:
            command = args.arm_switch_cmd.replace("{arm}", arm)
            proc = subprocess.run(command, shell=True, text=True,
                                  capture_output=True)
            if proc.returncode != 0:
                raise RuntimeError("arm switch %r failed (rc=%d): %s" %
                                   (command, proc.returncode,
                                    (proc.stderr or proc.stdout or "").strip()[:300]))
            if args.switch_settle_secs:
                time.sleep(args.switch_settle_secs)
        elif not state.get("warned"):
            print("warning: --arm-switch-cmd not set; the host switch must already "
                  "be configured for each arm (see README). The run records the arm "
                  "label and verifies it from the marks.", file=sys.stderr, flush=True)
            state["warned"] = True
        state["arm"] = arm
        return (time.monotonic() - started) * 1000.0


def run_cycle(args, api, arm, index, pair_index, switch_state):
    rec = {
        "cycle": index,
        "pair_index": pair_index,
        "arm": arm,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "errors": [],
        "violations": [],
    }
    sid = ""
    connections = None
    try:
        rec["switch_ms"] = switch_arm(args, arm, switch_state)

        t = time.monotonic()
        rec["create_start_ms"] = now_ms()
        sid = api.create(args.template, args.timeout)
        rec["sid"] = sid
        api.wait_state(sid, "running", args.timeout)
        rec["create_end_ms"] = now_ms()
        rec["create_ms"] = (time.monotonic() - t) * 1000.0

        if args.connections:
            connections = Connections(args, sid)
            t = time.monotonic()
            rec["connections_ready"] = connections.start(args.connections)
            rec["connections_ms"] = (time.monotonic() - t) * 1000.0
        else:
            rec["connections_ready"] = 0
            rec["connections_ms"] = 0.0

        t = time.monotonic()
        rec["pause_start_ms"] = now_ms()
        api.pause(sid, args.memory, args.timeout)
        rec["pause_end_ms"] = now_ms()
        rec["pause_ms"] = (time.monotonic() - t) * 1000.0

        try:
            # Usually readable from the committed checkpoint right away; a
            # slow commit is retried after resume from the restored run dir.
            rec["snapshot_state"] = read_snapshot_state(args, sid,
                                                        timeout=args.pause_state_timeout)
        except StateUnavailable as exc:
            rec["state_error"] = str(exc)

        t = time.monotonic()
        rec["resume_start_ms"] = now_ms()
        api.resume(sid, args.timeout)
        rec["resume_end_ms"] = now_ms()
        rec["resume_ms"] = (time.monotonic() - t) * 1000.0

        if "snapshot_state" not in rec:
            try:
                # <run-root>/<sid>/snap-state/state.json is the exact state.json
                # CH restored, so it also carries the snapshot's cursor/pending.
                rec["snapshot_state"] = read_snapshot_state(args, sid)
                rec.pop("state_error", None)
            except StateUnavailable as exc:
                rec["state_error"] = str(exc)

        rows, bad, _raw = inspect_guest_vsock(args, sid)
        rec["vsock_rows"] = len(rows)
        rec["vsock_rows_data"] = rows
        rec["vsock_bad_rows"] = bad
        rec["exec_ok"] = True
    except Exception as exc:  # noqa: BLE001 - a failed cycle is data, not a crash
        rec["errors"].append("%s: %s" % (type(exc).__name__, exc))
    finally:
        if connections is not None:
            try:
                connections.stop()
            except Exception as exc:  # noqa: BLE001
                rec["errors"].append("connections.stop: %s" % exc)
        if sid and not args.keep:
            try:
                api.delete(sid)
            except Exception as exc:  # noqa: BLE001
                rec["errors"].append("delete: %s" % exc)
    return rec


# --------------------------------------------------------------- measurement

PAUSE_RESET = [("runtime", "pause"), ("runtime", "snapshot-take")]
RESUME_GUESTLINK = [("guestlink", "restore")]
RESUME_RUNTIME = [("runtime", "resume")]


def collect_marks(query, rec):
    sid = rec.get("sid")
    marks = {}
    attribution = {}
    if not sid or "pause_start_ms" not in rec or "resume_end_ms" not in rec:
        rec["marks"] = marks
        rec["attribution"] = attribution
        rec["mark_error"] = "cycle did not reach pause/resume"
        return

    pause_window = (rec["pause_start_ms"], rec["pause_end_ms"])
    resume_window = (rec["resume_start_ms"], rec.get("resume_end_ms", rec["resume_start_ms"]))

    value, meta = query.interval(sid, *pause_window, "snap.vsock_reset", PAUSE_RESET)
    marks["snap.vsock_reset"] = value
    attribution["snap.vsock_reset"] = meta

    recs, meta = query.records(sid, *pause_window, "snap.vsock_reset_result", PAUSE_RESET)
    attribution["snap.vsock_reset_result"] = meta
    if recs:
        marks["snap.vsock_reset_result"] = {
            "published": as_bool01(kv_value(recs, "published")),
            "acked": as_bool01(kv_value(recs, "acked")),
            "timeout": kv_value(recs, "timeout"),
        }

    value, meta = query.interval(sid, *resume_window, "restore.dial.first_try",
                                 RESUME_GUESTLINK)
    marks["restore.dial.first_try"] = value
    attribution["restore.dial.first_try"] = meta

    value, meta = query.interval(sid, *resume_window, "restore.dial", RESUME_GUESTLINK)
    marks["restore.dial"] = value
    attribution["restore.dial"] = meta

    recs, meta = query.records(sid, *resume_window, "restore.dial_attempts",
                               RESUME_GUESTLINK)
    marks["restore.dial_attempts"] = attempt_count(recs) if recs else None
    attribution["restore.dial_attempts"] = meta

    recs, meta = query.records(sid, *resume_window, "restore.dial_failed",
                               RESUME_GUESTLINK)
    failures = 0
    for item in recs:
        flag = as_bool01(item.get("failed", item.get("value")))
        if flag is None or flag:
            failures += 1
    marks["restore.dial_failed"] = failures if recs else 0
    attribution["restore.dial_failed"] = meta

    value, meta = query.interval(sid, *resume_window, "restore.guest_ack",
                                 RESUME_GUESTLINK)
    marks["restore.guest_ack"] = value
    attribution["restore.guest_ack"] = meta

    recs, meta = query.records(sid, *resume_window, "restore.snapshot_state",
                               RESUME_RUNTIME)
    attribution["restore.snapshot_state"] = meta
    if recs:
        pending = as_bool01(kv_value(recs, "transport_reset_pending", "pending"))
        cursor = kv_value(recs, "local_port_last")
        if isinstance(cursor, (int, float)):
            cursor = int(cursor)
        else:
            cursor = None
        marks["restore.snapshot_state"] = {
            "transport_reset_pending": pending,
            "local_port_last": cursor,
        }
        # The mark is emitted from the same state.json bytes that restore
        # wrote, so it is a valid fallback when the checkpoint carrier could
        # not be read (encrypted/tarstream, wrong root, slow commit).
        state = rec.setdefault("snapshot_state", {})
        if state.get("transport_reset_pending") is None and pending is not None:
            state["transport_reset_pending"] = pending
            state.setdefault("source", "restore.snapshot_state mark")
        if state.get("local_port_last") is None and cursor is not None:
            state["local_port_last"] = cursor
            state.setdefault("source", "restore.snapshot_state mark")

    begin, meta = query.cumulative(sid, *resume_window, "restore.begin", RESUME_RUNTIME)
    ready, _ = query.cumulative(sid, *resume_window, "restore.ready", RESUME_RUNTIME)
    attribution["runtime.total"] = meta
    marks["runtime.total"] = (ready - begin
                              if begin is not None and ready is not None else None)

    rec["marks"] = marks
    rec["attribution"] = attribution


def evaluate_assertions(args, rec):
    """Turn the collected marks/state into per-cycle assertions."""
    checks = {"pending_matches_arm": None, "stale_rows_zero": None,
              "first_try_ok": None, "restore_under_deadline": None}
    violations = rec.setdefault("violations", [])
    marks = rec.get("marks") or {}
    state = rec.get("snapshot_state") or {}
    arm = rec.get("arm")
    pending = state.get("transport_reset_pending")
    rec["state_verified"] = pending is not None

    if pending is not None:
        if arm == "off":
            checks["pending_matches_arm"] = pending == 1
            if pending != 1:
                violations.append(
                    "off arm snapshot has transport_reset_pending=%d; the reset "
                    "switch is probably still on (negative control invalid)" % pending)
        else:
            checks["pending_matches_arm"] = pending == 0
            if pending == 1:
                rec["fallback"] = True
                result = marks.get("snap.vsock_reset_result") or {}
                if result.get("acked") == 1:
                    violations.append(
                        "on arm: pending=1 but snap.vsock_reset_result acked=true")
        rec["fallback"] = bool(rec.get("fallback"))

    local_port_last = state.get("local_port_last")
    rows = rec.get("vsock_rows_data")
    if local_port_last is not None and rows is not None:
        stale, inspection, host_rows = stale_rows(rows, local_port_last)
        rec["stale"] = stale
        rec["stale_count"] = len(stale)
        rec["host_rows"] = len(host_rows)
        rec["inspection_row"] = inspection
        rec["inspection_port_above_cursor"] = (
            inspection is not None and inspection["peer_port"] > local_port_last)
        checks["stale_rows_zero"] = len(stale) == 0
        if stale:
            violations.append("stale guest vsock rows: %s" %
                              json.dumps(stale[:5], sort_keys=True))
        if inspection is None:
            violations.append(
                "no host-initiated row (guest port 5000, peer CID 2) found for the "
                "inspection exec; /proc/net/vsock format or the port assumption is "
                "wrong, so the inspection row cannot be excluded")
        elif not rec["inspection_port_above_cursor"]:
            violations.append(
                "inspection connection port %d is not above the snapshot cursor %d; "
                "the port cursor may have been reused" %
                (inspection["peer_port"], local_port_last))
    else:
        rec["stale"] = None
        rec["stale_count"] = None

    attempts = marks.get("restore.dial_attempts")
    first_try = marks.get("restore.dial.first_try")
    failures = marks.get("restore.dial_failed") or 0
    if attempts is not None:
        checks["first_try_ok"] = attempts == 1 and failures == 0
    elif first_try is not None and "restore.dial" in marks:
        checks["first_try_ok"] = failures == 0
    elif failures:
        checks["first_try_ok"] = False
    if checks["first_try_ok"] is False:
        violations.append("first-try dial failed (attempts=%s, dial_failed=%s, "
                          "first_try=%s)" % (attempts, failures, first_try))

    total = marks.get("runtime.total")
    if total is None:
        total = rec.get("resume_ms")
    if total is not None:
        checks["restore_under_deadline"] = total <= args.restore_deadline_ms
        if total > args.restore_deadline_ms:
            violations.append("restore %.1f ms exceeds deadline %.1f ms" %
                              (total, args.restore_deadline_ms))

    if args.dial_deadline_ms and marks.get("restore.dial") is not None:
        if marks["restore.dial"] > args.dial_deadline_ms:
            violations.append("restore.dial %.1f ms exceeds --dial-deadline-ms %.1f" %
                              (marks["restore.dial"], args.dial_deadline_ms))

    if args.require_state and pending is None:
        violations.append("snapshot state unverified: %s" %
                          rec.get("state_error", "missing restore.snapshot_state mark"))
    rec["assert"] = checks


# ------------------------------------------------------------------ schedule

def build_schedule(arms, cycles, order, concurrency=1):
    if len(arms) == 1:
        return [arms[0]] * cycles
    first, second = arms[0], arms[1]
    if order == "block":
        half = cycles // 2
        return [first] * half + [second] * (cycles - half)
    # ABBA per cycle at C=1. At C>1 apply the pattern to batches of C cycles so
    # the whole pool works on one arm at a time and the switch never flips under
    # an in-flight cycle.
    pattern = [first, second, second, first]
    repeat = max(1, concurrency)
    schedule = []
    while len(schedule) < cycles:
        for arm in pattern:
            schedule.extend([arm] * repeat)
    return schedule[:cycles]


# ------------------------------------------------------------------ summary

ARM_METRICS = [
    ("create", lambda r: r.get("create_ms")),
    ("pause", lambda r: r.get("pause_ms")),
    ("resume", lambda r: r.get("resume_ms")),
    ("snap.vsock_reset", lambda r: (r.get("marks") or {}).get("snap.vsock_reset")),
    ("restore.dial.first_try", lambda r: (r.get("marks") or {}).get("restore.dial.first_try")),
    ("restore.dial", lambda r: (r.get("marks") or {}).get("restore.dial")),
    ("restore.guest_ack", lambda r: (r.get("marks") or {}).get("restore.guest_ack")),
    ("runtime.total", lambda r: (r.get("marks") or {}).get("runtime.total")),
]


def arm_values(records, arm, getter):
    values = []
    for rec in records:
        if rec.get("arm") != arm:
            continue
        value = getter(rec)
        if isinstance(value, (int, float)):
            values.append(float(value))
    return values


def stats_row(values):
    return {
        "n": len(values),
        "median": statistics.median(values) if values else None,
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
    }


def write_summary(path, args, records, warnings):
    arms = list(dict.fromkeys(r.get("arm") for r in records))
    lines = ["# restore stress: vsock reset before freeze (design v3)", ""]
    lines.append("- api: `%s`" % args.api)
    lines.append("- template: `%s`" % args.template)
    lines.append("- arm schedule: `%s` (%s), %d cycles, concurrency %d, "
                 "%d connections/cycle" %
                 (",".join(arms), args.order, len(records), args.concurrency,
                  args.connections))
    lines.append("- arm switch: %s" % ("`%s`" % args.arm_switch_cmd
                                       if args.arm_switch_cmd
                                       else "external (not changed by this run)"))
    lines.append("- journal: `%s`%s" %
                 (args._journal_path or "-",
                  (" since `%s`" % args._journal_since) if args._journal_since else ""))
    lines.append("")

    if warnings:
        lines.append("## warnings")
        lines.append("")
        for warning in warnings:
            lines.append("- " + warning)
        lines.append("")

    lines.append("## per-arm durations (ms)")
    lines.append("")
    header = ("| metric | arm | n | median | p50 | p95 | p99 |")
    lines.append(header)
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for name, getter in ARM_METRICS:
        for arm in arms:
            row = stats_row(arm_values(records, arm, getter))
            lines.append("| %s | %s | %d | %s | %s | %s | %s |" %
                         (name, arm, row["n"], fmt(row["median"]),
                          fmt(row["p50"]), fmt(row["p95"]), fmt(row["p99"])))
    lines.append("")

    lines.append("## per-arm quality")
    lines.append("")
    lines.append("| arm | cycles | errors | violations | first-try fail | "
                 "stale cycles | stale rows | fallback | state unverified |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for arm in arms:
        subset = [r for r in records if r.get("arm") == arm]
        lines.append("| %s | %d | %d | %d | %d | %d | %d | %d | %d |" % (
            arm, len(subset),
            sum(1 for r in subset if r.get("errors")),
            sum(1 for r in subset if r.get("violations")),
            sum(1 for r in subset if (r.get("assert") or {}).get("first_try_ok") is False),
            sum(1 for r in subset if (r.get("stale_count") or 0) > 0),
            sum((r.get("stale_count") or 0) for r in subset),
            sum(1 for r in subset if r.get("fallback")),
            sum(1 for r in subset if not r.get("state_verified")),
        ))
    lines.append("")

    lines.append("| arm | snapshot pending=False | pending=True | reset published | "
                 "reset acked | dial attempts mean |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for arm in arms:
        subset = [r for r in records if r.get("arm") == arm]
        pending_false = pending_true = 0
        published = acked = 0
        attempts = []
        for rec in subset:
            state = rec.get("snapshot_state") or {}
            pending = state.get("transport_reset_pending")
            if pending == 0:
                pending_false += 1
            elif pending == 1:
                pending_true += 1
            result = (rec.get("marks") or {}).get("snap.vsock_reset_result") or {}
            if result.get("published") == 1:
                published += 1
            if result.get("acked") == 1:
                acked += 1
            value = (rec.get("marks") or {}).get("restore.dial_attempts")
            if isinstance(value, (int, float)):
                attempts.append(float(value))
        lines.append("| %s | %d | %d | %d | %d | %s |" % (
            arm, pending_false, pending_true, published, acked,
            fmt(statistics.mean(attempts)) if attempts else fmt(None)))
    lines.append("")

    # Paired A/B: pair the k-th cycle of one arm with the k-th cycle of the
    # other. For ABBA, that is exactly the adjacent (A,B) and (B,A) pairs.
    off = {r["pair_index"]: r for r in records if r.get("arm") == arms[0]} if arms else {}
    on_arm = arms[1] if len(arms) > 1 else None
    on = ({r["pair_index"]: r for r in records if r.get("arm") == on_arm}
          if on_arm else {})
    pairs = [(off[k], on[k]) for k in sorted(set(off) & set(on))]

    lines.append("## paired A/B (on - off)")
    lines.append("")
    if len(arms) < 2 or not pairs:
        lines.append("not enough paired cycles (need both arms with matching "
                     "pair indexes).")
        lines.append("")
    else:
        lines.append("| metric | pairs | off median | on median | median diff | "
                     "bootstrap 95% CI | change |")
        lines.append("|---|---:|---:|---:|---:|---|---:|")
        for name, getter in ARM_METRICS:
            diffs = []
            off_values = []
            on_values = []
            for off_rec, on_rec in pairs:
                a = getter(off_rec)
                b = getter(on_rec)
                if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
                    continue
                off_values.append(float(a))
                on_values.append(float(b))
                diffs.append(float(b) - float(a))
            if not diffs:
                continue
            ci = bootstrap_ci(diffs)
            off_median = statistics.median(off_values)
            change = ("%+.1f%%" % ((statistics.median(diffs) / off_median) * 100.0)
                      if off_median else "-")
            lines.append("| %s | %d | %s | %s | %+.2f | [%s, %s] | %s |" % (
                name, len(diffs), fmt(off_median), fmt(statistics.median(on_values)),
                statistics.median(diffs),
                ("%+.2f" % ci[0]) if ci else "-",
                ("%+.2f" % ci[1]) if ci else "-",
                change))
        lines.append("")
        lines.append("Pairs use the ABBA/block order above; diff < 0 means the "
                     "`%s` arm was faster. Bootstrap is the 95%% CI of the paired "
                     "median difference (2000 resamples, fixed seed)." % on_arm)
        lines.append("")

    lines.append("## negative control")
    lines.append("")
    if len(arms) < 2:
        lines.append("Only one arm was run. The design requires both arms on the "
                     "same binary: off must reproduce pending=true and a slow "
                     "dial, on must show pending=false and a 1-3 ms dial.")
    else:
        off_recs = [r for r in records if r.get("arm") == arms[0]]
        on_recs = [r for r in records if r.get("arm") == on_arm]
        off_pending = sum(1 for r in off_recs
                          if (r.get("snapshot_state") or {}).get(
                              "transport_reset_pending") == 1)
        on_pending = sum(1 for r in on_recs
                         if (r.get("snapshot_state") or {}).get(
                             "transport_reset_pending") == 0)
        off_dial = arm_values(off_recs, arms[0],
                              lambda r: (r.get("marks") or {}).get("restore.dial"))
        on_dial = arm_values(on_recs, on_arm,
                             lambda r: (r.get("marks") or {}).get("restore.dial"))
        lines.append("- off: pending=true in %d/%d cycles; dial median %s ms" %
                     (off_pending, len(off_recs),
                      ("%.2f" % statistics.median(off_dial)) if off_dial else "-"))
        lines.append("- on: pending=false in %d/%d cycles; dial median %s ms" %
                     (on_pending, len(on_recs),
                      ("%.2f" % statistics.median(on_dial)) if on_dial else "-"))
        if off_dial and statistics.median(off_dial) < 10.0:
            lines.append("- **negative control may be invalid**: the off arm's "
                         "dial is already below 10 ms (design 5.1 expects the "
                         "60-70 ms gate path).")
        if off_pending == 0 and off_recs:
            lines.append("- **negative control may be invalid**: no off-arm "
                         "snapshot reported pending=true.")
    lines.append("")

    lines.append("## assumptions / caveats")
    lines.append("")
    lines.append("- `restore.dial*` marks are sid-tagged by current builds; when a "
                 "mark set has no `sid`, attribution falls back to its time window "
                 "and concurrency > 1 is reported as ambiguous instead of borrowing "
                 "another sandbox's number.")
    lines.append("- `/proc/net/vsock` columns are parsed by header name, with a "
                 "10-column positional fallback; a kernel whose layout differs "
                 "will show up as `vsock_bad_rows` and an unverified stale check.")
    lines.append("- `local_port_last` comes from the restored snap-state/state.json "
                 "or the checkpoint Snapshot S (appended ZIP tail), with the "
                 "`restore.snapshot_state` mark as fallback. A build without that "
                 "mark and a non-plaintext carrier need an explicit `--state-json`.")
    lines.append("- the inspection exec is excluded by choosing the highest host "
                 "port at inspection time; port allocation must be monotonic "
                 "(0005). A lower-than-cursor inspection port is reported as a "
                 "violation.")
    lines.append("- guest exec flags follow this deployment: `--run-root "
                 "/run/sandbox/sandboxes`, no `--base-root` for exec.")
    lines.append("")

    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


# --------------------------------------------------------------------- main

def capture_journal(args, since):
    path = Path(args.out) / "journal.jsonl"
    cmd = ["journalctl", "-o", "json", "--no-pager", "--since", since]
    if args.journal_until:
        cmd.extend(["--until", args.journal_until])
    for unit in args.journal_unit:
        cmd.extend(["-u", unit])
    with open(path, "w", encoding="utf-8") as handle:
        try:
            proc = subprocess.run(cmd, stdout=handle, stderr=subprocess.PIPE, text=True)
        except FileNotFoundError as exc:
            raise SystemExit("journalctl not found (%s); pass --journal-file with "
                             "an existing capture" % exc)
    if proc.returncode != 0:
        raise RuntimeError("journalctl failed (rc=%d): %s" %
                           (proc.returncode, (proc.stderr or "").strip()[:300]))
    return path


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--arms", default="off,on",
                        help="comma list, e.g. off,on (first arm is the control)")
    parser.add_argument("--cycles", type=int, default=8,
                        help="total cycles; ABBA needs cycles divisible by 4")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--connections", type=int, default=1,
                        help="host->guest exec sessions held open per cycle")
    parser.add_argument("--api", default=os.environ.get("E2B_API_URL", "http://127.0.0.1:3000"))
    parser.add_argument("--key", default=os.environ.get("E2B_API_KEY", ""))
    parser.add_argument("--template", default=os.environ.get("E2B_TEMPLATE_ID", ""))
    parser.add_argument("--journal-since", default="",
                        help="journalctl --since value; default is this run's start")
    parser.add_argument("--journal-until", default="")
    parser.add_argument("--journal-unit", action="append", default=[],
                        help="optional journalctl -u filter (repeatable)")
    parser.add_argument("--journal-file", default="",
                        help="use an existing `journalctl -o json` capture instead of running journalctl")
    parser.add_argument("--out", default="", help="output directory (default: test/results/restore-stress-<ts>)")
    parser.add_argument("--append", action="store_true",
                        help="append to an existing cycles.jsonl in --out")
    parser.add_argument("--order", choices=("abba", "block"), default="abba",
                        help="abba interleaves cycles; block runs one arm at a time")
    parser.add_argument("--arm-switch-cmd", default="",
                        help="command template run when the arm changes; {arm} is replaced")
    parser.add_argument("--switch-settle-secs", type=float, default=0.0)
    parser.add_argument("--sandbox-ctl", default=os.environ.get("SANDBOX_CTL", "sandbox-ctl"))
    parser.add_argument("--run-root", default="/run/sandbox/sandboxes",
                        help="tmpfs run root used by sandbox-ctl exec in this deployment")
    parser.add_argument("--base-root", default="/var/lib/sandbox",
                        help="persistent root used to find the paused sandbox checkpoint")
    parser.add_argument("--state-json", action="append", default=[],
                        help="explicit state.json / snapshot path or glob ({sid} expands); repeatable")
    parser.add_argument("--state-timeout", type=float, default=5.0,
                        help="seconds to wait for state.json after resume")
    parser.add_argument("--pause-state-timeout", type=float, default=1.0,
                        help="seconds to try the committed checkpoint state.json "
                             "after pause before falling back to the restored run dir")
    parser.add_argument("--timeout", type=int, default=600, help="sandbox timeout / wait budget (s)")
    parser.add_argument("--exec-timeout", type=float, default=60.0)
    parser.add_argument("--conn-timeout", type=float, default=30.0)
    parser.add_argument("--restore-deadline-ms", type=float, default=10000.0)
    parser.add_argument("--dial-deadline-ms", type=float, default=0.0,
                        help="optional hard per-cycle dial deadline (0 disables)")
    parser.add_argument("--require-state", action="store_true",
                        help="treat an unreadable snapshot state as a violation")
    parser.add_argument("--soft", action="store_true",
                        help="always exit 0 (violations are only reported)")
    parser.add_argument("--keep", action="store_true", help="keep sandboxes after each cycle")
    parser.add_argument("--memory", type=lambda v: v.lower() != "false", default=True)
    parser.add_argument("--ops-out", default="",
                        help="optional per-operation JSONL for stage_tree.py")
    args = parser.parse_args()

    arms = [arm.strip() for arm in args.arms.split(",") if arm.strip()]
    if not arms:
        parser.error("--arms is empty")
    if args.cycles < 1:
        parser.error("--cycles must be >= 1")
    if len(arms) > 2:
        parser.error("--arms supports at most two arms (for paired A/B)")
    if not args.template:
        parser.error("no template: pass --template or set E2B_TEMPLATE_ID")
    if not args.key:
        parser.error("no API key: pass --key or set E2B_API_KEY")

    args.out = args.out or str(Path(__file__).resolve().parent / ".." / "results" /
                               ("restore-stress-" + time.strftime("%Y%m%d-%H%M%S")))
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    cycles_path = out_dir / "cycles.jsonl"
    if cycles_path.exists() and cycles_path.stat().st_size > 0 and not args.append:
        parser.error("%s already exists; use a new --out or pass --append" % cycles_path)

    args._journal_since = args.journal_since or ("@%d" % int(time.time()))
    args._journal_path = ""

    schedule = build_schedule(arms, args.cycles, args.order, args.concurrency)
    api = Api(args.api, args.key, args.timeout)

    print("# restore_stress api=%s template=%s arms=%s cycles=%d c=%d conns=%d order=%s"
          % (args.api, args.template, ",".join(arms), args.cycles,
             args.concurrency, args.connections, args.order), flush=True)
    print("# journal since %s; out %s" % (args._journal_since, out_dir), flush=True)

    records = []
    pair_counters = defaultdict(int)
    items = []
    for index, arm in enumerate(schedule):
        items.append((index, arm, pair_counters[arm]))
        pair_counters[arm] += 1
    switch_state = {"arm": None, "lock": threading.Lock()}
    started = time.time()

    def one(index, arm, pair_index):
        return run_cycle(args, api, arm, index, pair_index, switch_state)

    # Run consecutive cycles of the same arm as one batch. An arm switch (which
    # may restart a runner pool) must never happen while another cycle of the
    # previous arm is still in flight, and ABBA at C>1 must not flip the switch
    # back under a concurrent cycle.
    batches = []
    for item in items:
        if batches and batches[-1][0] == item[1]:
            batches[-1][1].append(item)
        else:
            batches.append((item[1], [item]))

    for arm, batch in batches:
        if args.concurrency <= 1:
            for index, _arm, pair_index in batch:
                record = one(index, arm, pair_index)
                records.append(record)
                print("cycle %d arm=%s sid=%s create=%s pause=%s resume=%s%s" % (
                    index, arm, record.get("sid", "-"),
                    fmt(record.get("create_ms"), 7), fmt(record.get("pause_ms"), 7),
                    fmt(record.get("resume_ms"), 7),
                    (" ERR " + record["errors"][0]) if record.get("errors") else ""),
                    flush=True)
            continue
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = {pool.submit(one, index, arm, pair_index): (index, arm)
                       for index, _arm, pair_index in batch}
            for future in as_completed(futures):
                index, future_arm = futures[future]
                try:
                    record = future.result()
                except Exception as exc:  # noqa: BLE001
                    record = {"cycle": index, "arm": future_arm, "errors": [
                        "%s: %s" % (type(exc).__name__, exc)], "violations": []}
                records.append(record)
                print("cycle %d arm=%s sid=%s%s" % (
                    index, future_arm, record.get("sid", "-"),
                    (" ERR " + record["errors"][0]) if record.get("errors") else ""),
                    flush=True)

    records.sort(key=lambda r: r.get("cycle", 0))

    # Node-side marks are only complete after the last cycle; capture the whole
    # run window and attribute per cycle (sid first, time window fallback).
    if args.journal_file:
        journal_path = Path(args.journal_file)
        if not journal_path.exists():
            raise SystemExit("journal file not found: %s" % journal_path)
        args._journal_path = str(journal_path)
        args._journal_since = ""
    else:
        journal_path = capture_journal(args, args._journal_since)
        args._journal_path = str(journal_path)

    query = JournalQuery(group_marks(load_journal(journal_path)).values())
    for record in records:
        collect_marks(query, record)
        evaluate_assertions(args, record)

    with open(cycles_path, "a" if args.append else "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    if args.ops_out:
        op_path = Path(args.ops_out)
        op_path.parent.mkdir(parents=True, exist_ok=True)
        with open(op_path, "a", encoding="utf-8") as handle:
            for record in records:
                if not record.get("sid"):
                    continue
                handle.write(json.dumps({
                    "op": "pause",
                    "sid": record["sid"],
                    "c": args.concurrency,
                    "start_ms": record.get("pause_start_ms"),
                    "end_ms": record.get("pause_end_ms"),
                    "dur_ms": record.get("pause_ms"),
                    "ok": bool(record.get("pause_end_ms")),
                    "attempt": 1,
                    "arm": record.get("arm"),
                }) + "\n")
                handle.write(json.dumps({
                    "op": "resume",
                    "sid": record["sid"],
                    "c": args.concurrency,
                    "start_ms": record.get("resume_start_ms"),
                    "end_ms": record.get("resume_end_ms"),
                    "dur_ms": record.get("resume_ms"),
                    "ok": bool(record.get("resume_end_ms")),
                    "attempt": 1,
                    "arm": record.get("arm"),
                }) + "\n")

    warnings = []
    ambiguous = [r for r in records
                 if any(meta.get("ambiguous") for meta in (r.get("attribution") or {}).values())]
    if ambiguous:
        warnings.append("%d/%d cycles have ambiguous mark attribution (untagged "
                        "restore.dial* sets at concurrency > 1); those metrics are "
                        "reported as missing, not guessed." % (len(ambiguous), len(records)))
    unverified = [r for r in records if not r.get("state_verified")]
    if unverified:
        warnings.append("%d/%d cycles have no transport_reset_pending value; the "
                        "arm assertion is unverified for them (see state_error)." %
                        (len(unverified), len(records)))
    stale_unverified = [r for r in records
                        if r.get("exec_ok") and r.get("stale_count") is None]
    if stale_unverified:
        warnings.append("%d/%d cycles have no usable local_port_last; the stale "
                        "row check is unverified there (state and the "
                        "restore.snapshot_state mark both lacked a cursor)." %
                        (len(stale_unverified), len(records)))
    errored = [r for r in records if r.get("errors")]
    if errored:
        warnings.append("%d/%d cycles had infrastructure errors; they are "
                        "counted, not dropped." % (len(errored), len(records)))
    bad_rows = [r for r in records if (r.get("vsock_bad_rows") or 0) > 0]
    if bad_rows:
        warnings.append("%d/%d cycles had unparsable /proc/net/vsock rows; "
                        "the stale check may undercount (header/column layout "
                        "changed)." % (len(bad_rows), len(records)))
    if len(arms) == 2:
        per_arm = min(sum(1 for r in records if r.get("arm") == arm) for arm in arms)
        if per_arm < 30:
            warnings.append("only %d cycles/arm; design 5.2 asks for >=30 paired "
                            "rounds and abandons n=9 p95." % per_arm)

    write_summary(out_dir / "summary.md", args, records, warnings)
    (out_dir / "run.json").write_text(json.dumps({
        "argv": sys.argv,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(started)),
        "finished": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "api": args.api,
        "template": args.template,
        "arms": arms,
        "cycles": args.cycles,
        "concurrency": args.concurrency,
        "connections": args.connections,
        "order": args.order,
        "journal_since": args._journal_since,
        "journal_path": args._journal_path,
        "arm_switch_cmd": args.arm_switch_cmd,
    }, indent=1, sort_keys=True) + "\n", encoding="utf-8")

    violations = sum(len(r.get("violations") or []) for r in records)
    hard_errors = sum(1 for r in records if r.get("errors"))
    print("\n%d cycles, %d violation(s), %d cycle error(s); wrote %s and %s"
          % (len(records), violations, hard_errors, cycles_path,
             out_dir / "summary.md"), flush=True)
    if (violations or hard_errors) and not args.soft:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
