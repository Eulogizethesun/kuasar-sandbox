#!/usr/bin/env python3
"""stage_tree.py -- decompose kuasar-sandbox create / pause / resume into a
nested stage tree, in the same shape as the CubeSandbox pauseprof issue.

It joins two inputs:

  --ops      per-operation client records (epoch ms) written by
             e2b_stage_bench.py --ops-out
  --journal  `journalctl -o json` output covering the same window. Only the
             MESSAGE field is used: KUASAR_PERF marks emitted by the
             instrumentation, plus the timing lines the components already
             print (sandbox launch phase, capture_ms, handshake_ms, ...).

Rules (same as CubeSandbox's method):
  * a cumulative mark's offset is relative to its own mark set; two adjacent
    marks delimit an interval, cumulative offsets are never added together;
  * interval marks ("kind":"interval") are already durations;
  * marks from different processes are aligned by wall clock but every number
    reported is a duration, so clock offsets cancel;
  * a stage with no observation is printed as "-", never as 0.

Usage:
  journalctl -o json --no-pager --since "-10 min" > /tmp/journal.json
  python3 test/perf/stage_tree.py --ops ops.jsonl --journal /tmp/journal.json
  python3 test/perf/stage_tree.py ... --json out.json --md out.md
"""
import argparse
import json
import math
import statistics
import sys
from collections import defaultdict

# ---------------------------------------------------------------- references

# CubeSandbox v0.7.1 + PR stack + dirtylog, C1, n=9 (issue #24 §二). Only
# stages with a structural counterpart are listed; see COMPARE_NOTE.
CUBE_REF = {
    # pause
    "cube.pause.client": 36.2,
    "cube.pause.outer": 11.3,
    "cube.pause.request": 24.9,
    "cube.pause.shim_total": 14.2,
    "cube.pause.disconnect_agent": 0.02,
    "cube.pause.vmm_snapshot": 14.1,
    "cube.pause.commit_rootfs": 0.73,
    "cube.pause.write_spec": 0.13,
    "cube.pause.mark_paused": 0.16,
    "cube.pause.destroy_live": 7.1,
    # resume
    "cube.resume.client": 92.8,
    "cube.resume.outer": 6.1,
    "cube.resume.request": 86.7,
    "cube.resume.start": 44.7,
    "cube.resume.probe": 38.6,
    "cube.resume.shim_create": 39.2,
    "cube.resume.launch_vmm": 1.5,
    "cube.resume.restore_vm": 11.1,
    "cube.resume.reset_vm": 25.7,
    "cube.resume.create_sandbox": 0.5,
}

COMPARE_NOTE = """\
CubeSandbox 的数值取自 issue #24 §二（v0.7.1 + PR 栈 + dirtylog，C1，n=9），
kuasar 的数值是本次实测（2C2G，python:3.12-slim 模板）。两边的阶段是**按架构角色**
对齐的，不是同一段代码，绝对值只能看量级；`① 客户端` 与 `② 请求窗口` 的差值是各自
的 HTTP/调度/轮询开销。"""

# ------------------------------------------------------------------ utilities


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = int(math.ceil(len(s) * p / 100.0)) - 1
    return s[max(0, min(k, len(s) - 1))]


def mean(values):
    return statistics.mean(values) if values else None


def fmt(v, width=8):
    return f"{'-':>{width}}" if v is None else f"{v:>{width}.2f}"


def dur_units(text, unit):
    v = float(text)
    if unit == "us" or unit == "\u00b5s" or unit == "µs":
        return v / 1000.0
    if unit == "ns":
        return v / 1e6
    if unit == "s":
        return v * 1000.0
    return v


# ------------------------------------------------------------------- journal

import re

RE_PHASE = re.compile(
    r'msg="sandbox launch phase".*?sid=(\S+).*?kind=(\w+).*?phase=(\w+) duration=([0-9.]+)(ms|\u00b5s|\u03bcs|ns|s)')
RE_TERMINAL = re.compile(
    r'msg="sandbox launch terminal".*?sid=(\S+).*?starting_total_duration=([0-9.]+)(ms|\u00b5s|\u03bcs|ns|s)')
RE_CAPTURE = re.compile(r"pause capture completed.*?capture_ms=([0-9.]+)")
RE_HANDSHAKE = re.compile(r"pause proxy activity.*?handshake_ms=([0-9.]+)")
RE_ARTIFACT = re.compile(r"task_artifact_prepare_duration=([0-9.]+)(ms|\u00b5s|\u03bcs|ns|s)")
RE_RESTORE_ACK = re.compile(r"restore notify acked in ([0-9]+)\u00b5s")
RE_PREFETCH = re.compile(r"memory prefetch \w+ .*?duration=([0-9.]+)(ms|\u00b5s|\u03bcs|ns|s)")


def load_journal(path):
    """Return (marks, lines) where lines are (epoch_ms, message)."""
    marks = []
    lines = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                # plain `journalctl -o short-iso-precise` fallback
                lines.append((None, raw))
                continue
            msg = rec.get("MESSAGE")
            if isinstance(msg, bytes):
                msg = msg.decode("utf-8", "replace")
            if not isinstance(msg, str):
                continue
            ts = rec.get("__REALTIME_TIMESTAMP")
            ts_ms = int(ts) / 1000.0 if ts else None
            if msg.startswith("KUASAR_PERF "):
                try:
                    payload = json.loads(msg[len("KUASAR_PERF "):])
                except json.JSONDecodeError:
                    continue
                if ts_ms is not None and not payload.get("wall_ns"):
                    payload["wall_ns"] = int(ts_ms * 1e6)
                payload["_ts_ms"] = ts_ms
                marks.append(payload)
            else:
                lines.append((ts_ms, msg))
    return marks, lines


def group_marks(marks):
    """rid -> {comp, op, sid, run, first_wall_ns, events{evt: [(kind, off_ms, wall_ns, rec)]}}"""
    sets = {}
    for m in marks:
        rid = m.get("rid")
        if not rid:
            continue
        # rid is only unique inside one process: every sandbox-ctl run child
        # restarts its own sequence, so the pid is part of the key.
        key = (rid, m.get("pid"))
        s = sets.setdefault(key, {
            "comp": m.get("comp"), "op": m.get("op"), "sid": m.get("sid"),
            "run": m.get("run"), "events": defaultdict(list),
        })
        if m.get("sid"):
            s["sid"] = m["sid"]
        if m.get("run"):
            s["run"] = m["run"]
        s["events"][m["evt"]].append(
            (m.get("kind"), m.get("off_ms", 0.0), m.get("wall_ns", 0), m))
    for s in sets.values():
        walls = [w for evts in s["events"].values() for (_, _, w, _) in evts]
        s["first_wall_ns"] = min(walls) if walls else 0
        s["last_wall_ns"] = max(walls) if walls else 0
    return sets


def evt_off(mset, evt, idx=0):
    got = mset["events"].get(evt) if mset else None
    if not got or len(got) <= idx:
        return None
    return got[idx][1]


def evt_wall(mset, evt, idx=0):
    got = mset["events"].get(evt) if mset else None
    if not got or len(got) <= idx:
        return None
    return got[idx][2]


def evt_rec(mset, evt, idx=0):
    got = mset["events"].get(evt) if mset else None
    if not got or len(got) <= idx:
        return None
    return got[idx][3]


def interval(mset, a, b):
    """Interval between two cumulative marks (b - a) in ms."""
    oa, ob = evt_off(mset, a), evt_off(mset, b)
    if oa is None or ob is None:
        return None
    return ob - oa


def duration(mset, evt, idx=0):
    rec = evt_rec(mset, evt, idx)
    if not rec or rec.get("kind") != "interval":
        return None
    return rec.get("off_ms")


def first_last(mset):
    if not mset:
        return None
    return (mset["last_wall_ns"] - mset["first_wall_ns"]) / 1e6


# ------------------------------------------------------------------- ops

def load_ops(path):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def select_sets(sets, sid, start_ms, end_ms, comp=None, op=None):
    lo, hi = start_ms * 1e6 - 50e6, end_ms * 1e6 + 80e6
    got = []
    for s in sets:
        if sid and s.get("sid") and s["sid"] != sid:
            continue
        if comp and s.get("comp") != comp:
            continue
        if op and s.get("op") != op:
            continue
        if not s["first_wall_ns"]:
            continue
        if s["first_wall_ns"] < lo or s["first_wall_ns"] > hi:
            continue
        got.append(s)
    return got


def pick(sets, sid, start, end, comp, op):
    """Pick the single most relevant mark set; the longest one on ties."""
    cands = select_sets(sets, sid, start, end, comp=comp, op=op)
    if not cands:
        return None
    cands.sort(key=lambda s: (len(s["events"]), s["first_wall_ns"]))
    return cands[-1]


# ------------------------------------------------------------- per-op metrics

def metrics_create(op, sets, lines):
    sid, start, end = op["sid"], op["start_ms"], op["end_ms"]
    api = pick(sets, sid, start, end, "api", "create")
    adm = pick(sets, sid, start, end, "orch", "create-admission")
    launch = pick(sets, sid, start, end, "orch", "create")
    tail = pick(sets, sid, start, end, "orch", "launch-tail")
    runner = pick(sets, sid, start, end, "runner", "run-sandbox")
    task = pick(sets, sid, start, end, "runner", "launch-task")
    runtime = pick(sets, sid, start, end, "runtime", "resume")
    m = {}
    m["client"] = op["dur_ms"]
    m["api.window"] = interval(api, "api.recv", "api.response")
    if m["api.window"] is not None:
        m["residual.client"] = m["client"] - m["api.window"]
    m["api.decode"] = interval(api, "api.recv", "api.decoded")
    m["api.core"] = duration(api, "api.core")

    m["admission"] = interval(adm, "admission.begin", "admission.end")
    m["admission.preflight"] = duration(adm, "admission.launch_preflight")
    m["admission.claim_insert"] = duration(adm, "admission.claim_insert")
    m["admission.publish"] = duration(adm, "admission.publish")
    m["admission.route_barrier"] = duration(adm, "admission.route_barrier")

    disp = evt_rec(launch, "orch.launch.dispatch")
    m["launch.dispatch"] = disp.get("delay_ms") if disp else None
    m["launch.total"] = interval(launch, "orch.launch.enter", "orch.launch.exit")

    m["unit.start"] = interval(runner, "runner.begin", "runner.assigned")
    m["unit.pidfile"] = interval(runner, "runner.begin", "runner.pidfile")
    m["unit.session"] = interval(runner, "runner.pidfile", "runner.session")
    m["unit.wait_assign"] = interval(runner, "runner.session", "runner.assigned")
    m["unit.readiness_dial"] = interval(runner, "runner.assigned", "runner.readiness_dial")

    m["task.bootstrap"] = duration(task, "task.bootstrap")
    m["task.artifact"] = duration(task, "task.artifact")
    m["task.complete_prepare"] = duration(task, "task.complete_prepare")

    gl = pick(sets, None, start, end, "guestlink", "restore")
    m["runtime.restore_dial"] = duration(gl, "restore.dial")
    m["runtime.guest_ack"] = duration(gl, "restore.guest_ack")
    if runtime:
        m["runtime.open"] = interval(runtime, "restore.begin", "restore.open")
        m["runtime.memory_source"] = interval(runtime, "restore.open", "restore.memory_source")
        m["runtime.disks"] = interval(runtime, "restore.memory_source", "restore.disks")
        m["runtime.ch_spawn"] = interval(runtime, "restore.disks", "restore.ch_spawned")
        m["runtime.api_ready"] = duration(runtime, "restore.api_ready")
        m["runtime.vm_resume"] = duration(runtime, "restore.vm_resume")
        m["runtime.ack"] = duration(runtime, "restore.ack")
        m["runtime.total"] = (lambda a, b: None if a is None or b is None else b - a)(
            evt_off(runtime, "restore.begin"),
            max([evt_off(runtime, "restore.ready") or 0,
                 evt_off(runtime, "restore.ack") or 0]))
    else:
        for k in ("open", "memory_source", "disks", "ch_spawn", "api_ready",
                  "vm_resume", "ack", "ready_to_end", "total"):
            m["runtime." + k] = None

    for key in ("readiness_wait", "workspace_bind", "envd_init", "commit_store",
                "publish", "total"):
        m["tail." + key] = duration(tail, "tail." + key)

    # launch-phase lines emitted by the conductor (cross-check / fallback)
    for phase, val in phases_for(lines, sid, "create"):
        m["phase." + phase] = val
    return m


def metrics_resume(op, sets, lines):
    sid, start, end = op["sid"], op["start_ms"], op["end_ms"]
    api = pick(sets, sid, start, end, "api", "resume")
    launch = pick(sets, sid, start, end, "orch", "resume")
    tail = pick(sets, sid, start, end, "orch", "launch-tail")
    runner = pick(sets, sid, start, end, "runner", "run-sandbox")
    task = pick(sets, sid, start, end, "runner", "launch-task")
    runtime = pick(sets, sid, start, end, "runtime", "resume")
    m = {"client": op["dur_ms"]}
    m["api.window"] = interval(api, "api.recv", "api.response")
    if m["api.window"] is not None:
        m["residual.client"] = m["client"] - m["api.window"]
    if m["api.window"] is not None:
        m["residual.client"] = m["client"] - m["api.window"]
    m["api.decode"] = interval(api, "api.recv", "api.decoded")
    m["api.core"] = duration(api, "api.core")
    disp = evt_rec(launch, "orch.launch.dispatch")
    m["launch.dispatch"] = disp.get("delay_ms") if disp else None
    m["launch.total"] = interval(launch, "orch.launch.enter", "orch.launch.exit")
    m["unit.start"] = interval(runner, "runner.begin", "runner.assigned")
    m["task.bootstrap"] = duration(task, "task.bootstrap")
    m["task.artifact"] = duration(task, "task.artifact")
    m["task.complete_prepare"] = duration(task, "task.complete_prepare")
    gl = pick(sets, None, start, end, "guestlink", "restore")
    m["runtime.restore_dial"] = duration(gl, "restore.dial")
    m["runtime.guest_ack"] = duration(gl, "restore.guest_ack")
    if runtime:
        m["runtime.open"] = interval(runtime, "restore.begin", "restore.open")
        m["runtime.memory_source"] = interval(runtime, "restore.open", "restore.memory_source")
        m["runtime.disks"] = interval(runtime, "restore.memory_source", "restore.disks")
        m["runtime.ch_spawn"] = interval(runtime, "restore.disks", "restore.ch_spawned")
        m["runtime.api_ready"] = duration(runtime, "restore.api_ready")
        m["runtime.vm_resume"] = duration(runtime, "restore.vm_resume")
        m["runtime.ack"] = duration(runtime, "restore.ack")
        m["runtime.total"] = (lambda a, b: None if a is None or b is None else b - a)(
            evt_off(runtime, "restore.begin"),
            max([evt_off(runtime, "restore.ready") or 0,
                 evt_off(runtime, "restore.ack") or 0]))
    else:
        for k in ("open", "memory_source", "disks", "ch_spawn", "api_ready",
                  "vm_resume", "ack", "total"):
            m["runtime." + k] = None
    for key in ("readiness_wait", "workspace_bind", "envd_init", "commit_store",
                "publish", "total"):
        m["tail." + key] = duration(tail, "tail." + key)
    for phase, val in phases_for(lines, sid, "resume"):
        m["phase." + phase] = val
    return m


def metrics_pause(op, sets, lines):
    sid, start, end = op["sid"], op["start_ms"], op["end_ms"]
    api = pick(sets, sid, start, end, "api", "pause")
    orch = pick(sets, sid, start, end, "orch", "pause")
    barrier = pick(sets, sid, start, end, "orch", "pause-barrier")
    cleanup = pick(sets, sid, start, end, "orch", "pause-cleanup")
    runtime = pick(sets, sid, start, end, "runtime", "pause")
    take = pick(sets, sid, start, end, "runtime", "snapshot-take")
    m = {"client": op["dur_ms"]}
    m["api.window"] = interval(api, "api.recv", "api.response")
    if m["api.window"] is not None:
        m["residual.client"] = m["client"] - m["api.window"]
    if m["api.window"] is not None:
        m["residual.client"] = m["client"] - m["api.window"]
    m["api.decode"] = interval(api, "api.recv", "api.decoded")
    m["api.core"] = duration(api, "api.core")
    m["orch.admission"] = interval(orch, "orch.pause.begin", "orch.pause.admission")
    m["orch.sourceop"] = duration(orch, "orch.pause.sourceop")
    m["orch.op"] = duration(orch, "orch.pause.op")
    m["orch.barrier"] = duration(orch, "orch.pause.barrier")
    m["orch.capture"] = duration(orch, "orch.pause.capture")
    m["orch.commit"] = duration(orch, "orch.pause.commit")
    m["orch.cleanup"] = duration(orch, "orch.pause.cleanup")
    m["orch.publish"] = duration(orch, "orch.pause.publish")
    m["orch.total"] = interval(orch, "orch.pause.begin", "orch.pause.end")
    m["barrier.wait"] = interval(barrier, "barrier.begin", "barrier.wait_done")
    m["barrier.commit"] = interval(barrier, "barrier.wait_done", "barrier.commit")
    m["barrier.set_pausing"] = interval(barrier, "barrier.commit", "barrier.end")
    if runtime:
        m["snap.validate"] = interval(runtime, "snap.recv", "snap.validate")
        m["snap.history"] = interval(runtime, "snap.validate", "snap.history")
        m["snap.deps"] = interval(runtime, "snap.history", "snap.deps")
        m["snap.membarrier"] = interval(runtime, "snap.deps", "snap.membarrier")
        m["snap.quiesce"] = duration(runtime, "snap.quiesce")
        m["snap.take"] = duration(runtime, "snap.take")
        m["snap.total"] = interval(runtime, "snap.recv", "snap.end")
        if m["snap.total"] is not None and m["orch.capture"] is not None:
            m["residual.capture"] = m["orch.capture"] - m["snap.total"]
    else:
        for k in ("validate", "history", "deps", "membarrier", "quiesce", "take",
                  "total"):
            m["snap." + k] = None
    if take:
        m["take.vmm_pause"] = duration(take, "vmm.pause")
        m["take.disk"] = duration(take, "take.disk")
        m["take.ch_snapshot"] = interval(take, "take.begin", "take.ch_snapshot")
        m["take.mem_capture"] = interval(take, "take.ch_snapshot", "take.mem_capture")
        m["take.mem_build"] = interval(take, "take.mem_capture", "take.mem_build")
        m["take.mem_absorb"] = duration(take, "take.mem_absorb")
        m["take.commit"] = duration(take, "take.commit")
        m["take.total"] = interval(take, "take.begin", "take.end")
    else:
        for k in ("vmm_pause", "disk", "ch_snapshot", "mem_capture", "mem_build",
                  "mem_absorb", "commit", "total"):
            m["take." + k] = None
    m["cleanup.fence"] = duration(cleanup, "cleanup.fence")
    m["cleanup.detach"] = duration(cleanup, "cleanup.detach")
    m["cleanup.checkpoint"] = duration(cleanup, "cleanup.checkpoint")
    m["cleanup.rundir"] = duration(cleanup, "cleanup.rundir")
    # legacy lines
    for ts, msg in lines_for(lines, sid, start, end):
        mm = RE_CAPTURE.search(msg)
        if mm:
            m["log.capture_ms"] = float(mm.group(1))
        mm = RE_HANDSHAKE.search(msg)
        if mm:
            m["log.handshake_ms"] = float(mm.group(1))
    return m


def lines_for(lines, sid, start, end):
    lo, hi = start - 2000, end + 4000
    out = []
    for ts, msg in lines:
        if ts is None:
            continue
        if ts < lo or ts > hi:
            continue
        if sid and sid not in msg:
            continue
        out.append((ts, msg))
    return out


def phases_for(lines, sid, kind):
    out = []
    for ts, msg in lines:
        if sid not in msg:
            continue
        m = RE_PHASE.search(msg)
        if not m or m.group(1) != sid or m.group(2) != kind:
            continue
        out.append((m.group(3), dur_units(m.group(4), m.group(5))))
    return out


# ------------------------------------------------------------------ reporting

CREATE_TREE = [

    (0, "① 客户端：POST /sandboxes → state=running", "client", "cube.create.client"),
    (1, "② API 请求窗口（同步 admission 部分）", "api.window", "cube.create.api"),
    (2, "decode/校验", "api.decode", None),
    (2, "core.Create（admission）", "api.core", None),
    (3, "launch preflight（镜像/快照解析）", "admission.preflight", None),
    (3, "claim + 落库 + acceptedAt", "admission.claim_insert", None),
    (3, "cache/publish 初始状态", "admission.publish", None),
    (3, "proxy 路由 barrier", "admission.route_barrier", None),
    (1, "[客户端等待] response → state=running（含轮询）", "residual.client", None),
    (1, "③ 异步 launch 总计（conductor）", "launch.total", "cube.resume.client"),
    (2, "accept→launch worker 调度延迟", "launch.dispatch", None),
    (2, "runner 单元启动（systemd 到 assigned）", "unit.start", None),
    (3, "pidfile / session", "unit.pidfile", None),
    (3, "等待 assignment（config-socket）", "unit.wait_assign", None),
    (3, "readiness socket 连接", "unit.readiness_dial", None),
    (2, "runner 任务引导（bootstrap）", "task.bootstrap", None),
    (2, "artifact prepare（runner 进程内）", "task.artifact", None),
    (2, "complete prepare（回执 + final spec）", "task.complete_prepare", None),
    (2, "runtime 总计（restore/cold）", "runtime.total", "cube.resume.shim_create"),
    (3, "打开 Snapshot/Sandbox 元数据", "runtime.open", None),
    (3, "memory 层打开 + UFFD source", "runtime.memory_source", "cube.resume.restore_vm"),
    (3, "磁盘图重建", "runtime.disks", None),
    (3, "spawn CH", "runtime.ch_spawn", "cube.resume.launch_vmm"),
    (3, "CH API ready", "runtime.api_ready", None),
    (3, "POST /vm.resume", "runtime.vm_resume", "cube.resume.restore_vm"),
    (3, "restore ACK + stdio MUX", "runtime.ack", "cube.resume.reset_vm"),
    (4, "vsock CONNECT/OK（dial）", "runtime.restore_dial", None),
    (4, "guest restore_ack", "runtime.guest_ack", None),
    (2, "readiness 等待（conductor 侧）", "tail.readiness_wait", None),
    (2, "workspace 磁盘 bind", "tail.workspace_bind", None),
    (2, "envd /init", "tail.envd_init", "cube.resume.probe"),
    (2, "CommitPreparedRunning（store）", "tail.commit_store", None),
    (2, "publish/observe running", "tail.publish", None),
]

RESUME_TREE = [
    (0, "① 客户端：POST /connect → state=running", "client", "cube.resume.client"),
    (1, "② API 请求窗口（同步 admission）", "api.window", "cube.resume.request"),
    (2, "decode/校验", "api.decode", "cube.resume.outer"),
    (2, "core.Connect（admission + 恢复清理）", "api.core", None),
    (1, "[客户端等待] response → state=running（含轮询）", "residual.client", None),
    (1, "③ 异步 launch 总计（conductor）", "launch.total", None),
    (2, "accept→launch worker 调度延迟", "launch.dispatch", None),
    (2, "runner 单元启动（systemd 到 assigned）", "unit.start", None),
    (2, "runner 任务引导（bootstrap）", "task.bootstrap", None),
    (2, "artifact prepare（runner 进程内）", "task.artifact", None),
    (2, "complete prepare（回执 + final spec）", "task.complete_prepare", None),
    (2, "runtime 总计（restore）", "runtime.total", "cube.resume.shim_create"),
    (3, "打开 Snapshot/Sandbox 元数据", "runtime.open", None),
    (3, "memory 层打开 + UFFD source", "runtime.memory_source", "cube.resume.restore_vm"),
    (3, "磁盘图重建", "runtime.disks", None),
    (3, "spawn CH", "runtime.ch_spawn", "cube.resume.launch_vmm"),
    (3, "CH API ready", "runtime.api_ready", None),
    (3, "POST /vm.resume", "runtime.vm_resume", "cube.resume.restore_vm"),
    (3, "restore ACK + stdio MUX", "runtime.ack", "cube.resume.reset_vm"),
    (4, "vsock CONNECT/OK（dial）", "runtime.restore_dial", None),
    (4, "guest restore_ack", "runtime.guest_ack", None),
    (2, "readiness 等待（conductor 侧）", "tail.readiness_wait", None),
    (2, "workspace 磁盘 bind", "tail.workspace_bind", None),
    (2, "envd /init", "tail.envd_init", "cube.resume.probe"),
    (2, "CommitPreparedRunning（store）", "tail.commit_store", None),
    (2, "publish/observe running", "tail.publish", None),
]

PAUSE_TREE = [
    (0, "① 客户端：POST /pause → state=paused", "client", "cube.pause.client"),
    (1, "② API 请求窗口", "api.window", "cube.pause.request"),
    (2, "decode/校验", "api.decode", None),
    (2, "core.Pause（同步整段）", "api.core", None),
    (3, "admission（锁/状态/checkpoint 限额）", "orch.admission", None),
    (3, "source operation 互斥", "orch.sourceop", None),
    (3, "acceptedOps.Begin", "orch.op", None),
    (3, "proxy barrier + SetPausing", "orch.barrier", None),
    (4, "barrier 等待 proxy 静默", "barrier.wait", None),
    (4, "barrier commit", "barrier.commit", None),
    (4, "SetPausing（store）", "barrier.set_pausing", None),
    (3, "capture（exec sandbox-ctl snapshot）", "orch.capture", "cube.pause.shim_total"),
    (4, "runtime: 进入快照请求", "snap.validate", None),
    (4, "runtime: history/dependency 准备", "snap.deps", None),
    (4, "runtime: memory barrier + memory.high", "snap.membarrier", None),
    (4, "guest quiesce（sync/drop_caches/MUX）", "snap.quiesce", "cube.pause.disconnect_agent"),
    (4, "snapshot.Take 总计", "snap.take", "cube.pause.shim_total"),
    (5, "CH /vm.pause", "take.vmm_pause", None),
    (5, "磁盘 E 捕获", "take.disk", "cube.pause.commit_rootfs"),
    (5, "CH /vm.snapshot + config/state", "take.ch_snapshot", "cube.pause.vmm_snapshot"),
    (5, "memory 视图 + 压缩", "take.mem_capture", None),
    (5, "BuildCaptureSource", "take.mem_build", None),
    (5, "写 memory/Snapshot S", "take.mem_absorb", None),
    (5, "commit + close sink", "take.commit", None),
    (3, "CommitPaused（store）", "orch.commit", "cube.pause.mark_paused"),
    (3, "ownership cleanup 总计", "orch.cleanup", "cube.pause.destroy_live"),
    (4, "fence runner（SIGKILL+回收）", "cleanup.fence", None),
    (4, "vswitch detach", "cleanup.detach", None),
    (4, "checkpoint 清理", "cleanup.checkpoint", None),
    (4, "run dir 删除", "cleanup.rundir", None),
    (3, "publish paused", "orch.publish", None),
    (1, "[客户端] response → state=paused（含轮询）", "residual.client", None),
    (3, "[ctl] exec/连接 sandbox-ctl 开销", "residual.capture", None),
]


def agg(rows, key):
    vals = [r[key] for r in rows if r.get(key) is not None]
    return len(vals), mean(vals), pct(vals, 50), pct(vals, 95)


def render_tree(title, spec, rows, cube_col=True):
    out = []
    out.append(f"### {title}")
    out.append("")
    header = f"{'阶段（缩进=包含）':<46}{'n':>3} {'mean':>9} {'p50':>9} {'p95':>9}"
    if cube_col:
        header += f" {'CubeSandbox':>12}"
    out.append(header)
    out.append("-" * len(header))
    for depth, label, key, cube_key in spec:
        if key is None:
            out.append(f"{'    ' * depth}{label:<{46 - 4 * depth}}{'':>3} {'[未观测]':>9}")
            continue
        n, mu, p50, p95 = agg(rows, key)
        line = (f"{'    ' * depth}{label:<{46 - 4 * depth}}{n:>3} "
                f"{fmt(mu):>9} {fmt(p50):>9} {fmt(p95):>9}")
        if cube_col:
            ref = CUBE_REF.get(cube_key) if cube_key else None
            line += f" {fmt(ref, 12):>12}"
        out.append(line)
    out.append("")
    return "\n".join(out)


def summarize(rows, keys):
    out = []
    for k in keys:
        n, mu, p50, p95 = agg(rows, k)
        out.append((k, n, mu, p50, p95))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ops", required=True)
    ap.add_argument("--journal", required=True)
    ap.add_argument("--json", default="")
    ap.add_argument("--md", default="")
    args = ap.parse_args()

    ops = load_ops(args.ops)
    marks, lines = load_journal(args.journal)
    sets = list(group_marks(marks).values())
    if not sets:
        print("no KUASAR_PERF marks in journal: is KUASAR_PERF=1 set on the units?",
              file=sys.stderr)

    grouped = defaultdict(list)
    for op in ops:
        kind = op["op"]
        if kind == "create":
            m = metrics_create(op, sets, lines)
        elif kind == "resume":
            m = metrics_resume(op, sets, lines)
        elif kind == "pause":
            m = metrics_pause(op, sets, lines)
        else:
            continue
        m["sid"] = op["sid"]
        m["c"] = op.get("c")
        grouped[(kind, op.get("c"))].append(m)

    doc = {"reference": CUBE_REF, "note": COMPARE_NOTE, "groups": {}}
    md = ["# kuasar-sandbox create / pause / resume 阶段分解", "",
          f"ops={args.ops} journal={args.journal}", "", COMPARE_NOTE, ""]

    for (kind, conc), rows in sorted(grouped.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)):
        spec = {"create": CREATE_TREE, "resume": RESUME_TREE, "pause": PAUSE_TREE}[kind]
        title = {"create": "create", "resume": "resume（connect/wake）",
                 "pause": "pause"}[kind] + f" · C={conc} · n={len(rows)}"
        md.append(render_tree(title, spec, rows))
        doc["groups"][f"{kind}:C{conc}"] = {
            "n": len(rows),
            "stages": {k: dict(zip(("n", "mean_ms", "p50_ms", "p95_ms"), agg(rows, k)))
                       for _, _, k, _ in spec if k},
        }

    text = "\n".join(md)
    print(text)
    if args.md:
        with open(args.md, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, ensure_ascii=False)
            f.write("\n")


if __name__ == "__main__":
    main()
