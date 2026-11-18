# test/perf: create / pause / resume and restore-reset tooling

Three tools cover the design v3 ("vsock transport reset before freeze",
`docs/design/vsock-reset-before-freeze.md`) measurements. They only measure:
nothing here builds, deploys, changes configuration or commits.

| Tool | Purpose |
|---|---|
| `e2b_stage_bench.py` | E2B-compatible create / pause / resume load generator, with per-attempt ops JSONL |
| `stage_tree.py` | Joins the ops JSONL with `journalctl -o json` and prints the nested stage tree |
| `restore_stress.py` | Full create -> N connections -> pause -> resume -> guest inspection cycles, per-arm stats and paired A/B |

Prerequisites on the node:

- `KUASAR_PERF=1` on the conductor/runner/runtime units for the run window.
- The host switch is `KUASAR_VSOCK_RESET_BEFORE_FREEZE=1` (the design calls it
  `vsock_reset_before_freeze=on`). It is read once when a `sandbox-ctl` process
  starts and defaults to off; `KUASAR_VSOCK_RESET_TIMEOUT_MS` overrides the
  200 ms CH-side ACK timeout.
- `E2B_API_KEY`, `E2B_TEMPLATE_ID` (and optionally `E2B_API_URL`).
- `sandbox-ctl` on `PATH` (or `SANDBOX_CTL=/path/to/sandbox-ctl`).
- Read access to the journal (`journalctl`) and to the node roots
  (`/run/sandbox/sandboxes`, `/var/lib/sandbox`).
- A template re-captured with the new path for the `on` arm; an old template
  (`pending=true`) always walks the gate and cannot show the gain.

## 1. e2b_stage_bench.py

```sh
export E2B_API_URL=http://127.0.0.1:3000
export E2B_API_KEY=e2b_...
export E2B_TEMPLATE_ID=e2b-snp-...

# create only, C=1 and C=10
python3 test/perf/e2b_stage_bench.py create -c 1 -n 10 --ops-out /tmp/off-create.ops.jsonl

# full create -> pause -> resume, two warm-up rounds discarded, arm labelled
python3 test/perf/e2b_stage_bench.py pause-resume \
    --concurrency 1 -n 30 --arm off \
    --out /tmp/off.pr.jsonl --ops-out /tmp/off.pr.ops.jsonl \
    --warm --warm-base-root /var/lib/sandbox
```

Shared flags (place them **after** the subcommand):

- `--strict`: one attempt per operation; the first error aborts the run.
- `--max-retries N`: additional attempts per operation (default 2 = 3 total,
  the historical behaviour). In `--strict` mode retries are disabled.
- `--append`: allow appending to an existing `--out` / `--ops-out`. Without it
  the tool refuses to start if the file is non-empty, so two arms cannot
  silently share one samples file.
- `--arm off|on`: label written to every sample and ops record.

Every attempt, including failed ones, is written to `--ops-out` with `ok`,
`attempt`, `phase` (`create|wait|pause|connect|restore`) and `err_class`.
`stage_tree.py` ignores `ok=false` records, so failure counts remain visible in
the JSONL instead of being smoothed away by retries. The guest restore ACK is
node-side; from this API it is part of the `restore` phase, not a separate
client phase.

`pause-resume` discards `--warmup-rounds` (default 2) full rounds before the
measured rounds. The wrapper `e2b-stage-bench.sh` is unchanged, but reruns need
a new `OUT` path (or `--append`) now that stale sample files are refused.

## 2. stage_tree.py

```sh
journalctl -o json --no-pager --since "-20 min" > /tmp/journal.json
python3 test/perf/stage_tree.py \
    --ops /tmp/on.pr.ops.jsonl --journal /tmp/journal.json \
    --json /tmp/tree.json --md /tmp/tree.md --strict
```

Design-v3 additions:

- `pause`: `snap.vsock_reset` (reset-now duration) and
  `snap.vsock_reset_result` (published/acked). The result is printed as an
  annotation (`published=5/5 acked=5/5`), never averaged.
- `create` / `resume`: `restore.dial.first_try`, `restore.dial_attempts` and
  `restore.snapshot_state` (transport_reset_pending). The pending mark is an
  annotation (`pending=False 5/5, pending=True 0/5`).
- `--json` gains a per-group `quality` object with counters/rates for
  `snap.vsock_reset_result`, `restore.snapshot_state` and
  `restore.dial.first_try`. Booleans never enter the averaged `stages` object.
- `--strict`: a window matching more than one mark set for the same
  `(comp, op)` is an error and the script exits non-zero. Without it the
  longest candidate is used (fine at C=1, unsafe at C>1 because the longest set
  can belong to another sandbox).

## 3. restore_stress.py

One cycle: create -> establish N host->guest exec sessions -> pause -> read the
snapshot's vsock state -> resume -> `cat /proc/net/vsock` -> delete.

```sh
# negative control: run each arm separately, same binary, new out dir each time
python3 test/perf/restore_stress.py --arms off --cycles 30 --connections 2 \
    --template "$E2B_TEMPLATE_ID" --journal-since "@$(date +%s)" \
    --out /tmp/stress-off

python3 test/perf/restore_stress.py --arms on --cycles 30 --connections 2 \
    --template "$E2B_TEMPLATE_ID" --journal-since "@$(date +%s)" \
    --out /tmp/stress-on

# paired ABBA in one run; requires a per-cycle switch helper
python3 test/perf/restore_stress.py --arms off,on --cycles 64 \
    --arm-switch-cmd '/usr/local/bin/set-vsock-reset {arm}' \
    --out /tmp/stress-abba
```

Key flags: `--arms off,on`, `--cycles`, `--concurrency`, `--connections`,
`--api`, `--key`, `--template`, `--journal-since`, `--out`, plus: The label
`off` is the legacy arm (expects `pending=true`); any other label is treated as
the new path (expects `pending=false`, pending is a fallback).

- `--order abba|block`: `abba` (default) interleaves A B B A cycles; at
  `--concurrency N` the pattern is applied in blocks of N cycles, so all
  in-flight cycles belong to one arm and a switch never flips underneath them.
  `block` runs all of one arm then the other. Pairing is by the k-th cycle of
  each arm, which is exactly the adjacent ABBA pairs.
- `--arm-switch-cmd`: shell command template run whenever the batch changes
  arm (`{arm}` is replaced). The host switch is read once per `sandbox-ctl`
  process, so a real ABBA run needs the runner pool to recycle between
  batches; the harness never switches while a cycle of the previous arm is in
  flight. A deployment whose switch requires manually restarting the pool is
  usually measured with two separate `--arms off` / `--arms on` runs instead
  (then pairing is not available, but the per-arm stats are). Without
  `--arm-switch-cmd` the harness only warns and verifies the arm from the
  snapshot state; it never changes the host configuration itself. A minimal
  helper for a deployment that recycles runner units on demand:

  ```sh
  #!/bin/sh
  case "$1" in
    on)  systemctl set-environment KUASAR_VSOCK_RESET_BEFORE_FREEZE=1 ;;
    off) systemctl unset-environment KUASAR_VSOCK_RESET_BEFORE_FREEZE ;;
  esac
  # deployment-specific: make the pool start new sandbox-ctl processes here
  ```
- `--state-json PATH|GLOB`: explicit state.json / Snapshot S path (`{sid}`
  expands). By default the harness tries, in order:
  `<run-root>/<sid>/snap-state/state.json` (exact bytes CH restored),
  `<base-root>/sandboxes/<sid>/checkpoint/<sid>.snapshot`, then
  `<base-root>/<sid>/checkpoint/<sid>.snapshot`. Snapshot S is read through its
  appended ZIP tail; an encrypted/tarstream-only carrier has no readable
  state.json. If no carrier is readable, the
  `restore.snapshot_state.local_port_last` mark (emitted by restore from the
  same state.json bytes) is used as the fallback. A cycle reports
  `state_verified=false` only when both are absent.
- `--require-state`: treat an unreadable state.json as a violation (otherwise
  the cycle records `state_verified=false` and the stale/pending checks are
  reported as unverified).
- `--restore-deadline-ms` (default 10000) and optional `--dial-deadline-ms`
  (0 = disabled).
- `--soft`: report violations but exit 0.
- `--journal-file` / `--journal-unit` / `--journal-until`: offline or filtered
  journal collection.

Guest inspection uses the flag set of this deployment (verified against
`sandboxer/cmd/sandbox-ctl/exec.go`; `exec` has **no** `--base-root`):

```sh
sandbox-ctl exec --sandbox-id SID --run-root /run/sandbox/sandboxes \
    -- /bin/cat /proc/net/vsock
```

Connections are `sandbox-ctl exec --stdin -- /bin/sh -c 'echo READY; exec /bin/cat'`;
each one is a host-initiated vsock connection to guest port 5000, the row shape
the stale predicate looks for. They stay open until after resume.

Output in `--out`:

- `cycles.jsonl` -- one record per cycle with timings, marks, attribution meta,
  snapshot state, stale rows, assertions, violations and errors.
- `summary.md` -- per-arm duration stats (n, median, p50/p95/p99), quality
  counters (first-try failures, stale cycles/rows, fallbacks, unverified
  state), and the paired A/B section (median paired difference + bootstrap 95%
  CI).
- `journal.jsonl`, `run.json` -- captured window and run parameters.

Stale predicate (exactly design 5.3): in the guest `/proc/net/vsock`, a row is
stale iff local port = 5000, peer CID = 2, peer port in
`[0x40000000, local_port_last]` and state is `01`/`02`/`03`. `04` and later
(CLOSE / CLOSE_WAIT / etc.) are not stale. The row created by the inspection
exec itself is the highest host port at inspection time and is excluded; if
that port is not above the snapshot cursor the cycle records a cursor-reuse
violation.

## 4. Negative control (required by design 5.1)

1. Build one binary/image. Do not change it between arms.
2. Confirm `KUASAR_PERF=1` is active for the whole window.
3. Arm `off`: make sure `KUASAR_VSOCK_RESET_BEFORE_FREEZE` is unset (or not
   `"1"`) for the `sandbox-ctl` processes that serve the run. The value is read
   at process start, so existing runner processes keep it until recycled.
   Create new sandboxes/templates after the switch is effective. Run >=30
   cycles.
4. Arm `on`: set `KUASAR_VSOCK_RESET_BEFORE_FREEZE=1` for newly started
   `sandbox-ctl` processes (same binary), recycle/start a fresh runner pool,
   and re-capture the template used for creates; old templates are
   `pending=true` and take the gate path. `KUASAR_VSOCK_RESET_TIMEOUT_MS`
   (default 200) can pin the ACK timeout if the test hook needs a boundary.
5. Verify with `restore_stress.py` (or `stage_tree.py`):
   - off: snapshot `transport_reset_pending=true`, `snap.vsock_reset` absent,
     `restore.dial` back at 60-70 ms, `snap.vsock_reset_result` absent;
   - on: `pending=false`, `snap.vsock_reset_result` `published=true acked=true`,
     `restore.dial` 1-3 ms, `restore.dial.first_try` present;
   - both: zero stale rows, no restore over the deadline.
6. If off does not reproduce 60-70 ms / `pending=true`, the measurement is
   invalid (design 5.1); `restore_stress.py` prints a negative-control warning
   when the off dial median is below 10 ms or when no off snapshot is pending.
7. Never reuse one `--out`/`--ops-out` file across arms without `--append`;
   `e2b_stage_bench.py` refuses it by default.

## 5. CH test hooks (`KUASAR_TEST_HOOKS=1`)

The design's failure-injection hooks (`delay-ack`, `drop-ack`, `no-desc`,
`fail-after-publish`, `stale-kick`) live in patch 0015. They are compiled in
only in a hook-enabled CH build; a release binary ignores them.

Environment flow in this deployment: `sandbox-ctl` spawns CH with
`exec.CommandContext` and inherits the environment, and the runner
(`sandbox-runner@<run-id>.service`) starts `sandbox-ctl`. So setting
`KUASAR_TEST_HOOKS=1` **on the runner unit for that run** is enough; no CH
flag and no permanent default change is needed.

Set it per run without touching the committed/default unit files. The manager
environment is inherited by runner units started afterwards; do not restart
existing `sandbox-runner@<run-id>` units, that would kill live sandboxes.

```sh
# before the run: new runner processes inherit this (cleared on reboot)
sudo systemctl set-environment KUASAR_TEST_HOOKS=1

# run the injection scenario through the normal tools; new sandboxes get the
# hook-enabled CH, existing runner processes do not
python3 test/perf/restore_stress.py --arms on --cycles 5 ...

# afterwards, remove it so later runs use the default
sudo systemctl unset-environment KUASAR_TEST_HOOKS
```

Alternative (transient drop-in, also only affects units started after it is
installed):

```sh
sudo systemctl edit --runtime sandbox-runner@.service
#   [Service]
#   Environment=KUASAR_TEST_HOOKS=1

# let the pool start new runners; do not restart active ones
```

For a single manual sandbox (no conductor):

```sh
KUASAR_TEST_HOOKS=1 sandbox-ctl run --config ... --restore ...
```

Verify the hook is actually active before trusting a measurement: the design
specifies only the `KUASAR_TEST_HOOKS` gate; the per-hook selector is defined by
patch 0015. Check the CH build (release binaries ignore hooks) and the CH
stderr/log for the hook's announcement. If patch 0015 chooses a different
selector variable, add it to the same transient drop-in.

Expected assertions with a hook that makes the ACK fail
(`delay-ack`/`drop-ack`/`stale-kick` at the boundary):

- CH API response `published=true, acked=false`;
- snapshot state `transport_reset_pending=true` (fallback, no second stage);
- restore walks the gate and remains functional;
- one result per timeout boundary, never an intermediate state.

## 6. Assumptions that can invalidate measurements

- `/proc/net/vsock` is parsed by header name with a positional fallback. A
  kernel that omits `Peer_CID` or reorders columns produces
  `vsock_bad_rows`; the stale check is then unverified rather than wrong.
- `sandbox-ctl exec` here means: `--sandbox-id`, `--run-root
  /run/sandbox/sandboxes`, then `-- CMD`. There is no `--base-root` for
  `exec`. A deployment with a different PathID/leaf needs `--state-json`
  and/or a different `--run-root`.
- `local_port_last` comes from the restored `snap-state/state.json` or the
  checkpoint's Snapshot S, with `restore.snapshot_state.local_port_last` as the
  fallback. Encrypted/tarstream-only carriers work through the mark; only a
  build without `restore.snapshot_state` needs an explicit `--state-json`.
- `restore.dial*` marks are sid-tagged by the in-progress implementation
  (`HostClient.SID`). On an older build without that tag, concurrency > 1 makes
  the sets ambiguous; the harness reports those metrics as missing rather than
  borrowing another sandbox's number. Run C=1 for A/B numbers on such a build.
- Journal attribution uses a 2 s slack around each cycle; a clock jump or a
  journal without `__REALTIME_TIMESTAMP` degrades attribution.
- `restore_stress.py` uses the first exec after resume as the inspection; the
  inspection row is excluded by highest host port, which assumes monotonic
  host-port allocation (patch 0005).
- Client wall times include API polling (50 ms granularity); they are secondary
  metrics. Use the `snap.*`/`restore.*` marks for intervals.
