#!/usr/bin/env bash
# e2b-stage-bench.sh — create / pause / resume benchmark through the
# E2B-compatible API, shaped like the CubeSandbox benchmarks
# (create_bench.py + dd_warm_stage_bench.py + pause/resume stage summaries).
#
# Required environment:
#   E2B_API_KEY        API key issued by the node
#   E2B_TEMPLATE_ID    canonical template id (e.g. e2b-snp-...)
# Optional:
#   E2B_API_URL        control-plane URL (default http://127.0.0.1:3000)
#   MODE               pause-resume (default) | create
#   CONCURRENCY        for create: integer; for pause-resume: comma list (1,5,10)
#   ROUNDS             rounds per concurrency (default 3)
#   WARM=1             read local checkpoint files into page cache after pause
#   WARM_BASE_ROOT     node base root (default /var/lib/sandbox)
#   OUT                JSONL sample path (default test/results/e2b-stage-samples.jsonl)
#   CONDUCTOR_LOG      if set, also print the per-phase breakdown for that log
#
# The driver only uses the E2B HTTP surface (POST /sandboxes, /pause,
# /connect, DELETE); it does not need containerd access and can be pointed at
# any compatible node. A missing state field in GET /sandboxes/{id} is treated
# as "call returned = ready" for servers whose create/connect are synchronous.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${E2B_API_KEY:?set E2B_API_KEY}"
: "${E2B_TEMPLATE_ID:?set E2B_TEMPLATE_ID}"
E2B_API_URL="${E2B_API_URL:-http://127.0.0.1:3000}"
MODE="${MODE:-pause-resume}"
CONCURRENCY="${CONCURRENCY:-1,5,10}"
ROUNDS="${ROUNDS:-3}"
OUT="${OUT:-$HERE/../results/e2b-stage-samples.jsonl}"
mkdir -p "$(dirname "$OUT")"

DRIVER="$HERE/e2b_stage_bench.py"
case "$MODE" in
  create)
    python3 "$DRIVER" --api "$E2B_API_URL" --key "$E2B_API_KEY" \
      --template "$E2B_TEMPLATE_ID" create \
      -c "$CONCURRENCY" -n "$ROUNDS"
    ;;
  pause-resume)
    ARGS=(pause-resume --concurrency "$CONCURRENCY" -n "$ROUNDS" --out "$OUT")
    if [ "${WARM:-0}" = 1 ]; then
      ARGS+=(--warm --warm-base-root "${WARM_BASE_ROOT:-/var/lib/sandbox}")
    fi
    python3 "$DRIVER" --api "$E2B_API_URL" --key "$E2B_API_KEY" \
      --template "$E2B_TEMPLATE_ID" "${ARGS[@]}"
    if [ -n "${CONDUCTOR_LOG:-}" ]; then
      echo
      python3 "$DRIVER" analyze --log "$CONDUCTOR_LOG" --samples "$OUT"
    fi
    ;;
  *)
    echo "unknown MODE=$MODE (want create|pause-resume)" >&2
    exit 2
    ;;
esac
