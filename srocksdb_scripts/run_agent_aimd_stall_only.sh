#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

TS=$(date +%m%d_%H%M%S)
OUTDIR=${OUTDIR:-"$REPO_ROOT/srocksdb_evaluation/agent_aimd_stall_only_${TS}"}
# 12 hours
DURATION_SEC=${DURATION_SEC:-43200}

DB_PATH=${DB_PATH:-/mnt/f2fs/rlrocksdb_log}
OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_s.ini"}
RL_POLLER_BIN=${RL_POLLER_BIN:-"$REPO_ROOT/rl_poller"}

WRITE_MB_PER_SEC=${WRITE_MB_PER_SEC:-500}
VALUE_SIZE=${VALUE_SIZE:-1024}
M_MIN=${M_MIN:-0.01}
M_MAX=${M_MAX:-0.5}

# AIMD defaults
AIMD_AI_STEP=${AIMD_AI_STEP:-0.005}
AIMD_MD_BETA=${AIMD_MD_BETA:-0.7}
RECOVER_FREE_SEC=${RECOVER_FREE_SEC:-30}

# Safety / controller defaults
SEMI_SAFE_STEP=${SEMI_SAFE_STEP:-0.03}
SEMI_SAFE_FLOOR=${SEMI_SAFE_FLOOR:-0.02}
DELTA_SMOOTH_ETA=${DELTA_SMOOTH_ETA:-0.18}
GAMMA_M=${GAMMA_M:-1.8}
RISK_BACKLOG_EPS=${RISK_BACKLOG_EPS:-0.18}
RISK_LATENCY_EPS=${RISK_LATENCY_EPS:-0.15}

# Pure AIMD baseline: disable soft-guard by default, but allow overriding.
SOFT_GUARD_ENABLED=${SOFT_GUARD_ENABLED:-0}
SOFT_GUARD_CAP_VALUE=${SOFT_GUARD_CAP_VALUE:-0.06}

# Near-stall / release defaults
NEAR_STALL_BACKLOG_HI_BYTES=${NEAR_STALL_BACKLOG_HI_BYTES:-32000000000}
NEAR_STALL_BACKLOG_RISE_WINDOW_SEC=${NEAR_STALL_BACKLOG_RISE_WINDOW_SEC:-30}
NEAR_STALL_BACKLOG_RISE_HI_BYTES=${NEAR_STALL_BACKLOG_RISE_HI_BYTES:-24000000000}
NEAR_STALL_L0_TH=${NEAR_STALL_L0_TH:-12}
NEAR_STALL_L0_BACKLOG_MIN_BYTES=${NEAR_STALL_L0_BACKLOG_MIN_BYTES:-24000000000}
NEAR_STALL_L0_FORCE_TH=${NEAR_STALL_L0_FORCE_TH:-16}
NEAR_STALL_TRIGGER_CONSECUTIVE_SEC=${NEAR_STALL_TRIGGER_CONSECUTIVE_SEC:-3}
SEMI_SAFE_MIN_ON_SEC=${SEMI_SAFE_MIN_ON_SEC:-0}
SEMI_SAFE_RELEASE_BACKLOG_LO_BYTES=${SEMI_SAFE_RELEASE_BACKLOG_LO_BYTES:-16000000000}
SEMI_SAFE_RELEASE_BACKLOG_RISE_WINDOW_SEC=${SEMI_SAFE_RELEASE_BACKLOG_RISE_WINDOW_SEC:-30}
SEMI_SAFE_RELEASE_BACKLOG_RISE_MAX_BYTES=${SEMI_SAFE_RELEASE_BACKLOG_RISE_MAX_BYTES:-1000000000000}
SEMI_SAFE_RELEASE_L0_TH=${SEMI_SAFE_RELEASE_L0_TH:-8}
SEMI_SAFE_RELEASE_HOLD_SEC=${SEMI_SAFE_RELEASE_HOLD_SEC:-3}
SEMI_SAFE_COOLDOWN_SEC=${SEMI_SAFE_COOLDOWN_SEC:-10}
HARD_LOCK_BACKLOG_BYTES=${HARD_LOCK_BACKLOG_BYTES:-48000000000}
HARD_LOCK_RELEASE_BACKLOG_BYTES=${HARD_LOCK_RELEASE_BACKLOG_BYTES:-36000000000}
HARD_LOCK_RELEASE_HOLD_SEC=${HARD_LOCK_RELEASE_HOLD_SEC:-60}

SUDO_CMD=${SUDO_CMD:-}
CLEAR_DB_BEFORE=${CLEAR_DB_BEFORE:-1}
COPY_DB_LOG=${COPY_DB_LOG:-1}

EXTRA_AGENT_ARGS=()

usage() {
  cat <<'USAGE'
Usage: ./run_agent_aimd_stall_only.sh [options] [-- <extra args for run_agent_fifo.sh>]

Options:
  --outdir PATH
  --duration-sec N
  --db_path PATH
  --options_file PATH
  --rl_poller_bin PATH
  --write_mb_per_sec N
  --value_size N
  --m_min N
  --m_max N
  --aimd_ai_step N
  --aimd_md_beta N
  --recover_free_sec N
  --soft_guard_enabled
  --soft_guard_disabled
  --no-clear-db
  --no-copy-db-log
  -h, --help
USAGE
}

clear_db() {
  if [[ -z "$DB_PATH" || "$DB_PATH" == "/" || "$DB_PATH" == "." ]]; then
    echo "[aimd] skip db cleanup: unsafe DB_PATH=$DB_PATH" >&2
    return 1
  fi
  if [[ ! -d "$DB_PATH" ]]; then
    echo "[aimd] skip db cleanup: DB_PATH not found ($DB_PATH)" >&2
    return 0
  fi
  echo "[aimd] cleaning DB_PATH=$DB_PATH"
  if [[ -n "$SUDO_CMD" ]]; then
    $SUDO_CMD find "$DB_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  else
    find "$DB_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
}

copy_db_log() {
  local src="$DB_PATH/LOG"
  local dst="$OUTDIR/db_LOG"
  if [[ ! -f "$src" ]]; then
    echo "[aimd] WARN: DB LOG not found at $src"
    return 0
  fi
  if cp "$src" "$dst" 2>/dev/null; then
    echo "[aimd] copied DB LOG -> $dst"
    return 0
  fi
  if [[ -n "$SUDO_CMD" ]] && $SUDO_CMD test -f "$src"; then
    if $SUDO_CMD cp "$src" "$dst"; then
      $SUDO_CMD chown "$(id -u):$(id -g)" "$dst" 2>/dev/null || true
      echo "[aimd] copied DB LOG with sudo -> $dst"
      return 0
    fi
  fi
  echo "[aimd] WARN: failed to copy DB LOG from $src"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir) OUTDIR="$2"; shift 2;;
    --duration-sec) DURATION_SEC="$2"; shift 2;;
    --db_path) DB_PATH="$2"; shift 2;;
    --options_file) OPTIONS_FILE="$2"; shift 2;;
    --rl_poller_bin) RL_POLLER_BIN="$2"; shift 2;;
    --write_mb_per_sec) WRITE_MB_PER_SEC="$2"; shift 2;;
    --value_size) VALUE_SIZE="$2"; shift 2;;
    --m_min) M_MIN="$2"; shift 2;;
    --m_max) M_MAX="$2"; shift 2;;
    --aimd_ai_step) AIMD_AI_STEP="$2"; shift 2;;
    --aimd_md_beta) AIMD_MD_BETA="$2"; shift 2;;
    --recover_free_sec) RECOVER_FREE_SEC="$2"; shift 2;;
    --soft_guard_enabled) SOFT_GUARD_ENABLED=1; shift 1;;
    --soft_guard_disabled) SOFT_GUARD_ENABLED=0; shift 1;;
    --no-clear-db) CLEAR_DB_BEFORE=0; shift 1;;
    --no-copy-db-log) COPY_DB_LOG=0; shift 1;;
    --) shift; EXTRA_AGENT_ARGS+=("$@"); break;;
    -h|--help) usage; exit 0;;
    *) EXTRA_AGENT_ARGS+=("$1"); shift 1;;
  esac
done

mkdir -p "$OUTDIR"

echo "[aimd] OUTDIR=$OUTDIR"
echo "[aimd] DURATION_SEC=$DURATION_SEC"
echo "[aimd] DB_PATH=$DB_PATH"
echo "[aimd] OPTIONS_FILE=$OPTIONS_FILE"
echo "[aimd] RL_POLLER_BIN=$RL_POLLER_BIN"
echo "[aimd] M_MIN=$M_MIN M_MAX=$M_MAX"
echo "[aimd] AIMD_AI_STEP=$AIMD_AI_STEP AIMD_MD_BETA=$AIMD_MD_BETA"
echo "[aimd] SOFT_GUARD_ENABLED=$SOFT_GUARD_ENABLED"

if [[ "$CLEAR_DB_BEFORE" -eq 1 ]]; then
  clear_db
fi

SOFT_GUARD_FLAG=("--soft_guard_disabled")
if [[ "$SOFT_GUARD_ENABLED" -eq 1 ]]; then
  SOFT_GUARD_FLAG=("--soft_guard_enabled")
fi

"$SCRIPT_DIR/run_agent_fifo.sh" \
  --outdir "$OUTDIR" \
  --db_path "$DB_PATH" \
  --options_file "$OPTIONS_FILE" \
  --rl_poller_bin "$RL_POLLER_BIN" \
  --write_mb_per_sec "$WRITE_MB_PER_SEC" \
  --value_size "$VALUE_SIZE" \
  --m_min "$M_MIN" \
  --m_max "$M_MAX" \
  --duration-sec "$DURATION_SEC" \
  --recover_controller_mode AIMD_STALL_ONLY \
  "${SOFT_GUARD_FLAG[@]}" \
  --aimd_ai_step "$AIMD_AI_STEP" \
  --aimd_md_beta "$AIMD_MD_BETA" \
  --gamma_perf "$GAMMA_M" \
  --risk_backlog_eps "$RISK_BACKLOG_EPS" \
  --risk_latency_eps "$RISK_LATENCY_EPS" \
  --soft_guard_cap_value "$SOFT_GUARD_CAP_VALUE" \
  --recover_free_sec "$RECOVER_FREE_SEC" \
  --semi_safe_step "$SEMI_SAFE_STEP" \
  --semi_safe_floor "$SEMI_SAFE_FLOOR" \
  --delta_smooth_eta "$DELTA_SMOOTH_ETA" \
  --near_stall_backlog_hi_bytes "$NEAR_STALL_BACKLOG_HI_BYTES" \
  --near_stall_backlog_rise_window_sec "$NEAR_STALL_BACKLOG_RISE_WINDOW_SEC" \
  --near_stall_backlog_rise_hi_bytes "$NEAR_STALL_BACKLOG_RISE_HI_BYTES" \
  --near_stall_l0_th "$NEAR_STALL_L0_TH" \
  --near_stall_l0_backlog_min_bytes "$NEAR_STALL_L0_BACKLOG_MIN_BYTES" \
  --near_stall_l0_force_th "$NEAR_STALL_L0_FORCE_TH" \
  --near_stall_trigger_consecutive_sec "$NEAR_STALL_TRIGGER_CONSECUTIVE_SEC" \
  --semi_safe_min_on_sec "$SEMI_SAFE_MIN_ON_SEC" \
  --semi_safe_release_backlog_lo_bytes "$SEMI_SAFE_RELEASE_BACKLOG_LO_BYTES" \
  --semi_safe_release_backlog_rise_window_sec "$SEMI_SAFE_RELEASE_BACKLOG_RISE_WINDOW_SEC" \
  --semi_safe_release_backlog_rise_max_bytes "$SEMI_SAFE_RELEASE_BACKLOG_RISE_MAX_BYTES" \
  --semi_safe_release_l0_th "$SEMI_SAFE_RELEASE_L0_TH" \
  --semi_safe_release_hold_sec "$SEMI_SAFE_RELEASE_HOLD_SEC" \
  --semi_safe_cooldown_sec "$SEMI_SAFE_COOLDOWN_SEC" \
  --hard_lock_backlog_bytes "$HARD_LOCK_BACKLOG_BYTES" \
  --hard_lock_release_backlog_bytes "$HARD_LOCK_RELEASE_BACKLOG_BYTES" \
  --hard_lock_release_hold_sec "$HARD_LOCK_RELEASE_HOLD_SEC" \
  "${EXTRA_AGENT_ARGS[@]}"

if [[ "$COPY_DB_LOG" -eq 1 ]]; then
  copy_db_log
fi

echo "[aimd] done"
echo "[aimd] agent_log: $OUTDIR/agent_log.csv"
echo "[aimd] agent_stdout: $OUTDIR/agent_rl_fifo.log"
echo "[aimd] poller_log: $OUTDIR/poller.log"
echo "[aimd] metrics: $OUTDIR/rl_metrics.csv"
echo "[aimd] resource: $OUTDIR/resource_usage.csv"
echo "[aimd] db_LOG: $OUTDIR/db_LOG"
