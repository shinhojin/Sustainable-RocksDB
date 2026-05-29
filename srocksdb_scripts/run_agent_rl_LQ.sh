#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

TS=$(date +%m%d_%H%M%S)
#OUTDIR=${OUTDIR:-"$REPO_ROOT/srocksdb_evaluation/agent_rl_lq_nvme_${TS}"}
OUTDIR=${OUTDIR:-"$REPO_ROOT/srocksdb_evaluation/agent_rl_lq_${TS}"}

# 24 hours
# DURATION_SEC=${DURATION_SEC:-86400}
# 12 hours
DURATION_SEC=${DURATION_SEC:-43200}

# 2TB SATA SSD
# DB_PATH=${DB_PATH:-/mnt/2tb_f2fs/rlrocksdb_log}

# 1TB SATA SSD
DB_PATH=${DB_PATH:-/mnt/f2fs/rlrocksdb_log}

# 1TB NVMe SSD
# DB_PATH=${DB_PATH:-/mnt/nvme_f2fs/rlrocksdb_log}

RL_POLLER_BIN=${RL_POLLER_BIN:-"$REPO_ROOT/rl_poller"}

# NVMe SSD - 3000 MB/s write
#WRITE_MB_PER_SEC=${WRITE_MB_PER_SEC:-3000}

# SATA SSD - 500 MB/s write
WRITE_MB_PER_SEC=${WRITE_MB_PER_SEC:-500}

VALUE_SIZE=${VALUE_SIZE:-1024}

# S-RocksDB(C)
OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_c.ini"}
M_MIN=${M_MIN:-0.01}
M_MAX=${M_MAX:-0.2}

# S-RocksDB(A)
# OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_a.ini"}
# M_MIN=${M_MIN:-0.01}
# M_MAX=${M_MAX:-0.5}

# Controller defaults
DELTA_ACTIONS=${DELTA_ACTIONS:-"-0.02,-0.01,0,0.01,0.02"}
DELTA_SMOOTH_ETA=${DELTA_SMOOTH_ETA:-0.18}
DELTA_M_MAX=${DELTA_M_MAX:-0.02}

SEMI_SAFE_STEP=${SEMI_SAFE_STEP:-0.03}
SEMI_SAFE_FLOOR=${SEMI_SAFE_FLOOR:-0.02}

RECOVER_FREE_SEC=${RECOVER_FREE_SEC:-30}
GAMMA_M=${GAMMA_M:-1.8}

# Linear-Q defaults
RL_EXPLORATION_EPSILON=${RL_EXPLORATION_EPSILON:-0.25}
RL_GAMMA=${RL_GAMMA:-0.95}
RL_ALPHA=${RL_ALPHA:-0.05}
STATE_SCALE_COMPACTION_BYTES=${STATE_SCALE_COMPACTION_BYTES:-1000000000}
STATE_SCALE_FLUSH_BYTES=${STATE_SCALE_FLUSH_BYTES:-100000000}
STATE_SCALE_WRITE_IN_BPS=${STATE_SCALE_WRITE_IN_BPS:-100000000}
STATE_SCALE_DELAY_BPS=${STATE_SCALE_DELAY_BPS:-100000000}
STATE_SCALE_P99_US=${STATE_SCALE_P99_US:-20000.0}
STATE_SCALE_L0=${STATE_SCALE_L0:-64.0}
STATE_CLIP=${STATE_CLIP:-5.0}
RISK_BACKLOG_EPS=${RISK_BACKLOG_EPS:-0.18}
RISK_LATENCY_EPS=${RISK_LATENCY_EPS:-0.15}

# Near-stall / release defaults
NEAR_STALL_BACKLOG_HI_BYTES=${NEAR_STALL_BACKLOG_HI_BYTES:-32000000000}

NEAR_STALL_BACKLOG_RISE_WINDOW_SEC=${NEAR_STALL_BACKLOG_RISE_WINDOW_SEC:-30}
NEAR_STALL_BACKLOG_RISE_HI_BYTES=${NEAR_STALL_BACKLOG_RISE_HI_BYTES:-24000000000}

NEAR_STALL_L0_TH=${NEAR_STALL_L0_TH:-12}

NEAR_STALL_L0_FORCE_TH=${NEAR_STALL_L0_FORCE_TH:-16}
NEAR_STALL_L0_BACKLOG_MIN_BYTES=${NEAR_STALL_L0_BACKLOG_MIN_BYTES:-24000000000}
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

SOFT_GUARD_CAP_VALUE=${SOFT_GUARD_CAP_VALUE:-0.06}

SUDO_CMD=${SUDO_CMD:-}
CLEAR_DB_BEFORE=${CLEAR_DB_BEFORE:-1}
COPY_DB_LOG=${COPY_DB_LOG:-1}

EXTRA_AGENT_ARGS=()

usage() {
  cat <<'USAGE'
Usage: ./run_agent_rl_LQ.sh [options] [-- <extra args for run_agent_fifo.sh>]

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
  --rl_exploration_epsilon N
  --rl_gamma N
  --rl_alpha N
  --state_scale_compaction_bytes N
  --state_scale_flush_bytes N
  --state_scale_write_in_bps N
  --state_scale_delay_bps N
  --state_scale_p99_us N
  --state_scale_l0 N
  --state_clip N
  --no-clear-db
  --no-copy-db-log
  -h, --help
USAGE
}

clear_db() {
  if [[ -z "$DB_PATH" || "$DB_PATH" == "/" || "$DB_PATH" == "." ]]; then
    echo "[lq] skip db cleanup: unsafe DB_PATH=$DB_PATH" >&2
    return 1
  fi
  if [[ ! -d "$DB_PATH" ]]; then
    echo "[lq] skip db cleanup: DB_PATH not found ($DB_PATH)" >&2
    return 0
  fi
  echo "[lq] cleaning DB_PATH=$DB_PATH"
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
    echo "[lq] WARN: DB LOG not found at $src"
    return 0
  fi
  if cp "$src" "$dst" 2>/dev/null; then
    echo "[lq] copied DB LOG -> $dst"
    return 0
  fi
  if [[ -n "$SUDO_CMD" ]] && $SUDO_CMD test -f "$src"; then
    if $SUDO_CMD cp "$src" "$dst"; then
      $SUDO_CMD chown "$(id -u):$(id -g)" "$dst" 2>/dev/null || true
      echo "[lq] copied DB LOG with sudo -> $dst"
      return 0
    fi
  fi
  echo "[lq] WARN: failed to copy DB LOG from $src"
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
    --rl_exploration_epsilon) RL_EXPLORATION_EPSILON="$2"; shift 2;;
    --rl_gamma) RL_GAMMA="$2"; shift 2;;
    --rl_alpha) RL_ALPHA="$2"; shift 2;;
    --state_scale_compaction_bytes) STATE_SCALE_COMPACTION_BYTES="$2"; shift 2;;
    --state_scale_flush_bytes) STATE_SCALE_FLUSH_BYTES="$2"; shift 2;;
    --state_scale_write_in_bps) STATE_SCALE_WRITE_IN_BPS="$2"; shift 2;;
    --state_scale_delay_bps) STATE_SCALE_DELAY_BPS="$2"; shift 2;;
    --state_scale_p99_us) STATE_SCALE_P99_US="$2"; shift 2;;
    --state_scale_l0) STATE_SCALE_L0="$2"; shift 2;;
    --state_clip) STATE_CLIP="$2"; shift 2;;
    --no-clear-db) CLEAR_DB_BEFORE=0; shift 1;;
    --no-copy-db-log) COPY_DB_LOG=0; shift 1;;
    --) shift; EXTRA_AGENT_ARGS+=("$@"); break;;
    -h|--help) usage; exit 0;;
    *) EXTRA_AGENT_ARGS+=("$1"); shift 1;;
  esac
done

mkdir -p "$OUTDIR"

echo "[lq] OUTDIR=$OUTDIR"
echo "[lq] DURATION_SEC=$DURATION_SEC"
echo "[lq] DB_PATH=$DB_PATH"
echo "[lq] OPTIONS_FILE=$OPTIONS_FILE"
echo "[lq] RL_POLLER_BIN=$RL_POLLER_BIN"
echo "[lq] M_MIN=$M_MIN M_MAX=$M_MAX"
echo "[lq] RL_EPS=$RL_EXPLORATION_EPSILON RL_GAMMA=$RL_GAMMA RL_ALPHA=$RL_ALPHA"

if [[ "$CLEAR_DB_BEFORE" -eq 1 ]]; then
  clear_db
fi

"$SCRIPT_DIR/run_agent_fifo.sh" \
  --outdir "$OUTDIR" \
  --db_path "$DB_PATH" \
  --options_file "$OPTIONS_FILE" \
  --rl_poller_bin "$RL_POLLER_BIN" \
  --write_mb_per_sec "$WRITE_MB_PER_SEC" \
  --value_size "$VALUE_SIZE" \
  --duration-sec "$DURATION_SEC" \
  --m_min "$M_MIN" \
  --m_max "$M_MAX" \
  --recover_controller_mode RL_DELTA_M \
  --soft_guard_enabled \
  --rl_exploration_epsilon "$RL_EXPLORATION_EPSILON" \
  --rl_gamma "$RL_GAMMA" \
  --rl_alpha "$RL_ALPHA" \
  --state_scale_compaction_bytes "$STATE_SCALE_COMPACTION_BYTES" \
  --state_scale_flush_bytes "$STATE_SCALE_FLUSH_BYTES" \
  --state_scale_write_in_bps "$STATE_SCALE_WRITE_IN_BPS" \
  --state_scale_delay_bps "$STATE_SCALE_DELAY_BPS" \
  --state_scale_p99_us "$STATE_SCALE_P99_US" \
  --state_scale_l0 "$STATE_SCALE_L0" \
  --state_clip "$STATE_CLIP" \
  --gamma_perf "$GAMMA_M" \
  --risk_backlog_eps "$RISK_BACKLOG_EPS" \
  --risk_latency_eps "$RISK_LATENCY_EPS" \
  --soft_guard_cap_value "$SOFT_GUARD_CAP_VALUE" \
  --recover_free_sec "$RECOVER_FREE_SEC" \
  --delta_actions "$DELTA_ACTIONS" \
  --delta_m_max "$DELTA_M_MAX" \
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

echo "[lq] done"
echo "[lq] agent_log: $OUTDIR/agent_log.csv"
echo "[lq] agent_stdout: $OUTDIR/agent_rl_fifo.log"
echo "[lq] poller_log: $OUTDIR/poller.log"
echo "[lq] metrics: $OUTDIR/rl_metrics.csv"
echo "[lq] resource: $OUTDIR/resource_usage.csv"
echo "[lq] db_LOG: $OUTDIR/db_LOG"
