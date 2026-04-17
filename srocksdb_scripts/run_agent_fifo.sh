#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

DB_PATH=${DB_PATH:-}
OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_a.ini"}
RL_POLLER_BIN=${RL_POLLER_BIN:-"$REPO_ROOT/rl_poller"}
AGENT_PY=${AGENT_PY:-"$REPO_ROOT/srocksdb_src/agent_rl_fifo.py"}
PYTHON_BIN=${PYTHON_BIN:-python3}
WAL_DIR=${WAL_DIR:-}

WRITE_MB_PER_SEC=${WRITE_MB_PER_SEC:-500}
VALUE_SIZE=${VALUE_SIZE:-1024}
KEY_PREFIX=${KEY_PREFIX:-k}
FIXED_KEY_16=${FIXED_KEY_16:-1}
YCSB_WORKLOAD=${YCSB_WORKLOAD:-}
YCSB_RECORD_COUNT=${YCSB_RECORD_COUNT:-100000}
YCSB_OPERATION_COUNT=${YCSB_OPERATION_COUNT:-100000}
YCSB_DURATION_SEC=${YCSB_DURATION_SEC:-0}
YCSB_UNIFORM_DISTRIBUTION=${YCSB_UNIFORM_DISTRIBUTION:-0}
YCSB_SCAN_MAX_LEN=${YCSB_SCAN_MAX_LEN:-100}
STOP_AGENT_ON_POLLER_EXIT=${STOP_AGENT_ON_POLLER_EXIT:-0}

BASE_RATE_BPS=${BASE_RATE_BPS:-500000000}
DELTA_MAX=${DELTA_MAX:-0.5}
STEP_SEC=${STEP_SEC:-1.0}
M_MIN=${M_MIN:-0.01}
M_MAX=${M_MAX:-0.2}
HOLD_SEC=${HOLD_SEC:-10}
RECOVER_FREE_SEC=${RECOVER_FREE_SEC:-20}
RECOVER_STEP_SEC=${RECOVER_STEP_SEC:-5}
LADDER=${LADDER:-"0.01,0.02,0.04,0.06,0.10,0.20,0.40,0.60,0.80,1.00"}
RECOVER_CONTROLLER_MODE=${RECOVER_CONTROLLER_MODE:-RL_DELTA_M}
RL_ACTION_MODE=${RL_ACTION_MODE:-hold}
RL_ACTION_FILE=${RL_ACTION_FILE:-}

DELTA_ACTIONS=${DELTA_ACTIONS:-"-0.03,-0.02,-0.01,0,0.01,0.02,0.03"}
DELTA_M_MAX=${DELTA_M_MAX:-0.03}
SEMI_SAFE_STEP=${SEMI_SAFE_STEP:-0.03}
SEMI_SAFE_FLOOR=${SEMI_SAFE_FLOOR:-0.02}
DELTA_SMOOTH_ETA=${DELTA_SMOOTH_ETA:-0.2}
AIMD_AI_STEP=${AIMD_AI_STEP:-0.005}
AIMD_MD_BETA=${AIMD_MD_BETA:-0.7}
STARTUP_FORCE_SEC=${STARTUP_FORCE_SEC:-10}

RL_ONLINE_LEARNING_ENABLED=${RL_ONLINE_LEARNING_ENABLED:-0}
RL_ONLINE_UPDATE_INTERVAL_STEPS=${RL_ONLINE_UPDATE_INTERVAL_STEPS:-200}
RL_ONLINE_MIN_BUFFER_STEPS=${RL_ONLINE_MIN_BUFFER_STEPS:-1000}
RL_EXPLORATION_EPSILON=${RL_EXPLORATION_EPSILON:-0.0}
RL_GAMMA=${RL_GAMMA:-0.95}
RL_ALPHA=${RL_ALPHA:-0.05}
RL_LEARNING_RATE=${RL_LEARNING_RATE:-0.0001}
RL_MAX_GRAD_NORM=${RL_MAX_GRAD_NORM:-0.5}
RL_CKPT_DIR=${RL_CKPT_DIR:-}
RL_CKPT_EVERY_UPDATES=${RL_CKPT_EVERY_UPDATES:-10}
RL_ROLLBACK_ON_STALL=${RL_ROLLBACK_ON_STALL:-0}
RL_ROLLBACK_WINDOW_SEC=${RL_ROLLBACK_WINDOW_SEC:-300}
STATE_SCALE_COMPACTION_BYTES=${STATE_SCALE_COMPACTION_BYTES:-1000000000}
STATE_SCALE_FLUSH_BYTES=${STATE_SCALE_FLUSH_BYTES:-100000000}
STATE_SCALE_WRITE_IN_BPS=${STATE_SCALE_WRITE_IN_BPS:-100000000}
STATE_SCALE_DELAY_BPS=${STATE_SCALE_DELAY_BPS:-100000000}
STATE_SCALE_P99_US=${STATE_SCALE_P99_US:-20000.0}
STATE_SCALE_L0=${STATE_SCALE_L0:-64.0}
STATE_CLIP=${STATE_CLIP:-5.0}

STALL_PENALTY_C=${STALL_PENALTY_C:-200}
GAMMA_PERF=${GAMMA_PERF:-0.1}
RISK_BACKLOG_EPS=${RISK_BACKLOG_EPS:-0.2}
RISK_LATENCY_EPS=${RISK_LATENCY_EPS:-0.2}
RISK_BACKLOG_REF_BYTES=${RISK_BACKLOG_REF_BYTES:-1000000000}
RISK_LATENCY_REF_US=${RISK_LATENCY_REF_US:-1000}

SOFT_GUARD_ENABLED=${SOFT_GUARD_ENABLED:-1}
SOFT_GUARD_CAP_VALUE=${SOFT_GUARD_CAP_VALUE:-0.06}
SOFT_GUARD_REQUIRES_SAFE_MODE0=${SOFT_GUARD_REQUIRES_SAFE_MODE0:-1}

NEAR_STALL_BACKLOG_HI_BYTES=${NEAR_STALL_BACKLOG_HI_BYTES:-20000000000}
NEAR_STALL_BACKLOG_RISE_WINDOW_SEC=${NEAR_STALL_BACKLOG_RISE_WINDOW_SEC:-1800}
NEAR_STALL_BACKLOG_RISE_HI_BYTES=${NEAR_STALL_BACKLOG_RISE_HI_BYTES:-5000000000}
NEAR_STALL_L0_TH=${NEAR_STALL_L0_TH:-12}
NEAR_STALL_L0_BACKLOG_MIN_BYTES=${NEAR_STALL_L0_BACKLOG_MIN_BYTES:-24000000000}
NEAR_STALL_L0_FORCE_TH=${NEAR_STALL_L0_FORCE_TH:-15}
NEAR_STALL_TRIGGER_CONSECUTIVE_SEC=${NEAR_STALL_TRIGGER_CONSECUTIVE_SEC:-3}

SEMI_SAFE_MIN_ON_SEC=${SEMI_SAFE_MIN_ON_SEC:-900}
SEMI_SAFE_RELEASE_BACKLOG_LO_BYTES=${SEMI_SAFE_RELEASE_BACKLOG_LO_BYTES:-16000000000}
SEMI_SAFE_RELEASE_BACKLOG_RISE_WINDOW_SEC=${SEMI_SAFE_RELEASE_BACKLOG_RISE_WINDOW_SEC:-1800}
SEMI_SAFE_RELEASE_BACKLOG_RISE_MAX_BYTES=${SEMI_SAFE_RELEASE_BACKLOG_RISE_MAX_BYTES:-500000000}
SEMI_SAFE_RELEASE_L0_TH=${SEMI_SAFE_RELEASE_L0_TH:-10}
SEMI_SAFE_RELEASE_HOLD_SEC=${SEMI_SAFE_RELEASE_HOLD_SEC:-3}
SEMI_SAFE_COOLDOWN_SEC=${SEMI_SAFE_COOLDOWN_SEC:-10}

HARD_LOCK_BACKLOG_BYTES=${HARD_LOCK_BACKLOG_BYTES:-60000000000}
HARD_LOCK_RELEASE_BACKLOG_BYTES=${HARD_LOCK_RELEASE_BACKLOG_BYTES:-36000000000}
HARD_LOCK_RELEASE_HOLD_SEC=${HARD_LOCK_RELEASE_HOLD_SEC:-900}

TIMEOUT_SEC=${TIMEOUT_SEC:-1800}
OUTDIR=${OUTDIR:-"$REPO_ROOT/srocksdb_evaluation/rl_fifo_run_$(date +%m%d_%H%M%S)"}

usage() {
  cat <<'USAGE'
Usage: ./run_agent_fifo.sh [options]

Options:
  --outdir PATH
  --db_path PATH
  --options_file PATH
  --rl_poller_bin PATH
  --agent_py PATH
  --python_bin PATH
  --wal_dir PATH
  --write_mb_per_sec N
  --value_size N
  --key_prefix STR
  --fixed_key_16 0|1
  --stop_agent_on_poller_exit
  --ycsb_workload STR
  --ycsb_record_count N
  --ycsb_operation_count N
  --ycsb_duration_sec N
  --ycsb_uniform_distribution N
  --ycsb_scan_max_len N
  --base_rate_bps N
  --delta_max N
  --step_sec N
  --m_min N
  --m_max N
  --hold_sec N
  --recover_free_sec N
  --recover_step_sec N
  --ladder STR
  --recover_controller_mode STR
  --rl_action_mode STR
  --rl_action_file PATH
  --delta_actions STR
  --delta_m_max N
  --semi_safe_step N
  --semi_safe_floor N
  --delta_smooth_eta N
  --aimd_ai_step N
  --aimd_md_beta N
  --startup_force_sec N
  --rl_online_learning_enabled
  --rl_online_update_interval_steps N
  --rl_online_min_buffer_steps N
  --rl_exploration_epsilon N
  --rl_gamma N
  --rl_alpha N
  --rl_learning_rate N
  --rl_max_grad_norm N
  --rl_ckpt_dir PATH
  --rl_ckpt_every_updates N
  --rl_rollback_on_stall
  --rl_rollback_window_sec N
  --state_scale_compaction_bytes N
  --state_scale_flush_bytes N
  --state_scale_write_in_bps N
  --state_scale_delay_bps N
  --state_scale_p99_us N
  --state_scale_l0 N
  --state_clip N
  --stall_penalty_C N
  --gamma_perf N
  --risk_backlog_eps N
  --risk_latency_eps N
  --risk_backlog_ref_bytes N
  --risk_latency_ref_us N
  --soft_guard_enabled
  --soft_guard_disabled
  --soft_guard_cap_value N
  --soft_guard_requires_safe_mode0
  --soft_guard_requires_safe_mode_any
  --near_stall_backlog_hi_bytes N
  --near_stall_backlog_rise_window_sec N
  --near_stall_backlog_rise_hi_bytes N
  --near_stall_l0_th N
  --near_stall_l0_backlog_min_bytes N
  --near_stall_l0_force_th N
  --near_stall_trigger_consecutive_sec N
  --semi_safe_min_on_sec N
  --semi_safe_release_backlog_lo_bytes N
  --semi_safe_release_backlog_rise_window_sec N
  --semi_safe_release_backlog_rise_max_bytes N
  --semi_safe_release_l0_th N
  --semi_safe_release_hold_sec N
  --semi_safe_cooldown_sec N
  --hard_lock_backlog_bytes N
  --hard_lock_release_backlog_bytes N
  --hard_lock_release_hold_sec N
  --duration-sec N
  --timeout_sec N
  --fifo_path PATH
  --dry_run
  --verbose
  -h, --help
USAGE
}

DRY_RUN=0
VERBOSE=0
FIFO_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir) OUTDIR="$2"; shift 2;;
    --db_path) DB_PATH="$2"; shift 2;;
    --options_file) OPTIONS_FILE="$2"; shift 2;;
    --rl_poller_bin) RL_POLLER_BIN="$2"; shift 2;;
    --agent_py) AGENT_PY="$2"; shift 2;;
    --python_bin) PYTHON_BIN="$2"; shift 2;;
    --wal_dir) WAL_DIR="$2"; shift 2;;
    --write_mb_per_sec) WRITE_MB_PER_SEC="$2"; shift 2;;
    --value_size) VALUE_SIZE="$2"; shift 2;;
    --key_prefix) KEY_PREFIX="$2"; shift 2;;
    --fixed_key_16) FIXED_KEY_16="$2"; shift 2;;
    --stop_agent_on_poller_exit) STOP_AGENT_ON_POLLER_EXIT=1; shift 1;;
    --ycsb_workload) YCSB_WORKLOAD="$2"; shift 2;;
    --ycsb_record_count) YCSB_RECORD_COUNT="$2"; shift 2;;
    --ycsb_operation_count) YCSB_OPERATION_COUNT="$2"; shift 2;;
    --ycsb_duration_sec) YCSB_DURATION_SEC="$2"; shift 2;;
    --ycsb_uniform_distribution) YCSB_UNIFORM_DISTRIBUTION="$2"; shift 2;;
    --ycsb_scan_max_len) YCSB_SCAN_MAX_LEN="$2"; shift 2;;
    --base_rate_bps) BASE_RATE_BPS="$2"; shift 2;;
    --delta_max) DELTA_MAX="$2"; shift 2;;
    --step_sec) STEP_SEC="$2"; shift 2;;
    --m_min) M_MIN="$2"; shift 2;;
    --m_max) M_MAX="$2"; shift 2;;
    --hold_sec) HOLD_SEC="$2"; shift 2;;
    --recover_free_sec) RECOVER_FREE_SEC="$2"; shift 2;;
    --recover_step_sec) RECOVER_STEP_SEC="$2"; shift 2;;
    --ladder) LADDER="$2"; shift 2;;
    --recover_controller_mode) RECOVER_CONTROLLER_MODE="$2"; shift 2;;
    --rl_action_mode) RL_ACTION_MODE="$2"; shift 2;;
    --rl_action_file) RL_ACTION_FILE="$2"; shift 2;;
    --delta_actions) DELTA_ACTIONS="$2"; shift 2;;
    --delta_m_max) DELTA_M_MAX="$2"; shift 2;;
    --semi_safe_step) SEMI_SAFE_STEP="$2"; shift 2;;
    --semi_safe_floor) SEMI_SAFE_FLOOR="$2"; shift 2;;
    --delta_smooth_eta) DELTA_SMOOTH_ETA="$2"; shift 2;;
    --aimd_ai_step) AIMD_AI_STEP="$2"; shift 2;;
    --aimd_md_beta) AIMD_MD_BETA="$2"; shift 2;;
    --startup_force_sec) STARTUP_FORCE_SEC="$2"; shift 2;;
    --rl_online_learning_enabled) RL_ONLINE_LEARNING_ENABLED=1; shift 1;;
    --rl_online_update_interval_steps) RL_ONLINE_UPDATE_INTERVAL_STEPS="$2"; shift 2;;
    --rl_online_min_buffer_steps) RL_ONLINE_MIN_BUFFER_STEPS="$2"; shift 2;;
    --rl_exploration_epsilon) RL_EXPLORATION_EPSILON="$2"; shift 2;;
    --rl_gamma) RL_GAMMA="$2"; shift 2;;
    --rl_alpha) RL_ALPHA="$2"; shift 2;;
    --rl_learning_rate) RL_LEARNING_RATE="$2"; shift 2;;
    --rl_max_grad_norm) RL_MAX_GRAD_NORM="$2"; shift 2;;
    --rl_ckpt_dir) RL_CKPT_DIR="$2"; shift 2;;
    --rl_ckpt_every_updates) RL_CKPT_EVERY_UPDATES="$2"; shift 2;;
    --rl_rollback_on_stall) RL_ROLLBACK_ON_STALL=1; shift 1;;
    --rl_rollback_window_sec) RL_ROLLBACK_WINDOW_SEC="$2"; shift 2;;
    --state_scale_compaction_bytes) STATE_SCALE_COMPACTION_BYTES="$2"; shift 2;;
    --state_scale_flush_bytes) STATE_SCALE_FLUSH_BYTES="$2"; shift 2;;
    --state_scale_write_in_bps) STATE_SCALE_WRITE_IN_BPS="$2"; shift 2;;
    --state_scale_delay_bps) STATE_SCALE_DELAY_BPS="$2"; shift 2;;
    --state_scale_p99_us) STATE_SCALE_P99_US="$2"; shift 2;;
    --state_scale_l0) STATE_SCALE_L0="$2"; shift 2;;
    --state_clip) STATE_CLIP="$2"; shift 2;;
    --stall_penalty_C) STALL_PENALTY_C="$2"; shift 2;;
    --gamma_perf) GAMMA_PERF="$2"; shift 2;;
    --risk_backlog_eps) RISK_BACKLOG_EPS="$2"; shift 2;;
    --risk_latency_eps) RISK_LATENCY_EPS="$2"; shift 2;;
    --risk_backlog_ref_bytes) RISK_BACKLOG_REF_BYTES="$2"; shift 2;;
    --risk_latency_ref_us) RISK_LATENCY_REF_US="$2"; shift 2;;
    --soft_guard_enabled) SOFT_GUARD_ENABLED=1; shift 1;;
    --soft_guard_disabled) SOFT_GUARD_ENABLED=0; shift 1;;
    --soft_guard_cap_value) SOFT_GUARD_CAP_VALUE="$2"; shift 2;;
    --soft_guard_requires_safe_mode0) SOFT_GUARD_REQUIRES_SAFE_MODE0=1; shift 1;;
    --soft_guard_requires_safe_mode_any) SOFT_GUARD_REQUIRES_SAFE_MODE0=0; shift 1;;
    --near_stall_backlog_hi_bytes) NEAR_STALL_BACKLOG_HI_BYTES="$2"; shift 2;;
    --near_stall_backlog_rise_window_sec) NEAR_STALL_BACKLOG_RISE_WINDOW_SEC="$2"; shift 2;;
    --near_stall_backlog_rise_hi_bytes) NEAR_STALL_BACKLOG_RISE_HI_BYTES="$2"; shift 2;;
    --near_stall_l0_th) NEAR_STALL_L0_TH="$2"; shift 2;;
    --near_stall_l0_backlog_min_bytes) NEAR_STALL_L0_BACKLOG_MIN_BYTES="$2"; shift 2;;
    --near_stall_l0_force_th) NEAR_STALL_L0_FORCE_TH="$2"; shift 2;;
    --near_stall_trigger_consecutive_sec) NEAR_STALL_TRIGGER_CONSECUTIVE_SEC="$2"; shift 2;;
    --semi_safe_min_on_sec) SEMI_SAFE_MIN_ON_SEC="$2"; shift 2;;
    --semi_safe_release_backlog_lo_bytes) SEMI_SAFE_RELEASE_BACKLOG_LO_BYTES="$2"; shift 2;;
    --semi_safe_release_backlog_rise_window_sec) SEMI_SAFE_RELEASE_BACKLOG_RISE_WINDOW_SEC="$2"; shift 2;;
    --semi_safe_release_backlog_rise_max_bytes) SEMI_SAFE_RELEASE_BACKLOG_RISE_MAX_BYTES="$2"; shift 2;;
    --semi_safe_release_l0_th) SEMI_SAFE_RELEASE_L0_TH="$2"; shift 2;;
    --semi_safe_release_hold_sec) SEMI_SAFE_RELEASE_HOLD_SEC="$2"; shift 2;;
    --semi_safe_cooldown_sec) SEMI_SAFE_COOLDOWN_SEC="$2"; shift 2;;
    --hard_lock_backlog_bytes) HARD_LOCK_BACKLOG_BYTES="$2"; shift 2;;
    --hard_lock_release_backlog_bytes) HARD_LOCK_RELEASE_BACKLOG_BYTES="$2"; shift 2;;
    --hard_lock_release_hold_sec) HARD_LOCK_RELEASE_HOLD_SEC="$2"; shift 2;;
    --duration-sec) TIMEOUT_SEC="$2"; shift 2;;
    --timeout_sec) TIMEOUT_SEC="$2"; shift 2;;
    --fifo_path) FIFO_PATH="$2"; shift 2;;
    --dry_run) DRY_RUN=1; shift 1;;
    --verbose) VERBOSE=1; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "ERROR: unknown arg: $1"; usage; exit 2;;
  esac
done

METRICS_CSV="$OUTDIR/rl_metrics.csv"
POLLER_LOG="$OUTDIR/poller.log"
AGENT_LOG="$OUTDIR/agent_log.csv"
AGENT_STDOUT="$OUTDIR/agent_rl_fifo.log"
RESOURCE_CSV="$OUTDIR/resource_usage.csv"
if [[ -z "$FIFO_PATH" ]]; then
  FIFO_PATH="/tmp/rl_poller_cmd.fifo"
fi
if [[ -z "$WAL_DIR" ]]; then
  WAL_DIR="$DB_PATH/wal_dir"
fi

ensure_parent_dir(){ mkdir -p "$(dirname "$1")"; }

stop_poller() {
  if [[ -z "${POLLER_PID:-}" ]] || ! kill -0 "$POLLER_PID" 2>/dev/null; then
    return 0
  fi
  if [[ -p "$FIFO_PATH" ]]; then
    timeout 1s bash -lc "printf 'q\n' > '$FIFO_PATH'" >/dev/null 2>&1 || true
  fi
  local deadline=$((SECONDS + 2))
  while kill -0 "$POLLER_PID" 2>/dev/null; do
    if [[ $SECONDS -ge $deadline ]]; then
      break
    fi
    sleep 0.1
  done
  if kill -0 "$POLLER_PID" 2>/dev/null; then
    kill "$POLLER_PID" 2>/dev/null || true
    sleep 1
  fi
  if kill -0 "$POLLER_PID" 2>/dev/null; then
    kill -9 "$POLLER_PID" 2>/dev/null || true
  fi
  wait "$POLLER_PID" 2>/dev/null || true
}

cleanup() {
  set +e
  echo "[cleanup] stopping agent/poller..."
  if [[ -n "${MONITOR_PID:-}" ]]; then
    kill "$MONITOR_PID" 2>/dev/null || true
    wait "$MONITOR_PID" 2>/dev/null || true
  fi
  if [[ -n "${AGENT_PID:-}" ]]; then
    kill "$AGENT_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
  fi
  stop_poller
}
trap cleanup EXIT

sample_pid_stats() {
  local pid="$1"
  local cpu_var="$2"
  local rss_var="$3"
  local vsz_var="$4"
  local cpu="NaN"
  local rss="0"
  local vsz="0"
  if kill -0 "$pid" 2>/dev/null; then
    local line
    line=$(LC_ALL=C ps -p "$pid" -o %cpu= -o rss= -o vsz= 2>/dev/null | awk 'NF {print $1","$2","$3; exit}')
    if [[ -n "${line:-}" ]]; then
      IFS=',' read -r cpu rss vsz <<<"$line"
    fi
  fi
  printf -v "$cpu_var" '%s' "$cpu"
  printf -v "$rss_var" '%s' "$rss"
  printf -v "$vsz_var" '%s' "$vsz"
}

start_resource_monitor() {
  : > "$RESOURCE_CSV"
  chmod 666 "$RESOURCE_CSV" || true
  echo "ts,poller_cpu_pct,poller_rss_kb,poller_vsz_kb,agent_cpu_pct,agent_rss_kb,agent_vsz_kb,mem_total_kb,mem_available_kb,mem_used_kb" > "$RESOURCE_CSV"
  (
    while true; do
      local ts
      ts=$(date '+%Y-%m-%d %H:%M:%S')
      local poller_cpu poller_rss poller_vsz
      local agent_cpu agent_rss agent_vsz
      sample_pid_stats "$POLLER_PID" poller_cpu poller_rss poller_vsz
      sample_pid_stats "$AGENT_PID" agent_cpu agent_rss agent_vsz

      local mem_total=0
      local mem_available=0
      if [[ -r /proc/meminfo ]]; then
        local mem_line
        mem_line=$(awk '/^MemTotal:/{t=$2} /^MemAvailable:/{a=$2} END{if(t=="") t=0; if(a=="") a=0; print t","a}' /proc/meminfo)
        IFS=',' read -r mem_total mem_available <<<"$mem_line"
      fi
      local mem_used=$((mem_total - mem_available))
      if (( mem_used < 0 )); then
        mem_used=0
      fi

      echo "$ts,$poller_cpu,$poller_rss,$poller_vsz,$agent_cpu,$agent_rss,$agent_vsz,$mem_total,$mem_available,$mem_used" >> "$RESOURCE_CSV"

      if ! kill -0 "$AGENT_PID" 2>/dev/null && ! kill -0 "$POLLER_PID" 2>/dev/null; then
        break
      fi
      sleep 1
    done
  ) &
  MONITOR_PID=$!
}

echo "[fifo] DB_PATH=$DB_PATH"
echo "[fifo] OPTIONS_FILE=$OPTIONS_FILE"
echo "[fifo] AGENT_PY=$AGENT_PY"
echo "[fifo] METRICS_CSV=$METRICS_CSV"
echo "[fifo] POLLER_LOG=$POLLER_LOG"
echo "[fifo] AGENT_LOG=$AGENT_LOG"
echo "[fifo] RESOURCE_CSV=$RESOURCE_CSV"
echo "[fifo] FIFO_PATH=$FIFO_PATH"
echo "[fifo] WAL_DIR=$WAL_DIR"
echo "[fifo] WRITE_MB_PER_SEC=$WRITE_MB_PER_SEC VALUE_SIZE=$VALUE_SIZE KEY_PREFIX=$KEY_PREFIX FIXED_KEY_16=$FIXED_KEY_16"
echo "[fifo] YCSB_WORKLOAD=${YCSB_WORKLOAD:-none} YCSB_RECORD_COUNT=$YCSB_RECORD_COUNT YCSB_OPERATION_COUNT=$YCSB_OPERATION_COUNT YCSB_DURATION_SEC=$YCSB_DURATION_SEC YCSB_UNIFORM_DISTRIBUTION=$YCSB_UNIFORM_DISTRIBUTION YCSB_SCAN_MAX_LEN=$YCSB_SCAN_MAX_LEN"
echo "[fifo] TIMEOUT_SEC=$TIMEOUT_SEC STEP_SEC=$STEP_SEC"
echo "[fifo] DELTA_MAX=$DELTA_MAX"
echo "[fifo] M_MIN=$M_MIN M_MAX=$M_MAX"
echo "[fifo] HOLD_SEC=$HOLD_SEC RECOVER_FREE_SEC=$RECOVER_FREE_SEC RECOVER_STEP_SEC=$RECOVER_STEP_SEC"
echo "[fifo] LADDER=$LADDER"
echo "[fifo] RECOVER_CONTROLLER_MODE=$RECOVER_CONTROLLER_MODE RL_ACTION_MODE=$RL_ACTION_MODE RL_ACTION_FILE=$RL_ACTION_FILE"
echo "[fifo] DELTA_ACTIONS=$DELTA_ACTIONS DELTA_M_MAX=$DELTA_M_MAX SEMI_SAFE_STEP=$SEMI_SAFE_STEP SEMI_SAFE_FLOOR=$SEMI_SAFE_FLOOR DELTA_SMOOTH_ETA=$DELTA_SMOOTH_ETA"
echo "[fifo] AIMD_AI_STEP=$AIMD_AI_STEP AIMD_MD_BETA=$AIMD_MD_BETA"
echo "[fifo] STARTUP_FORCE_SEC=$STARTUP_FORCE_SEC"
echo "[fifo] RL_ONLINE_LEARNING_ENABLED=$RL_ONLINE_LEARNING_ENABLED RL_EXPLORATION_EPSILON=$RL_EXPLORATION_EPSILON RL_LR=$RL_LEARNING_RATE"
echo "[fifo] RISK_BACKLOG_EPS=$RISK_BACKLOG_EPS RISK_LATENCY_EPS=$RISK_LATENCY_EPS"
echo "[fifo] OUTDIR=$OUTDIR"

command -v "$PYTHON_BIN" >/dev/null 2>&1 || { echo "ERROR: $PYTHON_BIN not found"; exit 1; }
[[ -f "$OPTIONS_FILE" ]] || { echo "ERROR: options file not found: $OPTIONS_FILE"; exit 1; }
[[ -f "$AGENT_PY" ]] || { echo "ERROR: agent file not found: $AGENT_PY"; exit 1; }
[[ -d "$DB_PATH" ]] || { echo "ERROR: DB_PATH not found/dir: $DB_PATH"; exit 1; }

if [[ ! -x "$RL_POLLER_BIN" ]]; then
  if [[ -f "$REPO_ROOT/Makefile" ]]; then
    echo "[build] rl_poller not found; building..."
    (cd "$REPO_ROOT" && make -j8 rl_poller)
  fi
fi
[[ -x "$RL_POLLER_BIN" ]] || { echo "ERROR: rl_poller binary not executable: $RL_POLLER_BIN"; exit 1; }

ensure_parent_dir "$METRICS_CSV"
ensure_parent_dir "$POLLER_LOG"
ensure_parent_dir "$AGENT_LOG"
ensure_parent_dir "$FIFO_PATH"
mkdir -p "$WAL_DIR"
rm -f "$WAL_DIR"/*.log "$WAL_DIR"/LOG.old.* 2>/dev/null || true

: > "$METRICS_CSV"
chmod 666 "$METRICS_CSV" || true
: > "$AGENT_LOG"
chmod 666 "$AGENT_LOG" || true
: > "$POLLER_LOG"
chmod 666 "$POLLER_LOG" || true

if [[ -e "$FIFO_PATH" || -L "$FIFO_PATH" ]]; then
  rm -f "$FIFO_PATH"
fi
mkfifo "$FIFO_PATH"
chmod 666 "$FIFO_PATH" || true

TMP_OPT=$(mktemp /tmp/rl_options.XXXXXX.ini)
cp "$OPTIONS_FILE" "$TMP_OPT"
if ! grep -q '^\[DBOptions\]' "$TMP_OPT"; then
  printf '\n[DBOptions]\n' >> "$TMP_OPT"
fi

upsert_opt() {
  local key="$1" val="$2"
  if grep -q "^${key}=" "$TMP_OPT"; then
    sed -i "s|^${key}=.*|${key}=${val}|" "$TMP_OPT"
  else
    awk -v k="$key" -v v="$val" '
      BEGIN{done=0}
      /^\[DBOptions\]/{print; if(!done){print k"="v; done=1; next}}
      {print}
      END{if(!done){print "[DBOptions]"; print k"="v}}
    ' "$TMP_OPT" > "${TMP_OPT}.tmp" && mv "${TMP_OPT}.tmp" "$TMP_OPT"
  fi
}

upsert_opt rl_write_base_rate_bytes_per_sec "$BASE_RATE_BPS"
upsert_opt rl_write_delta_max "$DELTA_MAX"
upsert_opt wal_dir "$WAL_DIR"

echo "[fifo] Using temp options: $TMP_OPT"

POLLER_ARGS=(
  "$DB_PATH" "$TMP_OPT" "$METRICS_CSV"
  --write_mb_per_sec="$WRITE_MB_PER_SEC"
  --value_size="$VALUE_SIZE"
  --key_prefix="$KEY_PREFIX"
  --fixed_key_16="$FIXED_KEY_16"
  --cmd_fifo="$FIFO_PATH"
)
if [[ -n "${YCSB_WORKLOAD}" ]]; then
  POLLER_ARGS+=(
    --ycsb_workload="$YCSB_WORKLOAD"
    --ycsb_record_count="$YCSB_RECORD_COUNT"
    --ycsb_operation_count="$YCSB_OPERATION_COUNT"
    --ycsb_duration_sec="$YCSB_DURATION_SEC"
    --ycsb_uniform_distribution="$YCSB_UNIFORM_DISTRIBUTION"
    --ycsb_scan_max_len="$YCSB_SCAN_MAX_LEN"
  )
fi

"$RL_POLLER_BIN" "${POLLER_ARGS[@]}" >"$POLLER_LOG" 2>&1 &
POLLER_PID=$!

sleep 1
chmod 666 "$METRICS_CSV" || true

AGENT_ARGS=(
  --poller_csv "$METRICS_CSV"
  --fifo "$FIFO_PATH"
  --out_csv "$AGENT_LOG"
  --period_sec "$STEP_SEC"
  --timeout_sec "$TIMEOUT_SEC"
  --m_min "$M_MIN"
  --m_max "$M_MAX"
  --hold_sec "$HOLD_SEC"
  --recover_free_sec "$RECOVER_FREE_SEC"
  --recover_step_sec "$RECOVER_STEP_SEC"
  --ladder "$LADDER"
  --recover_controller_mode "$RECOVER_CONTROLLER_MODE"
  --rl_action_mode "$RL_ACTION_MODE"
  --rl_action_file "$RL_ACTION_FILE"
  --delta_actions "$DELTA_ACTIONS"
  --delta_m_max "$DELTA_M_MAX"
  --semi_safe_step "$SEMI_SAFE_STEP"
  --semi_safe_floor "$SEMI_SAFE_FLOOR"
  --delta_smooth_eta "$DELTA_SMOOTH_ETA"
  --aimd_ai_step "$AIMD_AI_STEP"
  --aimd_md_beta "$AIMD_MD_BETA"
  --startup_force_sec "$STARTUP_FORCE_SEC"
  --rl_online_update_interval_steps "$RL_ONLINE_UPDATE_INTERVAL_STEPS"
  --rl_online_min_buffer_steps "$RL_ONLINE_MIN_BUFFER_STEPS"
  --rl_exploration_epsilon "$RL_EXPLORATION_EPSILON"
  --rl_gamma "$RL_GAMMA"
  --rl_alpha "$RL_ALPHA"
  --rl_learning_rate "$RL_LEARNING_RATE"
  --rl_max_grad_norm "$RL_MAX_GRAD_NORM"
  --rl_ckpt_every_updates "$RL_CKPT_EVERY_UPDATES"
  --rl_rollback_window_sec "$RL_ROLLBACK_WINDOW_SEC"
  --state_scale_compaction_bytes "$STATE_SCALE_COMPACTION_BYTES"
  --state_scale_flush_bytes "$STATE_SCALE_FLUSH_BYTES"
  --state_scale_write_in_bps "$STATE_SCALE_WRITE_IN_BPS"
  --state_scale_delay_bps "$STATE_SCALE_DELAY_BPS"
  --state_scale_p99_us "$STATE_SCALE_P99_US"
  --state_scale_l0 "$STATE_SCALE_L0"
  --state_clip "$STATE_CLIP"
  --stall_penalty_C "$STALL_PENALTY_C"
  --gamma_perf "$GAMMA_PERF"
  --risk_backlog_eps "$RISK_BACKLOG_EPS"
  --risk_latency_eps "$RISK_LATENCY_EPS"
  --risk_backlog_ref_bytes "$RISK_BACKLOG_REF_BYTES"
  --risk_latency_ref_us "$RISK_LATENCY_REF_US"
  --soft_guard_cap_value "$SOFT_GUARD_CAP_VALUE"
  --near_stall_backlog_hi_bytes "$NEAR_STALL_BACKLOG_HI_BYTES"
  --near_stall_backlog_rise_window_sec "$NEAR_STALL_BACKLOG_RISE_WINDOW_SEC"
  --near_stall_backlog_rise_hi_bytes "$NEAR_STALL_BACKLOG_RISE_HI_BYTES"
  --near_stall_l0_th "$NEAR_STALL_L0_TH"
  --near_stall_l0_backlog_min_bytes "$NEAR_STALL_L0_BACKLOG_MIN_BYTES"
  --near_stall_l0_force_th "$NEAR_STALL_L0_FORCE_TH"
  --near_stall_trigger_consecutive_sec "$NEAR_STALL_TRIGGER_CONSECUTIVE_SEC"
  --semi_safe_min_on_sec "$SEMI_SAFE_MIN_ON_SEC"
  --semi_safe_release_backlog_lo_bytes "$SEMI_SAFE_RELEASE_BACKLOG_LO_BYTES"
  --semi_safe_release_backlog_rise_window_sec "$SEMI_SAFE_RELEASE_BACKLOG_RISE_WINDOW_SEC"
  --semi_safe_release_backlog_rise_max_bytes "$SEMI_SAFE_RELEASE_BACKLOG_RISE_MAX_BYTES"
  --semi_safe_release_l0_th "$SEMI_SAFE_RELEASE_L0_TH"
  --semi_safe_release_hold_sec "$SEMI_SAFE_RELEASE_HOLD_SEC"
  --semi_safe_cooldown_sec "$SEMI_SAFE_COOLDOWN_SEC"
  --hard_lock_backlog_bytes "$HARD_LOCK_BACKLOG_BYTES"
  --hard_lock_release_backlog_bytes "$HARD_LOCK_RELEASE_BACKLOG_BYTES"
  --hard_lock_release_hold_sec "$HARD_LOCK_RELEASE_HOLD_SEC"
)
if [[ "$RL_ONLINE_LEARNING_ENABLED" -eq 1 ]]; then
  AGENT_ARGS+=(--rl_online_learning_enabled)
fi
if [[ "$RL_ROLLBACK_ON_STALL" -eq 1 ]]; then
  AGENT_ARGS+=(--rl_rollback_on_stall)
fi
if [[ "${SOFT_GUARD_ENABLED:-1}" -eq 1 ]]; then
  AGENT_ARGS+=(--soft_guard_enabled)
else
  AGENT_ARGS+=(--soft_guard_disabled)
fi
if [[ "${SOFT_GUARD_REQUIRES_SAFE_MODE0:-1}" -eq 1 ]]; then
  AGENT_ARGS+=(--soft_guard_requires_safe_mode0)
else
  AGENT_ARGS+=(--soft_guard_requires_safe_mode_any)
fi
if [[ -n "${RL_CKPT_DIR}" ]]; then
  AGENT_ARGS+=(--rl_ckpt_dir "$RL_CKPT_DIR")
fi
if [[ "$DRY_RUN" -eq 1 ]]; then
  AGENT_ARGS+=(--dry_run)
fi
if [[ "$VERBOSE" -eq 1 ]]; then
  AGENT_ARGS+=(--verbose)
fi

echo "[run] starting agent_rl_fifo..."
PYTHONUNBUFFERED=1 "$PYTHON_BIN" -u "$AGENT_PY" "${AGENT_ARGS[@]}" \
  >"$AGENT_STDOUT" 2>&1 &
AGENT_PID=$!
start_resource_monitor

echo "[run] waiting for agent to finish ($TIMEOUT_SEC s)..."
deadline=$((SECONDS + TIMEOUT_SEC))
while kill -0 "$AGENT_PID" 2>/dev/null; do
  if [[ "$STOP_AGENT_ON_POLLER_EXIT" -eq 1 ]] && ! kill -0 "$POLLER_PID" 2>/dev/null; then
    echo "[run] poller exited; stopping agent (--stop_agent_on_poller_exit)"
    kill "$AGENT_PID" 2>/dev/null || true
    break
  fi
  if [[ $SECONDS -ge $deadline ]]; then
    echo "[run] agent timeout; sending SIGTERM"
    kill "$AGENT_PID" 2>/dev/null || true
    sleep 2
    if kill -0 "$AGENT_PID" 2>/dev/null; then
      echo "[run] agent still alive; sending SIGKILL"
      kill -9 "$AGENT_PID" 2>/dev/null || true
    fi
    break
  fi
  sleep 1
done
wait "$AGENT_PID" 2>/dev/null || true
stop_poller
if [[ -n "${MONITOR_PID:-}" ]]; then
  wait "$MONITOR_PID" 2>/dev/null || true
fi
sleep 2

echo
echo "=== Quick checks ==="
echo "[poller.log fifo lines]"
grep "\\[fifo\\] line=\\\"m" "$POLLER_LOG" || true
echo
if [[ -f "$DB_PATH/LOG" ]]; then
  echo "[RocksDB LOG write_multiplier changed tail]"
  tail -n 200 "$DB_PATH/LOG" | grep "\\[rl\\] write_multiplier changed" || true
fi
