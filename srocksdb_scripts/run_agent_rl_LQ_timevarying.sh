#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
INT32_KEYSPACE_MAX=2147483647

TS=$(date +%m%d_%H%M%S)
OUTDIR=${OUTDIR:-"$REPO_ROOT/srocksdb_evaluation/agent_rl_lq_timevarying_${TS}"}

DB_PATH=${DB_PATH:-/mnt/f2fs/rlrocksdb_log}
OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_r.ini"}
# OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_s.ini"}
RL_POLLER_BIN=${RL_POLLER_BIN:-"$REPO_ROOT/rl_poller"}

LOAD_RECORD_COUNT=${LOAD_RECORD_COUNT:-$INT32_KEYSPACE_MAX}
RUN_RECORD_COUNT=${RUN_RECORD_COUNT:-$INT32_KEYSPACE_MAX}
LOAD_WITH_AGENT=${LOAD_WITH_AGENT:-1}
LOAD_TIMEOUT_SEC=${LOAD_TIMEOUT_SEC:-1800}
LOAD_PHASE_MODE=${LOAD_PHASE_MODE:-duration}

# for each 3 hours
PHASE1_DURATION_SEC=${PHASE1_DURATION_SEC:-10800}
PHASE2_DURATION_SEC=${PHASE2_DURATION_SEC:-10800}
PHASE3_DURATION_SEC=${PHASE3_DURATION_SEC:-10800}

PHASE1_WORKLOAD=${PHASE1_WORKLOAD:-r10w90}
PHASE2_WORKLOAD=${PHASE2_WORKLOAD:-r90w10}
PHASE3_WORKLOAD=${PHASE3_WORKLOAD:-r50w50}

M_MIN=${M_MIN:-0.01}

# M_MAX=${M_MAX:-0.5}
M_MAX=${M_MAX:-0.2}

VALUE_SIZE=${VALUE_SIZE:-1024}
KEY_PREFIX=${KEY_PREFIX:-k}
FIXED_KEY_16=${FIXED_KEY_16:-1}
YCSB_UNIFORM_DISTRIBUTION=${YCSB_UNIFORM_DISTRIBUTION:-0}
YCSB_SCAN_MAX_LEN=${YCSB_SCAN_MAX_LEN:-100}

SUDO_CMD=${SUDO_CMD:-}
CLEAR_DB_BEFORE=${CLEAR_DB_BEFORE:-1}
COPY_DB_LOG=${COPY_DB_LOG:-1}

EXTRA_AGENT_ARGS=()

usage() {
  cat <<'USAGE'
Usage: ./run_agent_rl_LQ_timevarying.sh [options] [-- <extra args for run_agent_rl_LQ.sh>]

Options:
  --outdir PATH
  --db_path PATH
  --options_file PATH
  --rl_poller_bin PATH
  --load_record_count N
  --run_record_count N
  --load_with_agent 0|1
  --load_timeout_sec N
  --load_phase_mode record_count|duration
  --phase1_duration_sec N
  --phase2_duration_sec N
  --phase3_duration_sec N
  --phase1_workload STR
  --phase2_workload STR
  --phase3_workload STR
  --m_min N
  --m_max N
  --value_size N
  --key_prefix STR
  --fixed_key_16 0|1
  --ycsb_uniform_distribution 0|1
  --ycsb_scan_max_len N
  --no-clear-db
  --no-copy-db-log
  -h, --help

Default phases:
  phase0: load_uniform for 1800s over key range [1, INT32_MAX]
  phase1: r10w90  for 10800s over key range [1, INT32_MAX] using zipfian access
  phase2: r90w10  for 10800s over key range [1, INT32_MAX] using zipfian access
  phase3: r50w50  for 10800s over key range [1, INT32_MAX] using zipfian access
USAGE
}

clear_db() {
  if [[ -z "$DB_PATH" || "$DB_PATH" == "/" || "$DB_PATH" == "." ]]; then
    echo "[tv] skip db cleanup: unsafe DB_PATH=$DB_PATH" >&2
    return 1
  fi
  if [[ ! -d "$DB_PATH" ]]; then
    echo "[tv] skip db cleanup: DB_PATH not found ($DB_PATH)" >&2
    return 0
  fi
  echo "[tv] cleaning DB_PATH=$DB_PATH"
  if [[ -n "$SUDO_CMD" ]]; then
    $SUDO_CMD find "$DB_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  else
    find "$DB_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
}

copy_db_log() {
  local label="$1"
  local dst_dir="$2"
  local src="$DB_PATH/LOG"
  local dst="$dst_dir/db_LOG_after_${label}.log"
  if [[ ! -f "$src" ]]; then
    echo "[tv] WARN: DB LOG not found at $src"
    return 0
  fi
  if cp "$src" "$dst" 2>/dev/null; then
    echo "[tv] copied DB LOG -> $dst"
    return 0
  fi
  if [[ -n "$SUDO_CMD" ]] && $SUDO_CMD test -f "$src"; then
    if $SUDO_CMD cp "$src" "$dst"; then
      $SUDO_CMD chown "$(id -u):$(id -g)" "$dst" 2>/dev/null || true
      echo "[tv] copied DB LOG with sudo -> $dst"
      return 0
    fi
  fi
  echo "[tv] WARN: failed to copy DB LOG from $src"
}

run_load_phase() {
  if [[ "$LOAD_PHASE_MODE" == "record_count" && "$LOAD_RECORD_COUNT" -le 0 ]]; then
    echo "[tv] skipping preload phase (LOAD_RECORD_COUNT=$LOAD_RECORD_COUNT)"
    return 0
  fi

  local phase_dir="$OUTDIR/phase0_load"
  mkdir -p "$phase_dir"
  echo "[tv] preload start: mode=$LOAD_PHASE_MODE workload=load_uniform keyspace_max=$LOAD_RECORD_COUNT duration_cap=${LOAD_TIMEOUT_SEC}s"

  if [[ "$LOAD_WITH_AGENT" -eq 1 ]]; then
    "$SCRIPT_DIR/run_agent_rl_LQ.sh" \
    --outdir "$phase_dir" \
    --duration-sec "$LOAD_TIMEOUT_SEC" \
    --db_path "$DB_PATH" \
    --options_file "$OPTIONS_FILE" \
    --m_min "$M_MIN" \
    --m_max "$M_MAX" \
    --write_mb_per_sec 0 \
    --value_size "$VALUE_SIZE" \
    --no-clear-db \
    --no-copy-db-log \
      -- \
      --rl_poller_bin "$RL_POLLER_BIN" \
      --key_prefix "$KEY_PREFIX" \
      --fixed_key_16 "$FIXED_KEY_16" \
      --ycsb_workload load_uniform \
      --ycsb_record_count "$LOAD_RECORD_COUNT" \
      --stop_agent_on_poller_exit \
      $([[ "$LOAD_PHASE_MODE" == "duration" ]] && printf '%s ' --ycsb_duration_sec "$LOAD_TIMEOUT_SEC") \
      $([[ "$LOAD_PHASE_MODE" == "record_count" ]] && printf '%s ' --ycsb_operation_count "$LOAD_RECORD_COUNT") \
      "${EXTRA_AGENT_ARGS[@]}"
  else
    "$RL_POLLER_BIN" "$DB_PATH" "$OPTIONS_FILE" "$phase_dir/rl_metrics.csv" \
      --create_if_missing=1 \
      --value_size="$VALUE_SIZE" \
      --key_prefix="$KEY_PREFIX" \
      --fixed_key_16="$FIXED_KEY_16" \
      --ycsb_workload=load_uniform \
      --ycsb_record_count="$LOAD_RECORD_COUNT" \
      $([[ "$LOAD_PHASE_MODE" == "duration" ]] && printf '%s ' --ycsb_duration_sec="$LOAD_TIMEOUT_SEC") \
      $([[ "$LOAD_PHASE_MODE" == "record_count" ]] && printf '%s ' --ycsb_operation_count="$LOAD_RECORD_COUNT") \
      >"$phase_dir/poller.log" 2>&1
  fi

  if [[ "$COPY_DB_LOG" -eq 1 ]]; then
    copy_db_log "load" "$phase_dir"
  fi
  echo "[tv] preload done"
}

run_phase() {
  local index="$1"
  local label="$2"
  local workload="$3"
  local duration="$4"
  local phase_dir="$OUTDIR/phase${index}_${label}"

  mkdir -p "$phase_dir"
  echo "[tv] phase${index} start: label=$label workload=$workload duration=${duration}s"
  "$SCRIPT_DIR/run_agent_rl_LQ.sh" \
    --outdir "$phase_dir" \
    --duration-sec "$duration" \
    --db_path "$DB_PATH" \
    --options_file "$OPTIONS_FILE" \
    --m_min "$M_MIN" \
    --m_max "$M_MAX" \
    --write_mb_per_sec 0 \
    --value_size "$VALUE_SIZE" \
    --no-clear-db \
    -- \
    --rl_poller_bin "$RL_POLLER_BIN" \
    --key_prefix "$KEY_PREFIX" \
    --fixed_key_16 "$FIXED_KEY_16" \
    --stop_agent_on_poller_exit \
    --ycsb_workload "$workload" \
    --ycsb_record_count "$RUN_RECORD_COUNT" \
    --ycsb_duration_sec "$duration" \
    --ycsb_uniform_distribution "$YCSB_UNIFORM_DISTRIBUTION" \
    --ycsb_scan_max_len "$YCSB_SCAN_MAX_LEN" \
    "${EXTRA_AGENT_ARGS[@]}"

  if [[ "$COPY_DB_LOG" -eq 1 ]]; then
    copy_db_log "$label" "$phase_dir"
  fi
  echo "[tv] phase${index} done: label=$label"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir) OUTDIR="$2"; shift 2;;
    --db_path) DB_PATH="$2"; shift 2;;
    --options_file) OPTIONS_FILE="$2"; shift 2;;
    --rl_poller_bin) RL_POLLER_BIN="$2"; shift 2;;
    --load_record_count) LOAD_RECORD_COUNT="$2"; shift 2;;
    --run_record_count) RUN_RECORD_COUNT="$2"; shift 2;;
    --load_with_agent) LOAD_WITH_AGENT="$2"; shift 2;;
    --load_timeout_sec) LOAD_TIMEOUT_SEC="$2"; shift 2;;
    --load_phase_mode) LOAD_PHASE_MODE="$2"; shift 2;;
    --phase1_duration_sec) PHASE1_DURATION_SEC="$2"; shift 2;;
    --phase2_duration_sec) PHASE2_DURATION_SEC="$2"; shift 2;;
    --phase3_duration_sec) PHASE3_DURATION_SEC="$2"; shift 2;;
    --phase1_workload) PHASE1_WORKLOAD="$2"; shift 2;;
    --phase2_workload) PHASE2_WORKLOAD="$2"; shift 2;;
    --phase3_workload) PHASE3_WORKLOAD="$2"; shift 2;;
    --m_min) M_MIN="$2"; shift 2;;
    --m_max) M_MAX="$2"; shift 2;;
    --value_size) VALUE_SIZE="$2"; shift 2;;
    --key_prefix) KEY_PREFIX="$2"; shift 2;;
    --fixed_key_16) FIXED_KEY_16="$2"; shift 2;;
    --ycsb_uniform_distribution) YCSB_UNIFORM_DISTRIBUTION="$2"; shift 2;;
    --ycsb_scan_max_len) YCSB_SCAN_MAX_LEN="$2"; shift 2;;
    --no-clear-db) CLEAR_DB_BEFORE=0; shift 1;;
    --no-copy-db-log) COPY_DB_LOG=0; shift 1;;
    --) shift; EXTRA_AGENT_ARGS+=("$@"); break;;
    -h|--help) usage; exit 0;;
    *) EXTRA_AGENT_ARGS+=("$1"); shift 1;;
  esac
done

mkdir -p "$OUTDIR"

echo "[tv] OUTDIR=$OUTDIR"
echo "[tv] DB_PATH=$DB_PATH"
echo "[tv] OPTIONS_FILE=$OPTIONS_FILE"
echo "[tv] RL_POLLER_BIN=$RL_POLLER_BIN"
echo "[tv] LOAD_RECORD_COUNT=$LOAD_RECORD_COUNT RUN_RECORD_COUNT=$RUN_RECORD_COUNT LOAD_WITH_AGENT=$LOAD_WITH_AGENT LOAD_TIMEOUT_SEC=$LOAD_TIMEOUT_SEC LOAD_PHASE_MODE=$LOAD_PHASE_MODE"
echo "[tv] PHASE1=$PHASE1_WORKLOAD/${PHASE1_DURATION_SEC}s PHASE2=$PHASE2_WORKLOAD/${PHASE2_DURATION_SEC}s PHASE3=$PHASE3_WORKLOAD/${PHASE3_DURATION_SEC}s"
echo "[tv] M_MIN=$M_MIN M_MAX=$M_MAX"
echo "[tv] VALUE_SIZE=$VALUE_SIZE KEY_PREFIX=$KEY_PREFIX FIXED_KEY_16=$FIXED_KEY_16"

if [[ "$LOAD_WITH_AGENT" != "0" && "$LOAD_WITH_AGENT" != "1" ]]; then
  echo "ERROR: --load_with_agent must be 0 or 1 (got: $LOAD_WITH_AGENT)" >&2
  exit 2
fi
if [[ "$LOAD_PHASE_MODE" != "record_count" && "$LOAD_PHASE_MODE" != "duration" ]]; then
  echo "ERROR: --load_phase_mode must be record_count or duration (got: $LOAD_PHASE_MODE)" >&2
  exit 2
fi
if [[ "$LOAD_TIMEOUT_SEC" -le 0 ]]; then
  echo "ERROR: --load_timeout_sec must be > 0 (got: $LOAD_TIMEOUT_SEC)" >&2
  exit 2
fi
if [[ "$RUN_RECORD_COUNT" -le 0 ]]; then
  echo "ERROR: --run_record_count must be > 0 (got: $RUN_RECORD_COUNT)" >&2
  exit 2
fi
if [[ "$PHASE1_DURATION_SEC" -le 0 || "$PHASE2_DURATION_SEC" -le 0 || "$PHASE3_DURATION_SEC" -le 0 ]]; then
  echo "ERROR: all phase durations must be > 0" >&2
  exit 2
fi

[[ -f "$OPTIONS_FILE" ]] || { echo "ERROR: options file not found: $OPTIONS_FILE"; exit 1; }
[[ -d "$DB_PATH" ]] || { echo "ERROR: DB_PATH not found/dir: $DB_PATH"; exit 1; }

if [[ ! -x "$RL_POLLER_BIN" ]]; then
  if [[ -f "$REPO_ROOT/Makefile" ]]; then
    echo "[tv] rl_poller not found; building..."
    (cd "$REPO_ROOT" && make -j8 rl_poller)
  fi
fi
[[ -x "$RL_POLLER_BIN" ]] || { echo "ERROR: rl_poller binary not executable: $RL_POLLER_BIN"; exit 1; }

cat >"$OUTDIR/phase_plan.tsv" <<EOF
phase	label	workload	duration_sec
0	load	load_uniform:${LOAD_PHASE_MODE}	${LOAD_TIMEOUT_SEC}
1	write_heavy	${PHASE1_WORKLOAD}	${PHASE1_DURATION_SEC}
2	read_heavy	${PHASE2_WORKLOAD}	${PHASE2_DURATION_SEC}
3	mixed	${PHASE3_WORKLOAD}	${PHASE3_DURATION_SEC}
EOF

if [[ "$CLEAR_DB_BEFORE" -eq 1 ]]; then
  clear_db
fi

run_load_phase
run_phase 1 "write_heavy" "$PHASE1_WORKLOAD" "$PHASE1_DURATION_SEC"
run_phase 2 "read_heavy" "$PHASE2_WORKLOAD" "$PHASE2_DURATION_SEC"
run_phase 3 "mixed" "$PHASE3_WORKLOAD" "$PHASE3_DURATION_SEC"

echo "[tv] all phases completed"
echo "[tv] results root: $OUTDIR"
