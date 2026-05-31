#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

TS=$(date +%m%d_%H%M%S)
OUTDIR=${OUTDIR:-"$REPO_ROOT/srocksdb_evaluation/ycsb_${TS}"}
DB_PATH=${DB_PATH:-/mnt/f2fs/rlrocksdb_log}
OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_s.ini"}
RL_POLLER_BIN=${RL_POLLER_BIN:-"$REPO_ROOT/rl_poller"}

LOAD_RECORD_COUNT=${LOAD_RECORD_COUNT:-50000000}
RUN_DURATION_SEC=${RUN_DURATION_SEC:-3600} # 1 hour   
# RUN_DURATION_SEC=${RUN_DURATION_SEC:-43200} # 12 hours
LOAD_WITH_AGENT=${LOAD_WITH_AGENT:-1}
LOAD_TIMEOUT_SEC=${LOAD_TIMEOUT_SEC:-43200}
VALUE_SIZE=${VALUE_SIZE:-1024}
KEY_PREFIX=${KEY_PREFIX:-k}
FIXED_KEY_16=${FIXED_KEY_16:-1}
YCSB_SCAN_MAX_LEN=${YCSB_SCAN_MAX_LEN:-100}
YCSB_UNIFORM_DISTRIBUTION=${YCSB_UNIFORM_DISTRIBUTION:-0}

SUDO_CMD=${SUDO_CMD:-}

usage() {
  cat <<'USAGE'
Usage: ./run_agent_rl_ycsb.sh [options]

Options:
  --outdir PATH
  --db_path PATH
  --options_file PATH
  --rl_poller_bin PATH
  --load_record_count N         (default: 50000000)
  --run_duration_sec N          (default: 3600)
  --load_with_agent 0|1         (default: 1)
  --load_timeout_sec N          (default: 43200)
  --value_size N                (default: 1024)
  --key_prefix STR              (default: k)
  --fixed_key_16 0|1            (default: 1)
  --ycsb_scan_max_len N         (default: 100)
  --ycsb_uniform_distribution 0|1 (default: 0)
  -h, --help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir) OUTDIR="$2"; shift 2;;
    --db_path) DB_PATH="$2"; shift 2;;
    --options_file) OPTIONS_FILE="$2"; shift 2;;
    --rl_poller_bin) RL_POLLER_BIN="$2"; shift 2;;
    --load_record_count) LOAD_RECORD_COUNT="$2"; shift 2;;
    --run_duration_sec) RUN_DURATION_SEC="$2"; shift 2;;
    --load_with_agent) LOAD_WITH_AGENT="$2"; shift 2;;
    --load_timeout_sec) LOAD_TIMEOUT_SEC="$2"; shift 2;;
    --value_size) VALUE_SIZE="$2"; shift 2;;
    --key_prefix) KEY_PREFIX="$2"; shift 2;;
    --fixed_key_16) FIXED_KEY_16="$2"; shift 2;;
    --ycsb_scan_max_len) YCSB_SCAN_MAX_LEN="$2"; shift 2;;
    --ycsb_uniform_distribution) YCSB_UNIFORM_DISTRIBUTION="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "ERROR: unknown arg: $1"; usage; exit 2;;
  esac
done

clear_db() {
  if [[ -z "$DB_PATH" || "$DB_PATH" == "/" || "$DB_PATH" == "." ]]; then
    echo "[ycsb] skip db cleanup: unsafe DB_PATH=$DB_PATH" >&2
    return 1
  fi
  if [[ ! -d "$DB_PATH" ]]; then
    echo "[ycsb] ERROR: DB_PATH not found: $DB_PATH" >&2
    return 1
  fi
  echo "[ycsb] cleaning DB_PATH=$DB_PATH"
  if [[ -n "$SUDO_CMD" ]]; then
    $SUDO_CMD find "$DB_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  else
    find "$DB_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
}

copy_db_log_to_exp() {
  local workload="$1"
  local exp_dir="$2"
  local src="$DB_PATH/LOG"
  local dst="$exp_dir/db_LOG_after_${workload}.log"
  if [[ ! -f "$src" ]]; then
    echo "[ycsb] WARN: DB LOG not found at $src"
    return 0
  fi
  if cp "$src" "$dst" 2>/dev/null; then
    echo "[ycsb] copied DB LOG -> $dst"
    return 0
  fi
  if [[ -n "$SUDO_CMD" ]] && $SUDO_CMD test -f "$src"; then
    if $SUDO_CMD cp "$src" "$dst"; then
      $SUDO_CMD chown "$(id -u):$(id -g)" "$dst" 2>/dev/null || true
      echo "[ycsb] copied DB LOG with sudo -> $dst"
      return 0
    fi
  fi
  echo "[ycsb] WARN: failed to copy DB LOG from $src"
}

run_load_phase() {
  local exp_dir="$1"
  local load_out="$exp_dir/load_run"
  local load_metrics="$exp_dir/load_metrics.csv"
  local load_log="$exp_dir/load_poller.log"
  local load_agent_log="$exp_dir/load_agent_log.csv"
  local load_resource="$exp_dir/load_resource_usage.csv"
  local load_agent_stdout="$exp_dir/load_agent_stdout.log"
  mkdir -p "$exp_dir"

  echo "[ycsb] load start: ${LOAD_RECORD_COUNT} records"
  if [[ "$LOAD_WITH_AGENT" -eq 1 ]]; then
    "$SCRIPT_DIR/run_agent_rl_LQ.sh" \
      --outdir "$load_out" \
      --duration-sec "$LOAD_TIMEOUT_SEC" \
      --db_path "$DB_PATH" \
      --options_file "$OPTIONS_FILE" \
      --write_mb_per_sec 0 \
      --value_size "$VALUE_SIZE" \
      --no-clear-db \
      --no-copy-db-log \
      -- \
      --rl_poller_bin "$RL_POLLER_BIN" \
      --key_prefix "$KEY_PREFIX" \
      --fixed_key_16 "$FIXED_KEY_16" \
      --stop_agent_on_poller_exit \
      --ycsb_workload load \
      --ycsb_record_count "$LOAD_RECORD_COUNT"

    cp "$load_out/rl_metrics.csv" "$load_metrics"
    cp "$load_out/poller.log" "$load_log"
    cp "$load_out/agent_log.csv" "$load_agent_log"
    cp "$load_out/resource_usage.csv" "$load_resource"
    cp "$load_out/agent_rl_fifo.log" "$load_agent_stdout"
  else
    "$RL_POLLER_BIN" "$DB_PATH" "$OPTIONS_FILE" "$load_metrics" \
      --create_if_missing=1 \
      --value_size="$VALUE_SIZE" \
      --key_prefix="$KEY_PREFIX" \
      --fixed_key_16="$FIXED_KEY_16" \
      --ycsb_workload=load \
      --ycsb_record_count="$LOAD_RECORD_COUNT" \
      >"$load_log" 2>&1
  fi
  echo "[ycsb] load done: $load_metrics"
}

run_workload_phase() {
  local workload="$1"
  local exp_dir="$2"
  local run_out="$exp_dir/run"
  mkdir -p "$run_out"

  echo "[ycsb] run start: workload=${workload^^} duration=${RUN_DURATION_SEC}s"
  "$SCRIPT_DIR/run_agent_rl_LQ.sh" \
    --outdir "$run_out" \
    --duration-sec "$RUN_DURATION_SEC" \
    --db_path "$DB_PATH" \
    --options_file "$OPTIONS_FILE" \
    --write_mb_per_sec 0 \
    --value_size "$VALUE_SIZE" \
    --no-clear-db \
    -- \
    --rl_poller_bin "$RL_POLLER_BIN" \
    --key_prefix "$KEY_PREFIX" \
    --fixed_key_16 "$FIXED_KEY_16" \
    --ycsb_workload "$workload" \
    --ycsb_record_count "$LOAD_RECORD_COUNT" \
    --ycsb_duration_sec "$RUN_DURATION_SEC" \
    --ycsb_uniform_distribution "$YCSB_UNIFORM_DISTRIBUTION" \
    --ycsb_scan_max_len "$YCSB_SCAN_MAX_LEN"
  copy_db_log_to_exp "$workload" "$exp_dir"
  echo "[ycsb] run done: workload=${workload^^}"
}

mkdir -p "$OUTDIR"

echo "[ycsb] OUTDIR=$OUTDIR"
echo "[ycsb] DB_PATH=$DB_PATH"
echo "[ycsb] OPTIONS_FILE=$OPTIONS_FILE"
echo "[ycsb] LOAD_RECORD_COUNT=$LOAD_RECORD_COUNT RUN_DURATION_SEC=$RUN_DURATION_SEC"
echo "[ycsb] LOAD_WITH_AGENT=$LOAD_WITH_AGENT LOAD_TIMEOUT_SEC=$LOAD_TIMEOUT_SEC"
echo "[ycsb] VALUE_SIZE=$VALUE_SIZE KEY_PREFIX=$KEY_PREFIX FIXED_KEY_16=$FIXED_KEY_16"

if [[ "$LOAD_WITH_AGENT" != "0" && "$LOAD_WITH_AGENT" != "1" ]]; then
  echo "ERROR: --load_with_agent must be 0 or 1 (got: $LOAD_WITH_AGENT)" >&2
  exit 2
fi

[[ -f "$OPTIONS_FILE" ]] || { echo "ERROR: options file not found: $OPTIONS_FILE"; exit 1; }
[[ -d "$DB_PATH" ]] || { echo "ERROR: DB_PATH not found/dir: $DB_PATH"; exit 1; }

if [[ ! -x "$RL_POLLER_BIN" ]]; then
  if [[ -f "$REPO_ROOT/Makefile" ]]; then
    echo "[ycsb] rl_poller not found; building..."
    (cd "$REPO_ROOT" && make -j8 rl_poller)
  fi
fi
[[ -x "$RL_POLLER_BIN" ]] || { echo "ERROR: rl_poller binary not executable: $RL_POLLER_BIN"; exit 1; }

workloads=(a b c d e f)
idx=1
for wl in "${workloads[@]}"; do
  exp_dir="$OUTDIR/exp${idx}_ycsb_${wl}"
  echo
  echo "========== [Experiment ${idx}] YCSB ${wl^^} =========="
  clear_db
  run_load_phase "$exp_dir"
  run_workload_phase "$wl" "$exp_dir"
  idx=$((idx + 1))
done

echo
echo "[ycsb] all experiments completed"
echo "[ycsb] results root: $OUTDIR"
