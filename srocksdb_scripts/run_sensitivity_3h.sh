#!/usr/bin/env bash
set -euo pipefail

ulimit -n 1048576

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

TS=$(date +%m%d_%H%M%S)
OUTROOT=${OUTROOT:-"$REPO_ROOT/srocksdb_evaluation/sensitivity_3h_${TS}"}
DURATION_SEC=${DURATION_SEC:-10800}

DB_PATH=${DB_PATH:-/mnt/f2fs/rlrocksdb_log}
OPTIONS_FILE=${OPTIONS_FILE:-"$REPO_ROOT/srocksdb_options/rl_options_a.ini"}
RL_POLLER_BIN=${RL_POLLER_BIN:-"$REPO_ROOT/rl_poller"}

WRITE_MB_PER_SEC=${WRITE_MB_PER_SEC:-500}
VALUE_SIZE=${VALUE_SIZE:-1024}
SUDO_CMD=${SUDO_CMD:-}
COPY_DB_LOG=${COPY_DB_LOG:-1}

usage() {
  cat <<'USAGE'
Usage: ./run_sensitivity_3h.sh [options]

Runs 15 one-factor-at-a-time sensitivity experiments, each for 3 hours.
Each run uses a fresh DB because run_agent_rl_LQ.sh clears DB_PATH by default.

Options:
  --outroot PATH
  --duration-sec N
  --db_path PATH
  --options_file PATH
  --rl_poller_bin PATH
  --write_mb_per_sec N
  --value_size N
  --no-copy-db-log
  --copy-db-log
  -h, --help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outroot) OUTROOT="$2"; shift 2;;
    --duration-sec) DURATION_SEC="$2"; shift 2;;
    --db_path) DB_PATH="$2"; shift 2;;
    --options_file) OPTIONS_FILE="$2"; shift 2;;
    --rl_poller_bin) RL_POLLER_BIN="$2"; shift 2;;
    --write_mb_per_sec) WRITE_MB_PER_SEC="$2"; shift 2;;
    --value_size) VALUE_SIZE="$2"; shift 2;;
    --no-copy-db-log) COPY_DB_LOG=0; shift 1;;
    --copy-db-log) COPY_DB_LOG=1; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "ERROR: unknown arg: $1" >&2; usage; exit 2;;
  esac
done

mkdir -p "$OUTROOT"
MANIFEST_CSV="$OUTROOT/manifest.csv"
printf 'idx,group,param,value,outdir,notes\n' > "$MANIFEST_CSV"

copy_db_log() {
  local outdir="$1"
  local src="$DB_PATH/LOG"
  local dst="$outdir/db_LOG"
  if [[ ! -f "$src" ]]; then
    echo "[sensitivity] WARN: DB LOG not found at $src"
    return 0
  fi
  if cp "$src" "$dst" 2>/dev/null; then
    echo "[sensitivity] copied DB LOG -> $dst"
    return 0
  fi
  if [[ -n "$SUDO_CMD" ]] && $SUDO_CMD test -f "$src"; then
    if $SUDO_CMD cp "$src" "$dst"; then
      $SUDO_CMD chown "$(id -u):$(id -g)" "$dst" 2>/dev/null || true
      echo "[sensitivity] copied DB LOG with sudo -> $dst"
      return 0
    fi
  fi
  echo "[sensitivity] WARN: failed to copy DB LOG from $src"
}

run_case() {
  local idx="$1"
  local group="$2"
  local param="$3"
  local value="$4"
  local slug="$5"
  local notes="$6"
  shift 6
  local outdir="$OUTROOT/$(printf '%02d' "$idx")_${slug}"

  printf '%s,%s,%s,%s,%s,%s\n' \
    "$idx" "$group" "$param" "$value" "$outdir" "$notes" >> "$MANIFEST_CSV"

  echo
  echo "=== [$idx/15] $group :: $param=$value ==="
  echo "[sensitivity] outdir=$outdir"

  local -a cmd=(
    bash "$SCRIPT_DIR/run_agent_rl_LQ.sh"
    --outdir "$outdir"
    --duration-sec "$DURATION_SEC"
    --db_path "$DB_PATH"
    --options_file "$OPTIONS_FILE"
    --rl_poller_bin "$RL_POLLER_BIN"
    --write_mb_per_sec "$WRITE_MB_PER_SEC"
    --value_size "$VALUE_SIZE"
    --no-copy-db-log
  )
  cmd+=(-- "$@")

  local case_status=0
  "${cmd[@]}" || case_status=$?

  if [[ "$COPY_DB_LOG" -eq 1 ]]; then
    copy_db_log "$outdir"
  fi

  if [[ "$case_status" -ne 0 ]]; then
    return "$case_status"
  fi
}

run_case 1  "semi_safe" "near_stall_backlog_hi_bytes" "12000000000" "near_stall_backlog_hi_12gb" "3h run" \
  --near_stall_backlog_hi_bytes 12000000000
run_case 2  "semi_safe" "near_stall_backlog_hi_bytes" "32000000000" "near_stall_backlog_hi_32gb" "3h run" \
  --near_stall_backlog_hi_bytes 32000000000
run_case 3  "semi_safe" "near_stall_backlog_hi_bytes" "52000000000" "near_stall_backlog_hi_52gb" "3h run" \
  --near_stall_backlog_hi_bytes 52000000000

run_case 4  "semi_safe" "near_stall_l0_th" "6" "near_stall_l0_th_6" "3h run" \
  --near_stall_l0_th 8
run_case 5  "semi_safe" "near_stall_l0_th" "12" "near_stall_l0_th_12" "3h run" \
  --near_stall_l0_th 12
run_case 6  "semi_safe" "near_stall_l0_th" "16" "near_stall_l0_th_16" "3h run" \
  --near_stall_l0_th 16

run_case 7  "learning" "rl_alpha" "0.01" "rl_alpha_001" "3h run" \
  --rl_alpha 0.01
run_case 8  "learning" "rl_alpha" "0.05" "rl_alpha_005" "3h run" \
  --rl_alpha 0.05
run_case 9  "learning" "rl_alpha" "0.25" "rl_alpha_025" "3h run" \
  --rl_alpha 0.25

run_case 10 "learning" "rl_exploration_epsilon" "0.05" "rl_exploration_epsilon_005" "3h run, constant epsilon" \
  --rl_exploration_epsilon 0.05 \
  --rl_epsilon_schedule constant
run_case 11 "learning" "rl_exploration_epsilon" "0.25" "rl_exploration_epsilon_025" "3h run, constant epsilon" \
  --rl_exploration_epsilon 0.25 \
  --rl_epsilon_schedule constant
run_case 12 "learning" "rl_exploration_epsilon" "0.75" "rl_exploration_epsilon_075" "3h run, constant epsilon" \
  --rl_exploration_epsilon 0.75 \
  --rl_epsilon_schedule constant

run_case 13 "learning" "gamma_perf" "0.6" "gamma_perf_06" "3h run" \
  --gamma_perf 0.6
run_case 14 "learning" "gamma_perf" "1.8" "gamma_perf_18" "3h run" \
  --gamma_perf 1.8
run_case 15 "learning" "gamma_perf" "3.0" "gamma_perf_30" "3h run" \
  --gamma_perf 3.0

echo
echo "[sensitivity] completed all runs"
echo "[sensitivity] manifest=$MANIFEST_CSV"
