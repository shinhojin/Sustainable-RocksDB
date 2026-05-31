# Sustainable RocksDB (S-RocksDB)

This repository contains the S-RocksDB code built on top of RocksDB. The custom code in this tree focuses on write-stall-aware control: a poller inside the RocksDB process exports runtime metrics, and an external controller adjusts the write multiplier through a FIFO command channel.

The repository also includes experiment runners for:

- Linear-Q controller (`RL_DELTA_M`)
- AIMD and PID-backlog baselines
- YCSB A-F batch experiments
- Time-varying workload experiments
- 3-hour sensitivity sweeps

## Repository Layout

Core S-RocksDB files:

- `srocksdb_src/rl_poller.cc`
  - custom RocksDB-side poller / actuator
  - writes `rl_metrics.csv`
  - accepts runtime multiplier updates through `--cmd_fifo`
- `srocksdb_src/agent_rl_fifo.py`
  - external controller
  - reads poller metrics, classifies runtime state, and sends commands to the FIFO
- `srocksdb_options/rl_options_s.ini`
- `srocksdb_options/rl_options_r.ini`
  - example option presets with metrics and write throttling enabled
- `srocksdb_scripts/run_agent_fifo.sh`
  - low-level launcher that starts both `rl_poller` and `agent_rl_fifo.py`
- `srocksdb_scripts/run_agent_rl_LQ.sh`
  - main single-run S-RocksDB launcher
- `srocksdb_scripts/run_agent_rl_ycsb.sh`
  - YCSB load + A-F batch runner
- `srocksdb_scripts/run_agent_rl_LQ_timevarying.sh`
  - phased workload runner (`load_uniform`, `r10w90`, `r90w10`, `r50w50`)
- `srocksdb_scripts/run_agent_aimd_stall_only.sh`
- `srocksdb_scripts/run_agent_pid_backlog.sh`
  - baseline runners built on the same launch path
- `srocksdb_scripts/run_sensitivity_3h.sh`
  - 15-case sensitivity sweep
- `srocksdb_evaluation/`
  - default output root used by the scripts

## How the Current Code Runs

The typical control path is:

1. `run_agent_rl_LQ.sh` parses experiment-level options.
2. It delegates to `run_agent_fifo.sh`.
3. `run_agent_fifo.sh` creates a temporary options file, injects values such as `wal_dir` and `rl_write_base_rate_bytes_per_sec`, creates a FIFO, and starts:
   - `rl_poller`
   - `agent_rl_fifo.py`
4. `rl_poller` writes sampled metrics to `rl_metrics.csv`.
5. `agent_rl_fifo.py` reads the latest metrics row, computes the next multiplier, and writes commands such as `m=<value>` to the FIFO.

Supported controller modes in the current code:

- `RL_DELTA_M`
- `AIMD_STALL_ONLY`
- `PID`

The agent also has explicit `SAFE`, `SEMI_SAFE`, and `UNSAFE` states, plus soft-guard / hard-lock safety logic.

## Requirements

Minimum:

- Linux
- `gcc` / `g++`
- `make`
- `python3`
- Python `numpy`

For Python setup:

```bash
python3 -m pip install numpy
```

This is still a RocksDB tree, so if the build needs extra system libraries on your machine, refer to `INSTALL.md`.

## Build

Build the custom poller first:

```bash
make -j"$(nproc)" rl_poller
```

Optional:

```bash
make -j"$(nproc)" db_bench
```

The `Makefile` in this repository already defines the `rl_poller` target and builds it from `srocksdb_src/rl_poller.cc`.

## Before You Run

The scripts contain machine-specific defaults such as `/mnt/f2fs/rlrocksdb_log`, so in practice you should almost always override `--db_path`.

Prepare dedicated directories:

```bash
mkdir -p /path/to/db
mkdir -p /path/to/out
```

Important safety note:

- several runners delete the contents of `DB_PATH` before starting
- use a dedicated experiment directory for `DB_PATH`
- do not point `DB_PATH` at shared or important paths such as `/`, `/tmp`, or a reused database directory unless that is intentional

WAL behavior:

- `run_agent_fifo.sh` uses `WAL_DIR="$DB_PATH/wal_dir"` unless `--wal_dir` is provided
- it removes old WAL files in that directory before starting

## Main S-RocksDB Run

`srocksdb_scripts/run_agent_rl_LQ.sh` is the main single-run launcher.

Current defaults in the script:

- output root: `srocksdb_evaluation/agent_rl_lq_<timestamp>`
- duration: `43200` seconds
- options file: `srocksdb_options/rl_options_s.ini`
- write rate: `500 MB/s`
- `m_min=0.01`
- `m_max=0.5`

Example:

```bash
bash srocksdb_scripts/run_agent_rl_LQ.sh \
  --db_path /path/to/db \
  --options_file "$PWD/srocksdb_options/rl_options_s.ini" \
  --outdir /path/to/out/agent_rl_lq_$(date +%m%d_%H%M%S) \
  --duration-sec 43200 \
  --write_mb_per_sec 500 \
  --value_size 1024
```

If you want to keep the current DB contents:

```bash
bash srocksdb_scripts/run_agent_rl_LQ.sh \
  --no-clear-db \
  --db_path /path/to/db \
  --options_file "$PWD/srocksdb_options/rl_options_s.ini" \
  --outdir /path/to/out/no_clear_$(date +%m%d_%H%M%S)
```

## Common Runners

### 1) YCSB A-F batch

`srocksdb_scripts/run_agent_rl_ycsb.sh` runs:

1. a load phase
2. workloads `A, B, C, D, E, F`

Current defaults in the script:

- options file: `srocksdb_options/rl_options_s.ini`
- `LOAD_RECORD_COUNT=50000000`
- `RUN_DURATION_SEC=3600`
- `LOAD_WITH_AGENT=1`

Example:

```bash
bash srocksdb_scripts/run_agent_rl_ycsb.sh \
  --db_path /path/to/db \
  --options_file "$PWD/srocksdb_options/rl_options_s.ini" \
  --outdir /path/to/out/ycsb_$(date +%m%d_%H%M%S) \
  --load_record_count 50000000 \
  --run_duration_sec 3600 \
  --load_with_agent 1 \
  --load_timeout_sec 43200 \
  --value_size 1024 \
  --key_prefix k \
  --fixed_key_16 1 \
  --ycsb_scan_max_len 100 \
  --ycsb_uniform_distribution 0
```

Output layout:

- `<OUTDIR>/exp1_ycsb_a/`
- `<OUTDIR>/exp2_ycsb_b/`
- `<OUTDIR>/exp3_ycsb_c/`
- `<OUTDIR>/exp4_ycsb_d/`
- `<OUTDIR>/exp5_ycsb_e/`
- `<OUTDIR>/exp6_ycsb_f/`

Each experiment directory contains a `load_run/` sub-run and a `run/` sub-run.

### 2) Time-varying workload run

`srocksdb_scripts/run_agent_rl_LQ_timevarying.sh` runs:

- `phase0_load`
- `phase1_r10w90`
- `phase2_r90w10`
- `phase3_r50w50`

Default phase durations in the current script:

- preload timeout: `1800` seconds
- phase 1: `10800` seconds
- phase 2: `10800` seconds
- phase 3: `10800` seconds

### 3) Sensitivity sweep

`srocksdb_scripts/run_sensitivity_3h.sh` runs 15 one-factor-at-a-time cases and writes a manifest to:

- `<OUTROOT>/manifest.csv`

### 4) Baseline controllers

Available baseline launchers:

- `srocksdb_scripts/run_agent_aimd_stall_only.sh`
- `srocksdb_scripts/run_agent_pid_backlog.sh`

Both delegate to `run_agent_fifo.sh` and reuse the same poller / agent logging path. The difference is the controller mode and controller-specific parameters they pass.

## Passing Extra Low-Level Arguments

The higher-level scripts can forward extra arguments to `run_agent_fifo.sh` after `--`.

Example:

```bash
bash srocksdb_scripts/run_agent_rl_LQ.sh \
  --db_path /path/to/db \
  --outdir /path/to/out/test_run \
  -- \
  --key_prefix user \
  --fixed_key_16 0 \
  --stop_agent_on_poller_exit
```

Useful low-level arguments supported by `run_agent_fifo.sh` include:

- `--key_prefix`
- `--fixed_key_16`
- `--wal_dir`
- `--stop_agent_on_poller_exit`
- `--soft_guard_enabled`
- `--soft_guard_disabled`
- YCSB-specific knobs such as `--ycsb_workload`, `--ycsb_record_count`, and `--ycsb_duration_sec`

## What Gets Stored

### In `DB_PATH`

- SST files
- `MANIFEST-*`
- `CURRENT`, `IDENTITY`, `LOCK`
- RocksDB `LOG`
- `OPTIONS-*`

### In `WAL_DIR`

- WAL files such as `*.log`

### In a normal single-run `OUTDIR`

- `rl_metrics.csv`
  - poller metrics written by `rl_poller`
- `agent_log.csv`
  - controller decisions, state transitions, rewards, and applied commands
- `resource_usage.csv`
  - poller / agent CPU and memory samples
- `poller.log`
- `agent_rl_fifo.log`
- `config_snapshot.json`
  - the controller configuration captured by `agent_rl_fifo.py`
- `db_LOG`
  - copied RocksDB log when log-copy is enabled

YCSB and time-varying runners create nested per-phase / per-workload directories and may store copied logs as:

- `db_LOG_after_<workload>.log`

## Notes on Option Presets

The repository currently includes two example option presets:

- `srocksdb_options/rl_options_s.ini`
- `srocksdb_options/rl_options_r.ini`

Both enable the custom metrics path and write throttling. The current scripts use them as follows:

- `run_agent_rl_LQ.sh` defaults to `rl_options_r.ini`
- `run_agent_rl_ycsb.sh`, `run_agent_rl_LQ_timevarying.sh`, and the baseline runners default to `rl_options_s.ini`

## License

This repository inherits RocksDB licensing:

- `COPYING` for GPLv2
- `LICENSE.Apache` for Apache 2.0
