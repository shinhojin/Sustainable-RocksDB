# Sustainable RocksDB (S-RocksDB)

This repository is the code artifact for the **VLDB 2026 submitted paper**:

**How Much Can RocksDB Chew? Achieving Near-Zero Write Stalls with Sustainable RocksDB**

## 1) Paper Summary

S-RocksDB reformulates write stalls in RocksDB as a continuous control problem.
Instead of reactive stop-and-go behavior, it uses:

- a three-state control model: `SAFE`, `SEMI-SAFE`, `UNSAFE`
- online Q-learning in `SAFE` only
- deterministic guardrails in `SEMI-SAFE` and `UNSAFE`

## 2) Key Components in This Repository

- `srocksdb_src/rl_poller.cc`
  - in-process poller/actuator (C++)
  - samples runtime metrics, writes `rl_metrics.csv`, applies multiplier updates
- `srocksdb_src/agent_rl_fifo.py`
  - out-of-process controller (Python)
  - state classification + SAFE-region Q-learning + actuation decisions
- `srocksdb_scripts/run_agent_rl_LQ.sh`
  - recommended runner for S-RocksDB experiments
- `srocksdb_scripts/run_agent_rl_ycsb.sh`
  - YCSB load + A-F batch runner
- `srocksdb_options/rl_options_a.ini`, `srocksdb_options/rl_options_c.ini`
  - S-RocksDB options presets
- `srocksdb_evaluation/`
  - default output root for run artifacts

## 3) Environment and Build

Minimum:

- Linux
- `gcc/g++`, `make`
- `python3`

Build:

```bash
cd /path/to/Sustainable-RocksDB
make -j"$(nproc)" rl_poller
```

Optional:

```bash
make -j"$(nproc)" db_bench
```

Paper hardware (for reference):

- 2x Intel Xeon Gold 6338 (2.00GHz)
- 504 GB DRAM
- Samsung EVO 870 1TB SATA SSD

## 4) Runtime Paths

Prepare:

- `DB_PATH`: RocksDB data directory
- `OUTDIR`: experiment output directory
- `WAL_DIR` (optional): defaults to `DB_PATH/wal_dir`

Example:

```bash
mkdir -p /path/to/db
mkdir -p /path/to/Sustainable-RocksDB/srocksdb_evaluation
```

Safety:

- `run_agent_rl_LQ.sh` clears `DB_PATH` by default.
- `run_agent_rl_ycsb.sh` clears `DB_PATH` per experiment.
- Do not set `DB_PATH` to shared system paths such as `/` or `/tmp`.

## 5) Reproducing Main S-RocksDB Run (12h-style)

```bash
cd /path/to/Sustainable-RocksDB-temp

bash srocksdb_scripts/run_agent_rl_LQ.sh \
  --db_path /path/to/db \
  --options_file /path/to/Sustainable-RocksDB/srocksdb_options/rl_options_a.ini \
  --outdir /path/to/Sustainable-RocksDB/srocksdb_evaluation/agent_rl_lq_$(date +%m%d_%H%M%S) \
  --duration-sec 43200 \
  --write_mb_per_sec 500 \
  --value_size 1024
```

If you need to keep existing DB files:

```bash
bash srocksdb_scripts/run_agent_rl_LQ.sh \
  --no-clear-db \
  --db_path /path/to/db \
  --options_file /path/to/Sustainable-RocksDB/srocksdb_options/rl_options_a.ini \
  --outdir /path/to/Sustainable-RocksDB/srocksdb_evaluation/no_clear_$(date +%m%d_%H%M%S)
```

## 6) Reproducing YCSB A-F Batch (`run_agent_rl_ycsb.sh`)

The script performs:

1. YCSB load phase
2. YCSB workloads `A, B, C, D, E, F`

Usage:

```bash
cd /path/to/Sustainable-RocksDB-temp

bash srocksdb_scripts/run_agent_rl_ycsb.sh \
  --db_path /path/to/db \
  --options_file /path/to/Sustainable-RocksDB/srocksdb_options/rl_options_a.ini \
  --outdir /path/to/Sustainable-RocksDB/srocksdb_evaluation/ycsb_$(date +%m%d_%H%M%S) \
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

Expected directory pattern:

- `<OUTDIR>/exp1_ycsb_a/`
- `<OUTDIR>/exp2_ycsb_b/`
- ...
- `<OUTDIR>/exp6_ycsb_f/`

## 7) What Is Stored

### 7.1 In `DB_PATH`

- SST files (`*.sst`)
- MANIFEST files (`MANIFEST-*`)
- DB log (`LOG`)
- options snapshots (`OPTIONS-*`)
- metadata (`CURRENT`, `IDENTITY`, `LOCK`)

### 7.2 In `WAL_DIR`

- WAL files (`*.log`)

### 7.3 In `OUTDIR`

- `rl_metrics.csv`
  - poller metrics time series (pressure, stall, multiplier, YCSB counters)
- `agent_log.csv`
  - controller decisions, state transitions, reward terms, action traces
- `resource_usage.csv`
  - poller/agent CPU and memory samples
- `poller.log`
- `agent_rl_fifo.log`
- `config_snapshot.json`
- copied DB log (`db_LOG` or `db_LOG_after_<workload>.log`)

## 8) Options and Control Notes

The main S-RocksDB option files are:

- `srocksdb_options/rl_options_a.ini`
- `srocksdb_options/rl_options_c.ini`

These control runtime behavior such as:

- metric instrumentation
- write throttling enablement
- base write rate
- max delta / min multiplier bounds

In the control loop:

- epoch: 1 second (paper design)
- SAFE-only Q-learning updates
- UNSAFE stall-first recovery with hold
- SEMI-SAFE near-stall hysteresis guardrail

## 9) Quick Verification Checklist

After a run:

```bash
head -1 /path/to/outdir/rl_metrics.csv
head -1 /path/to/outdir/agent_log.csv
tail -n 50 /path/to/outdir/poller.log
tail -n 50 /path/to/outdir/agent_rl_fifo.log
```

## 10) License

Inherited from RocksDB:

- GPLv2 (`COPYING`)
- Apache 2.0 (`LICENSE.Apache`)
