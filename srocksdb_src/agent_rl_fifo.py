#!/usr/bin/env python3
"""
Online FIFO agent (stall-first + RL_DELTA_M / AIMD_STALL_ONLY / PID).

Self-audit:
- stall_event is detected using delta/cumulative stall counters (or is_write_stopped==1, or delayed_rate>0).
- SAFE MODE: on any stall_event, force m_cmd_effective=m_min immediately and keep it for HOLD_SEC.
- RL_DELTA_M: in SAFE state, Linear Q-learning chooses delta action to update m; in
  SEMI_SAFE/UNSAFE, safety rules override.
- AIMD_STALL_ONLY: in SAFE state, additive increase is applied every tick; after a
  stall-induced SAFE hold, the first SAFE tick applies multiplicative decrease from
  the pre-stall command, then resumes additive increase.
- PID: in SAFE state, a PID controller adjusts m around either a pending-compaction
  backlog target or an L0-file target; in SEMI_SAFE/UNSAFE, safety rules override.
- TD update is applied online with (s, a, r, s') transitions.
"""
import argparse
import csv
import json
import math
import random
import errno
import os
import signal
import sys
import time
import faulthandler
from collections import Counter, deque
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import numpy as np

MAX_TAIL_BYTES = 65536
READ_ERR_LOG_INTERVAL_SEC = 5.0
_READ_ERR_LAST: Dict[str, float] = {}


def log_read_error(kind: str, path: str, exc: Exception) -> None:
    now = time.monotonic()
    last = _READ_ERR_LAST.get(kind, 0.0)
    if now - last < READ_ERR_LOG_INTERVAL_SEC:
        return
    _READ_ERR_LAST[kind] = now
    detail = f"{type(exc).__name__}: {exc}"
    print(f"[warn] {kind} failed: path={path} err={detail}", file=sys.stderr)

DEFAULT_LADDER = [
    0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12, 0.15,
    0.20, 0.30, 0.40, 0.60, 0.80, 1.00,
]

DEFAULT_DELTA_ACTIONS = [-0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03]
DEFAULT_METRICS_HEADER = [
    "ts",
    "l0_file_count",
    "imm_memtable_bytes",
    "write_lat_p99_us",
    "write_in_bytes_per_sec",
    "write_multiplier_prev",
    "write_multiplier_cmd",
    "write_multiplier_applied",
    "true_pending_flush_bytes",
    "true_pending_compaction_bytes",
    "is_write_stopped",
    "actual_delayed_write_rate_bps",
    "compaction_pending",
    "memtable_flush_pending",
    "num_immutable_mem_table",
    "estimate_pending_compaction_bytes",
    "write_stall_stop_count",
    "write_stall_delay_count",
    "write_stall_total_count",
    "write_stall_delta_count",
    "write_stall_hist_count",
    "write_stall_hist_delta_count",
]

class RLActionProvider:
    def __init__(self, mode: str, action_file: str = "", n_actions: int = 2) -> None:
        self.mode = mode
        self.action_file = action_file
        self.last_action = 0
        self.n_actions = n_actions

    def get_action(self) -> int:
        if self.mode == "cycle":
            self.last_action = (self.last_action + 1) % self.n_actions
            return self.last_action
        if self.mode == "file" and self.action_file:
            try:
                with open(self.action_file, "r", encoding="utf-8") as f:
                    val = f.read().strip()
                act = int(val)
                if 0 <= act < self.n_actions:
                    self.last_action = act
                    return act
            except Exception:
                return 0
        return 0


class OnlineLearner:
    def __init__(
        self,
        enabled: bool,
        n_actions: int,
        lr: float,
        max_grad_norm: float,
        epsilon: float,
        update_interval: int,
        min_buffer: int,
        ckpt_dir: str,
        ckpt_every: int,
        rollback_on_stall: bool,
        rollback_window_sec: float,
    ) -> None:
        self.enabled = enabled
        self.n_actions = max(2, n_actions)
        self.lr = lr
        self.max_grad_norm = max_grad_norm
        self.epsilon = epsilon
        self.update_interval = update_interval
        self.min_buffer = min_buffer
        self.ckpt_dir = ckpt_dir
        self.ckpt_every = ckpt_every
        self.rollback_on_stall = rollback_on_stall
        self.rollback_window_sec = rollback_window_sec
        self.theta = [0.0 for _ in range(self.n_actions)]
        self.update_id = 0
        self.policy_version_id = "init"
        self.buffer: List[Dict[str, float]] = []
        self.last_update_ts = None
        self.last_good_theta = list(self.theta)
        self.last_good_update_id = 0
        self.last_rollback_update_id = -1
        if self.enabled and self.ckpt_dir:
            os.makedirs(self.ckpt_dir, exist_ok=True)

    def probs(self) -> List[float]:
        mx = max(self.theta)
        exps = [math.exp(t - mx) for t in self.theta]
        z = sum(exps)
        return [e / z for e in exps]

    def select_action(self, rng: float, allow_explore: bool) -> Tuple[int, int, int]:
        probs = self.probs()
        action = int(max(range(len(probs)), key=lambda i: probs[i]))
        explored = 0
        epsilon_used = 0
        if allow_explore and self.epsilon > 0.0:
            epsilon_used = 1
            if rng < self.epsilon:
                explored = 1
                action = (action + 1) % len(probs)
        return action, explored, epsilon_used

    def record(self, a: int, r: float) -> None:
        if not self.enabled:
            return
        self.buffer.append({"a": a, "r": r})

    def maybe_update(self, step: int, now: float) -> Dict[str, object]:
        info: Dict[str, object] = {}
        if not self.enabled:
            return info
        if step % self.update_interval != 0:
            return info
        if len(self.buffer) < self.min_buffer:
            return info
        batch = self.buffer[-min(len(self.buffer), 256):]
        r_vals = [b["r"] for b in batch]
        baseline = sum(r_vals) / max(1, len(r_vals))
        probs = self.probs()
        grads = [0.0 for _ in range(self.n_actions)]
        for b in batch:
            a = int(b["a"])
            adv = b["r"] - baseline
            for i in range(self.n_actions):
                grads[i] += ((1.0 if a == i else 0.0) - probs[i]) * adv
        grads = [g / max(1, len(batch)) for g in grads]
        grad_norm = math.sqrt(sum(g * g for g in grads))
        if self.max_grad_norm > 0.0 and grad_norm > self.max_grad_norm:
            scale = self.max_grad_norm / max(1e-9, grad_norm)
            grads = [g * scale for g in grads]
            grad_norm = self.max_grad_norm
        self.last_good_theta = list(self.theta)
        self.last_good_update_id = self.update_id
        for i in range(self.n_actions):
            self.theta[i] += self.lr * grads[i]
        self.update_id += 1
        self.policy_version_id = f"onlearn_{self.update_id}"
        self.last_update_ts = now
        ckpt_path = ""
        if self.ckpt_dir and (self.update_id % max(1, self.ckpt_every) == 0):
            ckpt_path = os.path.join(self.ckpt_dir, f"policy_update_{self.update_id}.json")
            with open(ckpt_path, "w", encoding="utf-8") as f:
                f.write(
                    "{\n"
                    f"  \"update_id\": {self.update_id},\n"
                    f"  \"theta\": [{', '.join(f'{t:.6f}' for t in self.theta)}]\n"
                    "}\n"
                )
        info.update(
            {
                "update_id": self.update_id,
                "buffer_size": len(self.buffer),
                "batch_size": len(batch),
                "loss": -sum(r_vals) / max(1, len(r_vals)),
                "grad_norm": grad_norm,
                "lr": self.lr,
                "ckpt_path": ckpt_path,
            }
        )
        return info

    def maybe_rollback(self, now: float, stall_event: int) -> bool:
        if not self.enabled or not self.rollback_on_stall:
            return False
        if self.last_update_ts is None or stall_event == 0:
            return False
        if now - self.last_update_ts > self.rollback_window_sec:
            return False
        if self.last_rollback_update_id == self.update_id:
            return False
        self.theta = list(self.last_good_theta)
        self.policy_version_id = f"rollback_to_{self.last_good_update_id}"
        self.last_rollback_update_id = self.update_id
        return True


class LinearQAgent:
    def __init__(
        self,
        n_actions: int,
        state_dim: int,
        alpha: float,
        gamma: float,
        epsilon: float,
        seed: int = 0,
    ) -> None:
        self.n_actions = n_actions
        self.state_dim = state_dim
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon
        self.rng = random.Random(seed)
        self.W = np.zeros((n_actions, state_dim), dtype=np.float32)
        self.num_updates = 0

    def q_values(self, s: np.ndarray) -> np.ndarray:
        return self.W.dot(s)

    def select_action(self, s: np.ndarray) -> Tuple[int, np.ndarray]:
        qs = self.q_values(s)
        if self.rng.random() < self.epsilon:
            a = self.rng.randrange(self.n_actions)
            return a, qs
        a = int(np.argmax(qs))
        return a, qs

    def update(
        self, s: np.ndarray, a: int, r: float, s_next: np.ndarray, done: bool
    ) -> Tuple[float, float, float]:
        q = float(self.W[a].dot(s))
        if done:
            q_next = 0.0
        else:
            q_next = float(np.max(self.W.dot(s_next)))
        td = r + self.gamma * q_next - q
        self.W[a] += (self.alpha * td) * s
        self.num_updates += 1
        return td, q, q_next


def read_header(path: str) -> Optional[List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            line = f.readline()
    except FileNotFoundError as exc:
        log_read_error("read_header", path, exc)
        return None
    except Exception as exc:
        log_read_error("read_header", path, exc)
        return None
    if not line:
        return None
    line = line.strip()
    if not line:
        return None
    return next(csv.reader([line]))


def read_last_complete_row(path: str, header: List[str], max_bytes: int = MAX_TAIL_BYTES) -> Optional[Dict[str, str]]:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size <= 0:
                return None
            read_size = min(size, max_bytes)
            f.seek(-read_size, os.SEEK_END)
            data = f.read().decode("utf-8", errors="ignore")
    except FileNotFoundError as exc:
        log_read_error("read_last_row", path, exc)
        return None
    except Exception as exc:
        log_read_error("read_last_row", path, exc)
        return None

    lines = data.splitlines()
    if not lines:
        return None
    if size > read_size and data[0] != "\n":
        lines = lines[1:]
    if not lines:
        return None

    ncol = len(header)
    for i in range(1, len(lines) + 1):
        line = lines[-i].strip()
        if not line:
            continue
        row = next(csv.reader([line]))
        if len(row) != ncol:
            continue
        return dict(zip(header, row))
    return None


def fallback_metrics_header(path: str) -> Optional[List[str]]:
    try:
        with open(path, "rb") as f:
            data = f.read(4096)
    except FileNotFoundError as exc:
        log_read_error("fallback_header", path, exc)
        return None
    except Exception as exc:
        log_read_error("fallback_header", path, exc)
        return None
    if not data:
        return None
    text = data.decode("utf-8", errors="ignore")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        cols = next(csv.reader([line]))
        if len(cols) == len(DEFAULT_METRICS_HEADER):
            return list(DEFAULT_METRICS_HEADER)
        break
    return None


def wait_for_metrics_header(path: str, timeout_sec: float = 5.0, poll_sec: float = 0.5) -> Tuple[List[str], bool, float]:
    """Try to read the CSV header before starting the control loop.

    We prefer the real header from the poller CSV; if it is not yet present,
    try a lightweight fallback sniff. After `timeout_sec` we give up and return
    the DEFAULT_METRICS_HEADER so the agent can still run.
    Returns: (header, used_fallback, waited_seconds)
    """
    start = time.monotonic()
    while True:
        header = read_header(path)
        if header:
            return header, False, time.monotonic() - start

        fallback = fallback_metrics_header(path)
        if fallback:
            return fallback, True, time.monotonic() - start

        if timeout_sec >= 0 and (time.monotonic() - start) >= timeout_sec:
            return list(DEFAULT_METRICS_HEADER), True, time.monotonic() - start

        time.sleep(max(0.01, poll_sec))


def to_float(s: Optional[str], default: float = 0.0) -> float:
    if s is None or s == "":
        return default
    try:
        v = float(s)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return v


def has_metric_value(val: Optional[str]) -> bool:
    return val is not None and str(val).strip() != ""


def safe_float(x, default: float = 0.0) -> float:
    if x is None:
        return default
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return v


def clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def log_ratio(x, scale) -> float:
    xv = max(0.0, safe_float(x, 0.0))
    sv = max(1e-9, safe_float(scale, 1.0))
    denom = max(math.log1p(sv), 1e-9)
    return math.log1p(xv) / denom


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def make_state(
    obs: dict,
    fsm_state: str,
    last_cmd: float,
    time_since_unsafe_sec: float,
    args,
) -> np.ndarray:
    m_prev = clip(last_cmd, 0.0, 1.0)
    l0_norm = clip(
        safe_float(obs.get("l0_file_count")) / args.state_scale_l0,
        0.0,
        args.state_clip,
    )
    comp_norm = clip(
        log_ratio(
            obs.get("estimate_pending_compaction_bytes"),
            args.state_scale_compaction_bytes,
        ),
        0.0,
        args.state_clip,
    )
    flush_norm = clip(
        log_ratio(obs.get("true_pending_flush_bytes"), args.state_scale_flush_bytes),
        0.0,
        args.state_clip,
    )
    delay_norm = clip(
        log_ratio(obs.get("actual_delayed_write_rate_bps"), args.state_scale_delay_bps),
        0.0,
        args.state_clip,
    )
    in_norm = clip(
        log_ratio(obs.get("write_in_bytes_per_sec"), args.state_scale_write_in_bps),
        0.0,
        args.state_clip,
    )
    p99_norm = clip(
        log_ratio(obs.get("write_lat_p99_us"), args.state_scale_p99_us),
        0.0,
        args.state_clip,
    )
    t_unsafe = clip(safe_float(time_since_unsafe_sec) / 600.0, 0.0, args.state_clip)

    is_safe = 1.0 if fsm_state == "SAFE" else 0.0
    is_semi = 1.0 if fsm_state == "SEMI_SAFE" else 0.0
    is_unsafe = 1.0 if fsm_state == "UNSAFE" else 0.0

    s = np.array(
        [
            1.0,
            m_prev,
            l0_norm,
            comp_norm,
            flush_norm,
            delay_norm,
            in_norm,
            p99_norm,
            t_unsafe,
            is_safe,
            is_semi,
            is_unsafe,
        ],
        dtype=np.float32,
    )
    if not np.all(np.isfinite(s)):
        s = np.nan_to_num(s, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    return s


def parse_ladder(s: str, m_min: float, m_max: float) -> List[float]:
    out: List[float] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except Exception:
            continue
    if not out:
        out = list(DEFAULT_LADDER)
    out = sorted(set(clamp(v, m_min, m_max) for v in out))
    return [v for v in out if v >= m_min and v <= m_max] or [m_min]


def parse_delta_actions(s: str, delta_m_max: float) -> List[float]:
    out: List[float] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except Exception:
            continue
    if not out:
        out = list(DEFAULT_DELTA_ACTIONS)
    lim = max(0.0, float(delta_m_max))
    clipped = [clamp(v, -lim, lim) for v in out]
    if not clipped:
        clipped = list(DEFAULT_DELTA_ACTIONS)
    return clipped


_FIFO_WRITER_FD: Optional[int] = None
_FIFO_WRITER_PATH: Optional[str] = None


def _fifo_close() -> None:
    global _FIFO_WRITER_FD
    if _FIFO_WRITER_FD is None:
        return
    try:
        os.close(_FIFO_WRITER_FD)
    except Exception:
        pass
    _FIFO_WRITER_FD = None


def _fifo_ensure_open(path: str) -> Tuple[bool, str]:
    global _FIFO_WRITER_FD, _FIFO_WRITER_PATH
    if _FIFO_WRITER_FD is not None and _FIFO_WRITER_PATH == path:
        return True, ""
    if _FIFO_WRITER_FD is not None and _FIFO_WRITER_PATH != path:
        _fifo_close()
    try:
        _FIFO_WRITER_FD = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
        _FIFO_WRITER_PATH = path
        return True, ""
    except OSError as exc:
        _FIFO_WRITER_FD = None
        _FIFO_WRITER_PATH = path
        return False, f"open_failed:{exc.errno}"


def _fifo_write(path: str, payload: str) -> Tuple[bool, str]:
    ok, err = _fifo_ensure_open(path)
    if not ok:
        return False, err
    data = payload.encode("utf-8")
    try:
        os.write(_FIFO_WRITER_FD, data)
        return True, ""
    except OSError as exc:
        # Reader restart/broken pipe path: close and reopen once, then retry once.
        if exc.errno in (errno.EPIPE, errno.EBADF):
            _fifo_close()
            ok2, err2 = _fifo_ensure_open(path)
            if not ok2:
                return False, f"write_failed:{exc.errno}|{err2}"
            try:
                os.write(_FIFO_WRITER_FD, data)
                return True, ""
            except OSError as exc2:
                if exc2.errno in (errno.EPIPE, errno.EBADF):
                    _fifo_close()
                return False, f"write_retry_failed:{exc2.errno}"
        return False, f"write_failed:{exc.errno}"


def main() -> int:
    faulthandler.enable()
    raw_argv = sys.argv[1:]
    # Allow `--delta_actions -0.03,-0.01,0,0.01` form (without shell quoting).
    # argparse can mistake the next token (starting with '-') as another option.
    normalized_argv: List[str] = []
    i = 0
    while i < len(raw_argv):
        tok = raw_argv[i]
        if tok == "--delta_actions" and i + 1 < len(raw_argv):
            normalized_argv.append(f"--delta_actions={raw_argv[i + 1]}")
            i += 2
            continue
        normalized_argv.append(tok)
        i += 1
    ap = argparse.ArgumentParser()
    ap.add_argument("--fifo", required=True)
    ap.add_argument("--poller_csv", default="")
    ap.add_argument("--metrics_csv", default="")
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--period_sec", type=float, default=1.0)
    ap.add_argument("--fifo_heartbeat_sec", type=float, default=5.0)
    ap.add_argument("--fifo_resend_gap", type=float, default=0.02)
    ap.add_argument("--fifo_resend_on_mismatch_enabled", action="store_true", default=True)
    ap.add_argument("--fifo_resend_on_mismatch_disabled", action="store_false", dest="fifo_resend_on_mismatch_enabled")
    ap.add_argument("--log_flush_sec", type=float, default=1.0)
    ap.add_argument("--timeout_sec", type=float, default=600.0)
    ap.add_argument("--m_min", type=float, default=0.01)
    ap.add_argument("--m_max", type=float, default=0.2)
    ap.add_argument("--hold_sec", type=float, default=10.0)
    ap.add_argument("--recover_free_sec", type=float, default=20.0)
    ap.add_argument("--recover_step_sec", type=float, default=5.0)
    ap.add_argument("--ladder", default=",".join(str(v) for v in DEFAULT_LADDER))
    ap.add_argument(
        "--recover_controller_mode",
        choices=["RL_DELTA_M", "AIMD_STALL_ONLY", "PID"],
        default="RL_DELTA_M",
    )
    ap.add_argument("--rl_action_mode", choices=["hold", "cycle", "file"], default="hold")
    ap.add_argument("--rl_action_file", default="")
    ap.add_argument("--delta_actions", default=",".join(str(v) for v in DEFAULT_DELTA_ACTIONS))
    ap.add_argument("--delta_m_max", type=float, default=0.02)
    ap.add_argument("--semi_safe_step", type=float, default=0.03)
    ap.add_argument("--semi_safe_floor", type=float, default=0.03)
    ap.add_argument("--delta_smooth_eta", type=float, default=0.18)
    ap.add_argument("--aimd_ai_step", type=float, default=0.005)
    ap.add_argument("--aimd_md_beta", type=float, default=0.7)
    ap.add_argument(
        "--pid_signal",
        choices=["backlog", "l0"],
        default="backlog",
        help="PID input signal: pending-compaction backlog or L0 file count",
    )
    ap.add_argument("--pid_backlog_target_bytes", type=float, default=12_000_000_000.0)
    ap.add_argument("--pid_backlog_scale_bytes", type=float, default=12_000_000_000.0)
    ap.add_argument("--pid_l0_target_files", type=float, default=8.0)
    ap.add_argument("--pid_l0_scale_files", type=float, default=8.0)
    ap.add_argument("--pid_kp", type=float, default=0.02)
    ap.add_argument("--pid_ki", type=float, default=0.004)
    ap.add_argument("--pid_kd", type=float, default=0.01)
    ap.add_argument("--pid_integral_min", type=float, default=-4.0)
    ap.add_argument("--pid_integral_max", type=float, default=4.0)
    ap.add_argument("--pid_output_max", type=float, default=0.03)
    ap.add_argument("--startup_force_sec", type=float, default=10.0)
    # online learning flags
    ap.add_argument("--rl_online_learning_enabled", action="store_true")
    ap.add_argument("--rl_online_update_interval_steps", type=int, default=200)
    ap.add_argument("--rl_online_min_buffer_steps", type=int, default=400)
    ap.add_argument("--rl_exploration_epsilon", type=float, default=0.25)
    ap.add_argument(
        "--rl_epsilon_schedule",
        choices=["warmup_180", "constant"],
        default="warmup_180",
    )
    ap.add_argument("--rl_gamma", type=float, default=0.95)
    ap.add_argument("--rl_alpha", type=float, default=0.05)
    ap.add_argument("--rl_learning_rate", type=float, default=1e-4)
    ap.add_argument("--rl_max_grad_norm", type=float, default=0.5)
    ap.add_argument("--rl_ckpt_dir", default="")
    ap.add_argument("--rl_ckpt_every_updates", type=int, default=10)
    ap.add_argument("--rl_rollback_on_stall", action="store_true")
    ap.add_argument("--rl_rollback_window_sec", type=float, default=300.0)
    ap.add_argument("--stall_penalty_C", type=float, default=200.0)
    ap.add_argument("--risk_backlog_eps", type=float, default=0.18)
    ap.add_argument("--risk_latency_eps", type=float, default=0.15)
    ap.add_argument("--risk_backlog_ref_bytes", type=float, default=1e9)
    ap.add_argument("--risk_latency_ref_us", type=float, default=1000.0)
    ap.add_argument("--gamma_perf", type=float, default=1.8)
    ap.add_argument("--soft_guard_enabled", action="store_true", default=True)
    ap.add_argument("--soft_guard_disabled", action="store_false", dest="soft_guard_enabled")
    ap.add_argument("--soft_guard_cap_value", type=float, default=0.06)
    ap.add_argument("--soft_guard_requires_safe_mode0", action="store_true", default=True)
    ap.add_argument("--soft_guard_requires_safe_mode_any", action="store_false", dest="soft_guard_requires_safe_mode0")
    ap.add_argument(
        "--near_stall_backlog_hi_bytes",
        type=int,
        default=32_000_000_000,
        help="Near-stall trigger: pending compaction high threshold",
    )
    ap.add_argument(
        "--near_stall_backlog_rise_window_sec",
        type=float,
        default=30.0,
        help="Near-stall trigger: backlog rise lookback window",
    )
    ap.add_argument(
        "--near_stall_backlog_rise_hi_bytes",
        type=int,
        default=24_000_000_000,
        help="Near-stall trigger: backlog rise threshold within lookback window",
    )
    ap.add_argument(
        "--near_stall_l0_th",
        type=int,
        default=12,
        help="Near-stall trigger: L0 threshold (combined with backlog floor)",
    )
    ap.add_argument(
        "--near_stall_l0_backlog_min_bytes",
        type=int,
        default=24_000_000_000,
        help="Near-stall trigger: minimum backlog required for L0-based trigger",
    )
    ap.add_argument(
        "--near_stall_l0_force_th",
        type=int,
        default=16,
        help="Near-stall trigger: force trigger when L0 reaches this threshold regardless of backlog (<=0 disables)",
    )
    ap.add_argument(
        "--near_stall_trigger_consecutive_sec",
        type=float,
        default=2.0,
        help="Near-stall trigger must hold for this many seconds before SEMI_SAFE latches",
    )
    ap.add_argument(
        "--semi_safe_min_on_sec",
        type=float,
        default=0.0,
        help="Minimum SEMI_SAFE on-time before release checks are allowed",
    )
    ap.add_argument(
        "--semi_safe_release_backlog_lo_bytes",
        type=int,
        default=16_000_000_000,
        help="SEMI_SAFE release: backlog must be below this threshold",
    )
    ap.add_argument(
        "--semi_safe_release_backlog_rise_window_sec",
        type=float,
        default=30.0,
        help="SEMI_SAFE release: backlog rise lookback window",
    )
    ap.add_argument(
        "--semi_safe_release_backlog_rise_max_bytes",
        type=int,
        default=1_000_000_000_000,
        help="SEMI_SAFE release: backlog rise must be <= this value within lookback window",
    )
    ap.add_argument(
        "--semi_safe_release_l0_th",
        type=int,
        default=8,
        help="SEMI_SAFE release: L0 file count must be <= this threshold",
    )
    ap.add_argument(
        "--semi_safe_release_hold_sec",
        type=float,
        default=3.0,
        help="SEMI_SAFE release conditions must hold for this long",
    )
    ap.add_argument(
        "--semi_safe_cooldown_sec",
        type=float,
        default=0.0,
        help="Cooldown after SEMI_SAFE release before re-arming",
    )
    ap.add_argument(
        "--hard_lock_backlog_bytes",
        type=int,
        default=48_000_000_000,
        help="Hard lock enters when backlog reaches this value",
    )
    ap.add_argument(
        "--hard_lock_release_backlog_bytes",
        type=int,
        default=24_000_000_000,
        help="Hard lock release backlog threshold",
    )
    ap.add_argument(
        "--hard_lock_release_hold_sec",
        type=float,
        default=120.0,
        help="Hard lock release conditions must hold for this long",
    )
    ap.add_argument("--state_scale_compaction_bytes", type=float, default=1e9)
    ap.add_argument("--state_scale_flush_bytes", type=float, default=1e8)
    ap.add_argument("--state_scale_write_in_bps", type=float, default=1e8)
    ap.add_argument("--state_scale_delay_bps", type=float, default=1e8)
    ap.add_argument("--state_scale_p99_us", type=float, default=20000.0)
    ap.add_argument("--state_scale_l0", type=float, default=64.0)
    ap.add_argument("--state_clip", type=float, default=5.0)
    ap.add_argument("--dry_run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(normalized_argv)
    aimd_ai_step = max(0.0, float(args.aimd_ai_step))
    aimd_md_beta = clamp(float(args.aimd_md_beta), 0.0, 1.0)
    pid_signal = str(args.pid_signal)
    pid_signal_is_l0 = pid_signal == "l0"
    pid_variant_id = "PID_L0" if pid_signal_is_l0 else "PID"
    pid_policy_version_id = "pid_l0" if pid_signal_is_l0 else "pid_backlog"
    pid_reason = "PID_L0" if pid_signal_is_l0 else "PID_BACKLOG"
    pid_no_metric_reason = "PID_NO_L0" if pid_signal_is_l0 else "PID_NO_BACKLOG"
    pid_integral_min = min(float(args.pid_integral_min), float(args.pid_integral_max))
    pid_integral_max = max(float(args.pid_integral_min), float(args.pid_integral_max))
    pid_output_max = max(0.0, float(args.pid_output_max))

    metrics_csv = args.poller_csv or args.metrics_csv
    if not metrics_csv:
        print("ERROR: --poller_csv (or --metrics_csv) required", file=sys.stderr)
        return 2
    if not os.path.isfile(metrics_csv):
        print(f"[warn] poller_csv not found yet: {metrics_csv}", file=sys.stderr)
    if not os.path.exists(args.fifo):
        print(f"[warn] fifo path not found yet: {args.fifo}", file=sys.stderr)

    ladder = parse_ladder(args.ladder, args.m_min, args.m_max)
    delta_actions = parse_delta_actions(args.delta_actions, args.delta_m_max)
    delta_zero_idx = 0
    if delta_actions:
        delta_zero_idx = min(range(len(delta_actions)), key=lambda i: abs(delta_actions[i]))
    n_actions = len(delta_actions)
    state_dim = 12
    q_agent = LinearQAgent(
        n_actions=n_actions,
        state_dim=state_dim,
        alpha=float(args.rl_alpha),
        gamma=float(args.rl_gamma),
        epsilon=float(args.rl_exploration_epsilon),
        seed=0,
    )
    if n_actions > 0:
        # Tie-break mitigation: slightly prefer the action closest to delta=0 at init.
        q_agent.W[delta_zero_idx, 0] += 1e-3

    out_dir = os.path.dirname(os.path.abspath(args.out_csv))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        online_learning = args.recover_controller_mode == "RL_DELTA_M"
        if online_learning:
            variant_id = "RL_DELTA_M_ONLY"
        elif args.recover_controller_mode == "PID":
            variant_id = pid_variant_id
        else:
            variant_id = str(args.recover_controller_mode)
        run_duration_sec = float(args.timeout_sec)
        if run_duration_sec.is_integer():
            run_duration_sec = int(run_duration_sec)
        snapshot_path = os.path.join(out_dir, "config_snapshot.json")
        try:
            with open(snapshot_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "variant_id": variant_id,
                        "hard_safety": True,
                        "soft_safety": bool(args.soft_guard_enabled),
                        "shield_enabled": False,
                        "online_learning": online_learning,
                        "run_duration_sec": run_duration_sec,
                        "rl_exploration_epsilon": float(args.rl_exploration_epsilon),
                        "rl_epsilon_schedule": str(args.rl_epsilon_schedule),
                        "rl_gamma": float(args.rl_gamma),
                        "rl_alpha": float(args.rl_alpha),
                        "soft_guard_enabled": bool(args.soft_guard_enabled),
                        "soft_guard_cap_value": float(args.soft_guard_cap_value),
                        "soft_guard_requires_safe_mode0": bool(args.soft_guard_requires_safe_mode0),
                        "near_stall_backlog_hi_bytes": int(args.near_stall_backlog_hi_bytes),
                        "near_stall_backlog_rise_window_sec": float(args.near_stall_backlog_rise_window_sec),
                        "near_stall_backlog_rise_hi_bytes": int(args.near_stall_backlog_rise_hi_bytes),
                        "near_stall_l0_th": int(args.near_stall_l0_th),
                        "near_stall_l0_backlog_min_bytes": int(args.near_stall_l0_backlog_min_bytes),
                        "near_stall_l0_force_th": int(args.near_stall_l0_force_th),
                        "near_stall_trigger_consecutive_sec": float(args.near_stall_trigger_consecutive_sec),
                        "semi_safe_min_on_sec": float(args.semi_safe_min_on_sec),
                        "semi_safe_release_backlog_lo_bytes": int(args.semi_safe_release_backlog_lo_bytes),
                        "semi_safe_release_backlog_rise_window_sec": float(args.semi_safe_release_backlog_rise_window_sec),
                        "semi_safe_release_backlog_rise_max_bytes": int(args.semi_safe_release_backlog_rise_max_bytes),
                        "semi_safe_release_l0_th": int(args.semi_safe_release_l0_th),
                        "semi_safe_release_hold_sec": float(args.semi_safe_release_hold_sec),
                        "semi_safe_cooldown_sec": float(args.semi_safe_cooldown_sec),
                        "hard_lock_backlog_bytes": int(args.hard_lock_backlog_bytes),
                        "hard_lock_release_backlog_bytes": int(args.hard_lock_release_backlog_bytes),
                        "hard_lock_release_hold_sec": float(args.hard_lock_release_hold_sec),
                        "state_scale_compaction_bytes": float(args.state_scale_compaction_bytes),
                        "state_scale_flush_bytes": float(args.state_scale_flush_bytes),
                        "state_scale_write_in_bps": float(args.state_scale_write_in_bps),
                        "state_scale_delay_bps": float(args.state_scale_delay_bps),
                        "state_scale_p99_us": float(args.state_scale_p99_us),
                        "state_scale_l0": float(args.state_scale_l0),
                        "state_clip": float(args.state_clip),
                        "fifo_heartbeat_sec": float(args.fifo_heartbeat_sec),
                        "fifo_resend_gap": float(args.fifo_resend_gap),
                        "fifo_resend_on_mismatch_enabled": bool(args.fifo_resend_on_mismatch_enabled),
                        "recover_controller_mode": str(args.recover_controller_mode),
                        "gamma_perf": float(args.gamma_perf),
                        "stall_penalty_C": float(args.stall_penalty_C),
                        "m_min": float(args.m_min),
                        "ladder": str(args.ladder),
                        "aimd_ai_step": aimd_ai_step,
                        "aimd_md_beta": aimd_md_beta,
                        "pid_signal": pid_signal,
                        "pid_backlog_target_bytes": float(args.pid_backlog_target_bytes),
                        "pid_backlog_scale_bytes": float(args.pid_backlog_scale_bytes),
                        "pid_l0_target_files": float(args.pid_l0_target_files),
                        "pid_l0_scale_files": float(args.pid_l0_scale_files),
                        "pid_kp": float(args.pid_kp),
                        "pid_ki": float(args.pid_ki),
                        "pid_kd": float(args.pid_kd),
                        "pid_integral_min": pid_integral_min,
                        "pid_integral_max": pid_integral_max,
                        "pid_output_max": pid_output_max,
                    },
                    f,
                    indent=2,
                )
                f.write("\n")
        except Exception:
            pass

    stop = {"flag": False}

    def _on_signal(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    header, _, _ = wait_for_metrics_header(
        metrics_csv, timeout_sec=10.0, poll_sec=0.25
    )
    last_cmd = args.m_min
    step_idx = 0
    reason_counts = Counter()
    last_stall_time = None
    ladder_idx_current = 0
    last_p99_us = None
    soft_guard_active = False
    soft_guard_until = 0.0
    semi_safe_release_streak_sec = 0.0
    hard_lock_active = False
    hard_lock_release_streak_sec = 0.0
    last_tick_ts = None
    last_backlog_bytes = None
    backlog_history: "deque[Tuple[float, float]]" = deque()
    soft_trigger_streak_sec = 0.0
    soft_trigger_latch_until = 0.0
    soft_guard_cooldown_ttl_sec = 0.0
    prev_fsm_state = "SAFE"
    prev_state = None
    prev_action = None
    prev_reward = None
    prev_done = None
    prev_is_trainable = False
    prev_write_stall_total_count = None
    prev_write_stall_hist_count = None
    last_fifo_sent_cmd = None
    last_fifo_send_ts = None
    aimd_pending_decrease = False
    aimd_last_decrease_base = float(args.m_min)
    pid_integral = 0.0
    pid_prev_error = None

    try:
        with open(args.out_csv, "w", encoding="utf-8", newline="") as out_f:
            writer = csv.writer(out_f)
            writer.writerow([
                # Core runtime / FSM state
                "ts",
                "step",
                "stall_event",
                "stall_delta",
                "is_write_stopped",
                "delayed_rate",
                "safe_mode",
                "safe_mode_ttl",
                "recover_mode",
                "prev_fsm_state",
                "next_fsm_state",
                "fsm_state",
                "recover_controller_mode",
                "stallfree_streak_sec",
                "ladder_idx",
                # Soft-guard signals
                "backlog_bytes_used",
                "l0_files_used",
                "delayed_rate_used",
                "soft_stall_event",
                "soft_guard_active",
                "soft_guard_ttl_sec",
                "soft_guard_cooldown_ttl_sec",
                "soft_trigger_raw",
                "soft_trigger",
                "soft_trigger_streak_sec",
                "soft_trigger_mask",
                "soft_trigger_reason",
                "soft_l0_files_used",
                "soft_compaction_backlog_bytes",
                "soft_pending_flush_bytes",
                "soft_backlog_delta_bytes",
                # RL outputs and reward decomposition
                "m_cmd_ladder",
                "delta_action_idx",
                "delta_value",
                "m_target",
                "m_cmd_candidate",
                "m_cmd_effective",
                "m_applied",
                "pid_error",
                "pid_integral",
                "pid_derivative",
                "pid_output",
                "r_total",
                "r_stall",
                "r_base",
                "r_perf_bonus",
                "r_risk_penalty_total",
                "risk_backlog_component",
                "risk_latency_component",
                "state_dim",
                "state_norm",
                "action_idx",
                "q_selected",
                "q_max",
                "td_error",
                "num_updates",
                "epsilon",
                # Training / provenance
                "policy_version_id",
                "update_id",
                "policy_rolled_back",
                "rl_training_mask",
                "reason",
                "fifo_send",
                "fifo_send_reason",
                "fifo_write_ok",
                "fifo_error",
                "metrics_row_ts",
                # Control-loop timing
                "loop_duration_ms",
                "decision_latency_ms",
                "actuation_latency_ms",
                # Debug views
                "time_since_start_sec",
                "soft_guard_enabled",
                "near_stall_signal",
                "write_stall_delta_count",
                "actual_delayed_write_rate_bps",
            ])
            out_f.flush()

            flush_every = int(
                max(1, round(args.log_flush_sec / max(args.period_sec, 1e-6)))
            )

            start_ts = time.monotonic()
            while not stop["flag"] and (time.monotonic() - start_ts) < args.timeout_sec:
                try:
                    loop_t0_ns = time.perf_counter_ns()
                    decision_t0_ns = 0
                    decision_t1_ns = 0
                    act_t0_ns = 0
                    act_t1_ns = 0
                    # --- main control tick ---
                    row = read_last_complete_row(metrics_csv, header) or {}
                    metrics_ts = row.get("ts", "")
    
                    is_write_stopped_raw = row.get("is_write_stopped")
                    stall_delta_raw = row.get("write_stall_delta_count")
                    stall_total_raw = row.get("write_stall_total_count")
                    stall_hist_raw = row.get("write_stall_hist_count")
                    delayed_rate_raw = row.get("actual_delayed_write_rate_bps")
    
                    is_write_stopped = int(to_float(is_write_stopped_raw, 0.0))
                    stall_delta = to_float(stall_delta_raw, 0.0)
                    stall_total = to_float(stall_total_raw, 0.0)
                    stall_hist = to_float(stall_hist_raw, 0.0)
                    delayed_rate = to_float(delayed_rate_raw, 0.0)
                    m_applied = to_float(row.get("write_multiplier_applied"), 0.0)
                    p99_us = to_float(row.get("write_lat_p99_us"), 0.0)
                    write_stall_delta_count_log = stall_delta if has_metric_value(stall_delta_raw) else -1.0
                    actual_delayed_write_rate_bps_log = delayed_rate if has_metric_value(delayed_rate_raw) else -1.0
                    is_write_stopped_log = is_write_stopped if has_metric_value(is_write_stopped_raw) else -1
                    backlog_metric_present = False
                    backlog = to_float(row.get("pending_compaction_bytes"), 0.0)
                    if has_metric_value(row.get("pending_compaction_bytes")):
                        backlog_metric_present = True
                    if backlog <= 0.0:
                        val = row.get("true_pending_compaction_bytes")
                        backlog = to_float(val, backlog)
                        if has_metric_value(val):
                            backlog_metric_present = True
                    if backlog <= 0.0:
                        val = row.get("estimate_pending_compaction_bytes")
                        backlog = to_float(val, backlog)
                        if has_metric_value(val):
                            backlog_metric_present = True
                    l0_metric_present = False
                    l0_files = to_float(row.get("num_level0_files"), 0.0)
                    if has_metric_value(row.get("num_level0_files")):
                        l0_metric_present = True
                    if l0_files <= 0.0:
                        val = row.get("l0_file_count")
                        l0_files = to_float(val, l0_files)
                        if has_metric_value(val):
                            l0_metric_present = True
    
                    pending_flush_raw = row.get("true_pending_flush_bytes")
                    pending_flush_bytes = to_float(pending_flush_raw, -1.0)
                    pending_flush_present = has_metric_value(pending_flush_raw)

                    backlog_delta = 0.0
                    backlog_delta_valid = False
                    if last_backlog_bytes is not None and backlog_metric_present:
                        backlog_delta = max(0.0, backlog - last_backlog_bytes)
                        backlog_delta_valid = True

                    stall_total_increased = False
                    if has_metric_value(stall_total_raw):
                        if prev_write_stall_total_count is not None and stall_total > prev_write_stall_total_count:
                            stall_total_increased = True
                        prev_write_stall_total_count = stall_total

                    stall_hist_increased = False
                    if has_metric_value(stall_hist_raw):
                        if prev_write_stall_hist_count is not None and stall_hist > prev_write_stall_hist_count:
                            stall_hist_increased = True
                        prev_write_stall_hist_count = stall_hist

                    stall_counter_increased = (
                        stall_delta > 0.0 or stall_total_increased or stall_hist_increased
                    )
                    stall_event = 1 if (stall_counter_increased or is_write_stopped == 1 or delayed_rate > 0.0) else 0
                    soft_stall_event = 1 if delayed_rate > 0.0 else 0
                    now = time.monotonic()

                    # Build a backlog history window for near-stall and release trend checks.
                    if backlog_metric_present:
                        backlog_history.append((now, backlog))
                    max_hist_window = max(
                        float(args.near_stall_backlog_rise_window_sec),
                        float(args.semi_safe_release_backlog_rise_window_sec),
                        1.0,
                    )
                    hist_cutoff = now - max_hist_window - 5.0
                    while backlog_history and backlog_history[0][0] < hist_cutoff:
                        backlog_history.popleft()

                    def backlog_window_delta(window_sec: float) -> Tuple[float, bool]:
                        if not backlog_metric_present or not backlog_history:
                            return (0.0, False)
                        target_ts = now - max(0.0, float(window_sec))
                        if backlog_history[0][0] > target_ts:
                            return (0.0, False)
                        base_val = backlog_history[0][1]
                        for ts_hist, val_hist in backlog_history:
                            if ts_hist >= target_ts:
                                base_val = val_hist
                                break
                        return (backlog - base_val, True)

                    near_stall_backlog_rise, near_stall_rise_valid = backlog_window_delta(
                        float(args.near_stall_backlog_rise_window_sec)
                    )
                    release_backlog_rise, release_rise_valid = backlog_window_delta(
                        float(args.semi_safe_release_backlog_rise_window_sec)
                    )

                    soft_trigger_mask = 0
                    soft_trigger_reasons: List[str] = []
                    soft_missing: List[str] = []

                    if backlog_metric_present:
                        if backlog >= float(args.near_stall_backlog_hi_bytes):
                            soft_trigger_mask |= 1 << 0
                            soft_trigger_reasons.append("BACKLOG_HI")
                        if near_stall_rise_valid:
                            if near_stall_backlog_rise >= float(args.near_stall_backlog_rise_hi_bytes):
                                soft_trigger_mask |= 1 << 1
                                soft_trigger_reasons.append("BACKLOG_RISE")
                        else:
                            soft_missing.append("BACKLOG_RISE_WINDOW")
                        if l0_metric_present:
                            if (
                                l0_files >= float(args.near_stall_l0_th)
                                and backlog >= float(args.near_stall_l0_backlog_min_bytes)
                            ):
                                soft_trigger_mask |= 1 << 2
                                soft_trigger_reasons.append("L0_BACKLOG")
                        else:
                                soft_missing.append("L0")
                    else:
                        soft_missing.append("BACKLOG")
                        if not l0_metric_present:
                            soft_missing.append("L0")

                    # Optional hard L0 near-stall trigger, independent from backlog level.
                    if (
                        l0_metric_present
                        and float(args.near_stall_l0_force_th) > 0.0
                        and l0_files >= float(args.near_stall_l0_force_th)
                    ):
                        soft_trigger_mask |= 1 << 3
                        soft_trigger_reasons.append("L0_FORCE")

                    l0_force_triggered = (soft_trigger_mask & (1 << 3)) != 0

                    soft_trigger_raw = 1 if soft_trigger_mask != 0 else 0
                    # Brief latch to absorb 1-2 tick flaps in near-stall signals.
                    if soft_trigger_raw:
                        soft_trigger_latch_until = now + max(args.period_sec * 2.0, 1.0)
                    soft_trigger_effective = 1 if (soft_trigger_raw or now < soft_trigger_latch_until) else 0
                    near_stall_signal = 1 if soft_trigger_effective == 1 else 0
                    if not soft_trigger_reasons and soft_missing:
                        soft_trigger_reason = "|".join(f"MISSING_{m}" for m in soft_missing)
                    elif soft_missing:
                        soft_trigger_reason = "|".join(soft_trigger_reasons + [f"MISSING_{m}" for m in soft_missing])
                    else:
                        soft_trigger_reason = "|".join(soft_trigger_reasons) if soft_trigger_reasons else "NONE"

                    time_since_start_sec = max(0.0, now - start_ts)
                    if args.rl_epsilon_schedule == "constant":
                        q_agent.epsilon = float(args.rl_exploration_epsilon)
                    elif time_since_start_sec < 180.0:
                        q_agent.epsilon = float(args.rl_exploration_epsilon)
                    else:
                        q_agent.epsilon = 0.1
                    if last_tick_ts is None:
                        tick_dt = 0.0
                    else:
                        tick_dt = max(0.0, now - last_tick_ts)
                    last_tick_ts = now
                    startup_force_active = (now - start_ts) < args.startup_force_sec

                    # soft trigger consecutive requirement
                    if soft_trigger_effective:
                        soft_trigger_streak_sec += tick_dt
                    else:
                        soft_trigger_streak_sec = 0.0
                    if l0_force_triggered:
                        # L0_FORCE reacts immediately once threshold is reached.
                        soft_trigger = 1
                    else:
                        soft_trigger = 1 if soft_trigger_streak_sec >= float(args.near_stall_trigger_consecutive_sec) else 0
    
                    if stall_event:
                        if args.recover_controller_mode == "AIMD_STALL_ONLY":
                            aimd_last_decrease_base = clamp(float(last_cmd), args.m_min, args.m_max)
                            aimd_pending_decrease = True
                        last_stall_time = now
                        ladder_idx_current = 0

                    safe_mode = False
                    safe_ttl = 0.0
                    if last_stall_time is not None:
                        elapsed = now - last_stall_time
                        if elapsed < args.hold_sec:
                            safe_mode = True
                            safe_ttl = max(0.0, args.hold_sec - elapsed)
    
                    # near-stall driven SEMI_SAFE latch (stall signals are excluded from trigger path)
                    if args.soft_guard_enabled:
                        # cooldown countdown
                        soft_guard_cooldown_ttl_sec = max(0.0, soft_guard_cooldown_ttl_sec - tick_dt)
                        stall_delta_increased = stall_counter_increased

                        if backlog_metric_present and backlog >= float(args.hard_lock_backlog_bytes):
                            hard_lock_active = True
                            hard_lock_release_streak_sec = 0.0

                        if (
                            not soft_guard_active
                            and soft_trigger
                            and soft_guard_cooldown_ttl_sec <= 0.0
                            and (not args.soft_guard_requires_safe_mode0 or not safe_mode)
                        ):
                            soft_guard_active = True
                            soft_guard_until = now + float(args.semi_safe_min_on_sec)
                            semi_safe_release_streak_sec = 0.0

                        if soft_guard_active:
                            hard_lock_release_ok = (
                                hard_lock_active
                                and backlog_metric_present
                                and backlog <= float(args.hard_lock_release_backlog_bytes)
                                and not safe_mode
                                and delayed_rate <= 0.0
                                and not stall_delta_increased
                            )
                            if hard_lock_release_ok:
                                hard_lock_release_streak_sec += tick_dt
                            else:
                                hard_lock_release_streak_sec = 0.0
                            if (
                                hard_lock_active
                                and hard_lock_release_streak_sec >= float(args.hard_lock_release_hold_sec)
                            ):
                                hard_lock_active = False
                                hard_lock_release_streak_sec = 0.0

                            release_conditions_met = (
                                now >= soft_guard_until
                                and not hard_lock_active
                                and backlog_metric_present
                                and l0_metric_present
                                and release_rise_valid
                                and backlog <= float(args.semi_safe_release_backlog_lo_bytes)
                                and release_backlog_rise <= float(args.semi_safe_release_backlog_rise_max_bytes)
                                and l0_files <= float(args.semi_safe_release_l0_th)
                                and not safe_mode
                                and delayed_rate <= 0.0
                                and not stall_delta_increased
                            )
                            if release_conditions_met:
                                semi_safe_release_streak_sec += tick_dt
                            else:
                                semi_safe_release_streak_sec = 0.0
                            if semi_safe_release_streak_sec >= float(args.semi_safe_release_hold_sec):
                                soft_guard_active = False
                                soft_guard_cooldown_ttl_sec = float(args.semi_safe_cooldown_sec)
                                semi_safe_release_streak_sec = 0.0
                    else:
                        soft_guard_active = False
                        hard_lock_active = False
                        hard_lock_release_streak_sec = 0.0
                        semi_safe_release_streak_sec = 0.0

                    if stall_event or safe_mode:
                        fsm_state = "UNSAFE"
                    elif soft_guard_active:
                        fsm_state = "SEMI_SAFE"
                    else:
                        fsm_state = "SAFE"
                    next_fsm_state = fsm_state

                    if last_stall_time is None:
                        stall_free_elapsed = now - start_ts
                    else:
                        stall_free_elapsed = max(0.0, now - last_stall_time)

                    time_since_unsafe_sec = stall_free_elapsed
                    s_t = make_state(row, fsm_state, last_cmd, time_since_unsafe_sec, args)
                    state_dim_log = int(s_t.shape[0]) if isinstance(s_t, np.ndarray) else 0
                    state_norm_log = float(np.linalg.norm(s_t)) if state_dim_log > 0 else 0.0

                    td_error = 0.0
                    q_selected = 0.0
                    q_next = 0.0
                    if (
                        prev_is_trainable
                        and prev_state is not None
                        and prev_action is not None
                        and prev_reward is not None
                        and prev_done is not None
                    ):
                        td_error, q_selected, q_next = q_agent.update(
                            prev_state,
                            prev_action,
                            prev_reward,
                            s_t,
                            prev_done,
                        )

                    recover_ready = (not safe_mode) and (stall_free_elapsed >= args.recover_free_sec)
                    recover_mode = "RECOVER" if recover_ready else "SAFE_HOLD"

                    # deterministic ladder progression (kept for logging/compat)
                    if recover_ready:
                        ladder_idx_current = min(
                            len(ladder) - 1,
                            int((stall_free_elapsed - args.recover_free_sec) // max(args.recover_step_sec, 1.0)) + 1,
                        )
                    else:
                        ladder_idx_current = 0
                    m_cmd_ladder = ladder[ladder_idx_current]

                    backlog_bytes_used = backlog
                    l0_files_used = l0_files
                    delayed_rate_used = delayed_rate

                    # Candidate decision (controller-mode specific)
                    decision_t0_ns = time.perf_counter_ns()
                    reason = "RULE_ONLY"
                    delta_action_idx = -1
                    delta_value = 0.0
                    m_target = -1.0
                    q_max = 0.0
                    pid_error_log = 0.0
                    pid_integral_log = pid_integral
                    pid_derivative_log = 0.0
                    pid_output_log = 0.0
                    actuation_gap = abs(float(m_applied) - float(last_cmd))
                    actuation_synced = actuation_gap <= float(args.fifo_resend_gap)
                    trainable = (
                        args.recover_controller_mode == "RL_DELTA_M"
                        and fsm_state == "SAFE"
                        and not safe_mode
                        and stall_event == 0
                        and not startup_force_active
                        and not soft_guard_active
                        and actuation_synced
                    )
                    if startup_force_active:
                        reason = "STARTUP_FORCE"
                        m_candidate = args.m_min
                        pid_integral = 0.0
                        pid_prev_error = None
                    else:
                        semi_safe_floor = clamp(float(args.semi_safe_floor), args.m_min, args.m_max)
                        if fsm_state == "UNSAFE":
                            reason = "UNSAFE"
                            m_candidate = args.m_min
                            pid_integral = 0.0
                            pid_prev_error = None
                        elif fsm_state == "SEMI_SAFE":
                            reason = "SEMI_SAFE"
                            m_candidate = max(semi_safe_floor, float(last_cmd) - float(args.semi_safe_step))
                            m_candidate = min(m_candidate, float(args.soft_guard_cap_value))
                            pid_integral = 0.0
                            pid_prev_error = None
                        else:
                            if args.recover_controller_mode == "AIMD_STALL_ONLY":
                                if aimd_pending_decrease:
                                    m_candidate = clamp(
                                        float(aimd_last_decrease_base) * aimd_md_beta,
                                        args.m_min,
                                        args.m_max,
                                    )
                                    reason = "AIMD_DECREASE"
                                    aimd_pending_decrease = False
                                else:
                                    m_candidate = min(args.m_max, float(last_cmd) + aimd_ai_step)
                                    reason = "AIMD_INCREASE"
                                delta_value = m_candidate - float(last_cmd)
                                m_target = m_candidate
                                pid_integral = 0.0
                                pid_prev_error = None
                            elif args.recover_controller_mode == "PID":
                                pid_metric_present = l0_metric_present if pid_signal_is_l0 else backlog_metric_present
                                if pid_metric_present:
                                    pid_dt = tick_dt if tick_dt > 0.0 else max(float(args.period_sec), 1e-6)
                                    if pid_signal_is_l0:
                                        pid_measure = float(l0_files)
                                        pid_target = float(args.pid_l0_target_files)
                                        pid_scale = max(1.0, float(args.pid_l0_scale_files))
                                    else:
                                        pid_measure = float(backlog)
                                        pid_target = float(args.pid_backlog_target_bytes)
                                        pid_scale = max(1.0, float(args.pid_backlog_scale_bytes))
                                    pid_error = (pid_target - pid_measure) / pid_scale
                                    pid_integral = clamp(
                                        pid_integral + pid_error * pid_dt,
                                        pid_integral_min,
                                        pid_integral_max,
                                    )
                                    pid_derivative = 0.0
                                    if pid_prev_error is not None and pid_dt > 0.0:
                                        pid_derivative = (pid_error - pid_prev_error) / pid_dt
                                    pid_output = (
                                        float(args.pid_kp) * pid_error
                                        + float(args.pid_ki) * pid_integral
                                        + float(args.pid_kd) * pid_derivative
                                    )
                                    pid_output = clamp(pid_output, -pid_output_max, pid_output_max)
                                    pid_prev_error = pid_error
                                    pid_error_log = pid_error
                                    pid_integral_log = pid_integral
                                    pid_derivative_log = pid_derivative
                                    pid_output_log = pid_output
                                    delta_value = pid_output
                                    m_target = clamp(float(last_cmd) + pid_output, args.m_min, args.m_max)
                                    m_candidate = m_target
                                    reason = pid_reason
                                else:
                                    pid_integral = 0.0
                                    pid_prev_error = None
                                    pid_integral_log = pid_integral
                                    m_candidate = float(last_cmd)
                                    m_target = m_candidate
                                    reason = pid_no_metric_reason
                            elif trainable:
                                action_idx, qs = q_agent.select_action(s_t)
                                q_max = float(np.max(qs)) if qs.size > 0 else 0.0
                                delta_action_idx = max(0, min(len(delta_actions) - 1, int(action_idx)))
                                raw_delta = float(delta_actions[delta_action_idx])
                                delta_value = clamp(
                                    raw_delta,
                                    -float(args.delta_m_max),
                                    float(args.delta_m_max),
                                )
                                m_target = clamp(float(last_cmd) + delta_value, args.m_min, args.m_max)
                                m_candidate = m_target
                                reason = "RL_DELTA_Q"
                            else:
                                m_candidate = float(last_cmd)
                                m_target = m_candidate
                                reason = "SAFE_NO_RL"
                                pid_integral = 0.0
                                pid_prev_error = None

                    m_candidate = clamp(float(m_candidate), args.m_min, args.m_max)
                    if m_target < 0.0:
                        m_target = m_candidate
                    delta_cmd = m_candidate - float(last_cmd)

                    # HARD SAFETY OVERRIDE (final cap).
                    override_reason = ""
                    if safe_mode or stall_event:
                        override_reason = "STALL_EVENT"
                    if startup_force_active:
                        override_reason = "STARTUP_FORCE"

                    if override_reason:
                        m_cmd_effective = args.m_min
                        reason = "SAFE_HOLD" if override_reason != "STARTUP_FORCE" else "STARTUP_FORCE"
                    else:
                        m_cmd_effective = clamp(m_candidate, args.m_min, args.m_max)
                    decision_t1_ns = time.perf_counter_ns()

                    if safe_mode and abs(m_cmd_effective - args.m_min) > 1e-9:
                        break

                    # reward components
                    if stall_event:
                        r_stall = -float(args.stall_penalty_C)
                        r_base = 0.0
                        r_smooth = 0.0
                        r_perf_bonus = 0.0
                        risk_backlog = 0.0
                        risk_latency = 0.0
                        r_risk = 0.0
                        r_total = r_stall
                    else:
                        r_base = 1.0
                        r_smooth = -float(args.delta_smooth_eta) * abs(float(delta_cmd))
                        perf_proxy = m_applied if m_applied > 0.0 else m_cmd_effective
                        r_perf_bonus = float(args.gamma_perf) * float(perf_proxy)
                        backlog_norm = min(1.0, backlog / max(1.0, args.risk_backlog_ref_bytes))
                        risk_backlog = float(args.risk_backlog_eps) * backlog_norm
                        p99_delta = 0.0
                        if last_p99_us is not None and p99_us > last_p99_us:
                            p99_delta = p99_us - last_p99_us
                        lat_norm = min(1.0, p99_delta / max(1.0, args.risk_latency_ref_us))
                        risk_latency = float(args.risk_latency_eps) * lat_norm
                        r_risk = risk_backlog + risk_latency
                        r_stall = 0.0
                        r_total = r_base + r_perf_bonus + r_smooth - r_risk
                    last_p99_us = p99_us

                    done = bool(stall_event == 1 or fsm_state == "UNSAFE")
                    rl_training_mask = 1 if trainable else 0
                    policy_rolled_back = 0
                    if trainable and delta_action_idx >= 0:
                        prev_state = s_t.copy()
                        prev_action = int(delta_action_idx)
                        prev_reward = float(r_total)
                        prev_done = bool(done)
                        prev_is_trainable = True
                    else:
                        prev_state = None
                        prev_action = None
                        prev_reward = None
                        prev_done = None
                        prev_is_trainable = False

                    if args.recover_controller_mode == "RL_DELTA_M":
                        policy_version_id = "linear_q"
                    elif args.recover_controller_mode == "AIMD_STALL_ONLY":
                        policy_version_id = "aimd_stall_only"
                    else:
                        policy_version_id = pid_policy_version_id
    
                    # Send command to RocksDB via FIFO using send-on-change semantics.
                    # Also resend periodically (heartbeat) and on observed apply mismatch.
                    act_t0_ns = time.perf_counter_ns()
                    fifo_send = 0
                    fifo_send_reason = "skip_nochange"
                    if args.dry_run:
                        fifo_ok, fifo_err = (False, "dry_run")
                    else:
                        need_send = False
                        if (
                            last_fifo_sent_cmd is None
                            or abs(float(m_cmd_effective) - float(last_fifo_sent_cmd)) > 1e-9
                        ):
                            need_send = True
                            fifo_send_reason = "changed"
                        elif (
                            last_fifo_send_ts is None
                            or (now - float(last_fifo_send_ts)) >= float(args.fifo_heartbeat_sec)
                        ):
                            need_send = True
                            fifo_send_reason = "heartbeat"
                        elif (
                            args.fifo_resend_on_mismatch_enabled
                            and abs(float(m_applied) - float(m_cmd_effective)) > float(args.fifo_resend_gap)
                        ):
                            need_send = True
                            fifo_send_reason = "mismatch_resend"

                        if need_send:
                            fifo_send = 1
                            fifo_ok, fifo_err = _fifo_write(args.fifo, f"m {m_cmd_effective:.3f}\n")
                            if fifo_ok:
                                last_fifo_sent_cmd = float(m_cmd_effective)
                                last_fifo_send_ts = now
                        else:
                            fifo_ok, fifo_err = (True, "")
                    act_t1_ns = time.perf_counter_ns()
                    last_cmd = m_cmd_effective
    
                    # step accounting
                    reason_counts[reason] += 1
                    step_idx += 1

                    # write log row
                    time_since_start_str = f"{time_since_start_sec:.1f}"
                    soft_guard_enabled_str = "1" if args.soft_guard_enabled else "0"
                    near_stall_signal_str = "1" if near_stall_signal == 1 else "0"
                    soft_guard_cooldown_ttl_str = f"{soft_guard_cooldown_ttl_sec:.1f}"
                    soft_trigger_raw_str = "1" if soft_trigger_raw else "0"
                    soft_trigger_str = "1" if soft_trigger else "0"
                    soft_trigger_streak_str = f"{soft_trigger_streak_sec:.1f}"
                    soft_trigger_mask_str = str(soft_trigger_mask)
                    soft_trigger_reason_str = soft_trigger_reason
                    soft_l0_files_used = l0_files if l0_metric_present else -1.0
                    soft_l0_files_used_str = f"{soft_l0_files_used:.0f}" if soft_l0_files_used >= 0.0 else "-1"
                    soft_compaction_backlog_bytes = backlog if backlog_metric_present else -1.0
                    soft_compaction_backlog_bytes_str = (
                        f"{soft_compaction_backlog_bytes:.0f}" if soft_compaction_backlog_bytes >= 0.0 else "-1"
                    )
                    soft_pending_flush_bytes = pending_flush_bytes if pending_flush_present else -1.0
                    soft_pending_flush_bytes_str = (
                        f"{soft_pending_flush_bytes:.0f}" if soft_pending_flush_bytes >= 0.0 else "-1"
                    )
                    soft_backlog_delta_bytes = backlog_delta if backlog_delta_valid else 0.0
                    soft_backlog_delta_bytes_str = f"{soft_backlog_delta_bytes:.0f}"
                    q_selected_str = f"{q_selected:.6f}"
                    q_max_str = f"{q_max:.6f}"
                    td_error_str = f"{td_error:.6f}"
                    pid_error_str = f"{pid_error_log:.6f}"
                    pid_integral_str = f"{pid_integral_log:.6f}"
                    pid_derivative_str = f"{pid_derivative_log:.6f}"
                    pid_output_str = f"{pid_output_log:.6f}"
                    write_stall_delta_count_str = (
                        f"{write_stall_delta_count_log:.0f}" if write_stall_delta_count_log >= 0.0 else "-1"
                    )
                    actual_delayed_write_rate_bps_str = (
                        f"{actual_delayed_write_rate_bps_log:.1f}" if actual_delayed_write_rate_bps_log >= 0.0 else "-1"
                    )
                    loop_duration_ms = (time.perf_counter_ns() - loop_t0_ns) / 1e6
                    decision_latency_ms = (
                        (decision_t1_ns - decision_t0_ns) / 1e6
                        if decision_t0_ns > 0 and decision_t1_ns >= decision_t0_ns
                        else 0.0
                    )
                    actuation_latency_ms = (
                        (act_t1_ns - act_t0_ns) / 1e6
                        if act_t0_ns > 0 and act_t1_ns >= act_t0_ns
                        else 0.0
                    )
                    writer.writerow([
                    # Core runtime / FSM state
                    datetime.now().isoformat(timespec="milliseconds"),
                    str(step_idx),
                    str(stall_event),
                    f"{stall_delta:.0f}",
                    str(is_write_stopped_log),
                    f"{delayed_rate:.1f}",
                    "1" if safe_mode else "0",
                    f"{safe_ttl:.1f}",
                    recover_mode,
                    prev_fsm_state,
                    next_fsm_state,
                    fsm_state,
                    args.recover_controller_mode,
                    f"{stall_free_elapsed:.1f}",
                    str(ladder_idx_current),
                    # Soft-guard signals
                    f"{backlog_bytes_used:.0f}",
                    f"{l0_files_used:.0f}",
                    f"{delayed_rate_used:.1f}",
                    str(soft_stall_event),
                    "1" if soft_guard_active else "0",
                    f"{max(0.0, soft_guard_until - now):.1f}" if soft_guard_active else "0.0",
                    soft_guard_cooldown_ttl_str,
                    soft_trigger_raw_str,
                    soft_trigger_str,
                    soft_trigger_streak_str,
                    soft_trigger_mask_str,
                    soft_trigger_reason_str,
                    soft_l0_files_used_str,
                    soft_compaction_backlog_bytes_str,
                    soft_pending_flush_bytes_str,
                    soft_backlog_delta_bytes_str,
                    # RL outputs and reward decomposition
                    f"{m_cmd_ladder:.3f}",
                    str(delta_action_idx),
                    f"{delta_value:.3f}",
                    f"{m_target:.3f}" if m_target >= 0.0 else "-1",
                    f"{m_candidate:.3f}",
                    f"{m_cmd_effective:.3f}",
                    f"{m_applied:.3f}",
                    pid_error_str,
                    pid_integral_str,
                    pid_derivative_str,
                    pid_output_str,
                    f"{r_total:.3f}",
                    f"{r_stall:.3f}",
                    f"{r_base:.3f}",
                    f"{r_perf_bonus:.3f}",
                    f"{r_risk:.3f}",
                    f"{risk_backlog:.3f}",
                    f"{risk_latency:.3f}",
                    str(state_dim_log),
                    f"{state_norm_log:.6f}",
                    str(delta_action_idx),
                    q_selected_str,
                    q_max_str,
                    td_error_str,
                    str(q_agent.num_updates),
                    f"{q_agent.epsilon:.6f}",
                    # Training / provenance
                    policy_version_id,
                    str(q_agent.num_updates),
                    str(policy_rolled_back),
                    str(rl_training_mask),
                    reason,
                    str(fifo_send),
                    fifo_send_reason,
                    "1" if fifo_ok else "0",
                    fifo_err,
                    metrics_ts,
                    # Control-loop timing
                    f"{loop_duration_ms:.3f}",
                    f"{decision_latency_ms:.3f}",
                    f"{actuation_latency_ms:.3f}",
                    # Debug views
                    time_since_start_str,
                    soft_guard_enabled_str,
                    near_stall_signal_str,
                    write_stall_delta_count_str,
                    actual_delayed_write_rate_bps_str,
                    ])
                    out_f.flush()

                    if step_idx % flush_every == 0:
                        print(
                            f"[fifo_step] stall_delta={stall_delta:.0f} mode={reason} m_cmd={m_cmd_effective:.3f}"
                        )
                        if args.verbose:
                            total = max(1, sum(reason_counts.values()))
                            top = reason_counts.most_common(3)
                            print(f"[fifo_reason_top] {top} (total={total})")

                    if step_idx <= 5:
                        print(
                            f"[loop_tick] step={step_idx} loop_ms={loop_duration_ms:.3f} "
                            f"decision_ms={decision_latency_ms:.3f} actuation_ms={actuation_latency_ms:.3f}",
                            file=sys.stderr,
                            flush=True,
                        )

                    prev_fsm_state = fsm_state

                    if backlog_metric_present:
                        last_backlog_bytes = backlog

                    time.sleep(args.period_sec)

                except Exception as exc:
                    print(f"[loop_error] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                    break
    finally:
        _fifo_close()
        elapsed = time.monotonic() - start_ts
        print(f"[loop_end] steps={step_idx} elapsed_sec={elapsed:.1f}", file=sys.stderr, flush=True)

    if args.verbose and reason_counts:
        total = sum(reason_counts.values())
        print(f"[fifo_summary] total_steps={total} reasons={reason_counts.most_common(5)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
