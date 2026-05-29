// Example:
//   mkfifo /tmp/rl_cmd.fifo
//   ./rl_poller /mnt/sda1/rlrocksdb_log rl_options.ini rl_metrics.csv
//     --write_mb_per_sec=500 --cmd_fifo=/tmp/rl_cmd.fifo
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <limits>
#include <cctype>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <map>
#include <sys/stat.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <utility>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/options_util.h"

static std::atomic<bool> g_stop{false};
static std::atomic<bool> g_writer_started{false};
static std::atomic<uint64_t> g_put_ok_count{0};
static std::atomic<uint64_t> g_bytes_ok_total{0};
static std::atomic<bool> g_fifo_thread_running{false};
// Last command value (default 1.0 until first valid command arrives).
static std::atomic<double> g_last_cmd_multiplier{1.0};
// Last applied value (default 1.0).
static std::atomic<double> g_last_applied_multiplier{1.0};
// Minimum clamp for rl_write_multiplier, loaded from options (default 0.2).
static std::atomic<double> g_write_m_min{0.2};
static std::atomic<bool> g_warned_applied_read{false};
static std::mutex g_log_mu;
static std::mutex g_last_error_mu;
static std::string g_last_error;
static std::atomic<bool> g_warned_tflush_prop{false};
static std::atomic<bool> g_warned_tcomp_prop{false};
static std::atomic<bool> g_warned_stall_prop{false};
static std::atomic<bool> g_warned_stall_cf_fallback{false};

// YCSB logical-op latency histogram (10us buckets up to 5s; last bucket is 5s+).
static constexpr uint64_t kYcsbLatencyBucketUs = 10;
static constexpr uint64_t kYcsbLatencyMaxUs = 5000000;
static constexpr size_t kYcsbLatencyBucketCount =
    static_cast<size_t>(kYcsbLatencyMaxUs / kYcsbLatencyBucketUs) + 1;
static std::vector<std::atomic<uint64_t>> g_ycsb_latency_hist(
    kYcsbLatencyBucketCount);
static std::atomic<uint64_t> g_ycsb_logical_ops_total{0};
static std::atomic<uint64_t> g_ycsb_logical_latency_us_total{0};

static void OnSigInt(int) { g_stop.store(true); }

static bool GetUInt64Prop(rocksdb::DB* db, const std::string& key, uint64_t* out) {
  std::string value;
  if (!db->GetProperty(key, &value)) return false;
  try {
    // Many properties are returned as decimal strings.
    *out = static_cast<uint64_t>(std::stoull(value));
    return true;
  } catch (...) {
    return false;
  }
}

static bool GetDoubleProp(rocksdb::DB* db, const std::string& key, double* out) {
  std::string value;
  if (!db->GetProperty(key, &value)) return false;
  try {
    *out = std::stod(value);
    return true;
  } catch (...) {
    return false;
  }
}

static bool GetWriteStallTotals(rocksdb::DB* db, uint64_t* total_stops,
                                uint64_t* total_delays, bool* used_cf) {
  if (used_cf) {
    *used_cf = false;
  }
  std::map<std::string, std::string> values;
  if (db->GetMapProperty(rocksdb::DB::Properties::kDBWriteStallStats, &values)) {
    auto it_stops = values.find(rocksdb::WriteStallStatsMapKeys::TotalStops());
    auto it_delays = values.find(rocksdb::WriteStallStatsMapKeys::TotalDelays());
    if (it_stops != values.end() && it_delays != values.end()) {
      try {
        *total_stops = static_cast<uint64_t>(std::stoull(it_stops->second));
        *total_delays = static_cast<uint64_t>(std::stoull(it_delays->second));
        return true;
      } catch (...) {
        // fall through to string parse
      }
    }
  }
  // Fallback: parse string property.
  std::string value;
  auto parse_key = [&](const std::string& key, uint64_t* out) -> bool {
    const auto pos = value.find(key);
    if (pos == std::string::npos) return false;
    size_t i = pos + key.size();
    while (i < value.size() && (value[i] == ' ' || value[i] == ':')) ++i;
    size_t j = i;
    while (j < value.size() && std::isdigit(static_cast<unsigned char>(value[j]))) ++j;
    if (j == i) return false;
    try {
      *out = static_cast<uint64_t>(std::stoull(value.substr(i, j - i)));
      return true;
    } catch (...) {
      return false;
    }
  };
  if (db->GetProperty(rocksdb::DB::Properties::kDBWriteStallStats, &value)) {
    bool ok1 = parse_key("total-stops", total_stops);
    bool ok2 = parse_key("total-delays", total_delays);
    if (ok1 && ok2) {
      return true;
    }
  }

  // Fallback: try CF-scoped write stall stats on default CF.
  values.clear();
  if (db->GetMapProperty(rocksdb::DB::Properties::kCFWriteStallStats, &values)) {
    auto it_stops = values.find(rocksdb::WriteStallStatsMapKeys::TotalStops());
    auto it_delays = values.find(rocksdb::WriteStallStatsMapKeys::TotalDelays());
    if (it_stops != values.end() && it_delays != values.end()) {
      try {
        *total_stops = static_cast<uint64_t>(std::stoull(it_stops->second));
        *total_delays = static_cast<uint64_t>(std::stoull(it_delays->second));
        if (used_cf) {
          *used_cf = true;
        }
        return true;
      } catch (...) {
        // fall through to string parse
      }
    }
  }
  value.clear();
  if (db->GetProperty(rocksdb::DB::Properties::kCFWriteStallStats, &value)) {
    bool ok1 = parse_key("total-stops", total_stops);
    bool ok2 = parse_key("total-delays", total_delays);
    if (ok1 && ok2) {
      if (used_cf) {
        *used_cf = true;
      }
      return true;
    }
  }
  return false;
}

static std::string NowIsoMs() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto t = system_clock::to_time_t(now);
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
  std::tm tm{};
  localtime_r(&t, &tm);

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << std::setw(3) << std::setfill('0')
      << ms.count();
  return oss.str();
}

static double ClampMultiplier(double value) {
  double m_min = g_write_m_min.load(std::memory_order_relaxed);
  if (m_min < 0.0) {
    m_min = 0.0;
  }
  double clamped = value;
  if (clamped < m_min) {
    clamped = m_min;
  }
  if (clamped > 1.0) {
    clamped = 1.0;
  }
  return clamped;
}

static void LogStderrLine(const std::string& line) {
  std::lock_guard<std::mutex> lock(g_log_mu);
  std::cerr << line << "\n";
  std::cerr.flush();
}

static void LogStderrBlock(const std::string& block) {
  std::lock_guard<std::mutex> lock(g_log_mu);
  std::cerr << block;
  if (!block.empty() && block.back() != '\n') {
    std::cerr << "\n";
  }
  std::cerr.flush();
}

static void LogStdoutLine(const std::string& line) {
  std::lock_guard<std::mutex> lock(g_log_mu);
  std::cout << line << "\n";
  std::cout.flush();
}

static std::string TrimAscii(const std::string& input) {
  size_t start = 0;
  while (start < input.size() &&
         std::isspace(static_cast<unsigned char>(input[start]))) {
    ++start;
  }
  size_t end = input.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return input.substr(start, end - start);
}

static std::string ToLowerAscii(const std::string& input) {
  std::string out = input;
  for (char& c : out) {
    unsigned char uc = static_cast<unsigned char>(c);
    if (uc >= 'A' && uc <= 'Z') {
      c = static_cast<char>(uc - 'A' + 'a');
    }
  }
  return out;
}

static bool GetBoolProp(rocksdb::DB* db, const std::string& key, uint64_t* out) {
  std::string value;
  if (!db->GetProperty(key, &value)) return false;
  std::string trimmed = ToLowerAscii(TrimAscii(value));
  if (trimmed == "1" || trimmed == "true") {
    *out = 1;
    return true;
  }
  if (trimmed == "0" || trimmed == "false") {
    *out = 0;
    return true;
  }
  try {
    *out = static_cast<uint64_t>(std::stoull(trimmed));
    return true;
  } catch (...) {
    return false;
  }
}

// Escape control characters for log safety.
static std::string EscapeForLog(const std::string& input) {
  std::string out;
  out.reserve(input.size());
  for (unsigned char c : input) {
    switch (c) {
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '"':
        out.append("\\\"");
        break;
      default:
        if (c < 0x20 || c == 0x7f) {
          std::ostringstream oss;
          oss << "\\x" << std::hex << std::setw(2) << std::setfill('0')
              << static_cast<int>(c);
          out.append(oss.str());
        } else {
          out.push_back(static_cast<char>(c));
        }
        break;
    }
  }
  return out;
}

static std::string FormatDoubleForCsv(double value) {
  if (std::isnan(value)) {
    return "1.0";
  }
  return std::to_string(value);
}

static bool ReadAppliedMultiplier(rocksdb::DB* db, double* applied,
                                  std::string* source) {
  double value = 0.0;
  if (GetDoubleProp(db, "rocksdb.rl.write_multiplier_applied", &value)) {
    *applied = value;
    if (source) *source = "property";
    return true;
  }
  if (GetDoubleProp(db, "rocksdb.rl.write_multiplier", &value)) {
    *applied = value;
    if (source) *source = "property";
    return true;
  }
  if (GetDoubleProp(db, "rocksdb.rl.write_multiplier_current", &value)) {
    *applied = value;
    if (source) *source = "property";
    return true;
  }
  if (GetDoubleProp(db, "rocksdb.rl.write_multiplier_prev", &value)) {
    *applied = value;
    if (source) *source = "snapshot";
    return true;
  }
  if (source) *source = "last";
  return false;
}

static bool IsQuitCommand(const std::string& line) {
  const std::string trimmed = TrimAscii(line);
  if (trimmed.empty()) {
    return false;
  }
  const std::string lower = ToLowerAscii(trimmed);
  return lower == "q";
}

static bool ParseMultiplierCommand(const std::string& line, double* out,
                                   std::string* error) {
  size_t pos = 0;
  while (pos < line.size() &&
         std::isspace(static_cast<unsigned char>(line[pos]))) {
    ++pos;
  }
  if (pos >= line.size()) {
    return false;
  }
  const char cmd = line[pos];
  if (cmd != 'm' && cmd != 'M') {
    return false;
  }
  ++pos;
  if (pos >= line.size() ||
      !std::isspace(static_cast<unsigned char>(line[pos]))) {
    if (error) {
      *error = "invalid format (use: m <val>)";
    }
    return false;
  }

  std::string numeric;
  bool had_stray = false;
  for (; pos < line.size(); ++pos) {
    unsigned char c = static_cast<unsigned char>(line[pos]);
    if (std::isspace(c)) {
      continue;
    }
    if (c == ',') {
      numeric.push_back('.');
      continue;
    }
    if ((c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.' ||
        c == 'e' || c == 'E') {
      numeric.push_back(static_cast<char>(c));
      continue;
    }
    had_stray = true;
  }

  if (numeric.empty()) {
    if (error) {
      *error =
          "no numeric value found (try `m 0.5` or `m0.5`), input was: " + line;
    }
    return false;
  }
  if (had_stray) {
    if (error) {
      *error = "invalid format (unexpected characters)";
    }
    return false;
  }

  try {
    size_t parsed = 0;
    const double value = std::stod(numeric, &parsed);
    if (parsed != numeric.size()) {
      if (error) {
        *error = "invalid numeric value after parsing: " + numeric;
      }
      return false;
    }
    *out = value;
    return true;
  } catch (...) {
    if (error) {
      *error = "invalid numeric value: " + numeric;
    }
    return false;
  }
}

static bool ApplyMultiplier(rocksdb::DB* db, double m, const std::string& src,
                            double* applied_out,
                            std::string* applied_source) {
  // Scenario A: apply cmd and read back current applied value.
  m = ClampMultiplier(m);
  std::unordered_map<std::string, std::string> opts;
  opts["rl_write_multiplier"] = std::to_string(m);
  g_last_cmd_multiplier.store(m, std::memory_order_relaxed);
  auto s = db->SetDBOptions(opts);
  if (!s.ok()) {
    LogStderrLine("[" + src + "] SetDBOptions failed: " + s.ToString());
    return false;
  }

  double applied = m;
  std::string source;
  bool ok_applied = ReadAppliedMultiplier(db, &applied, &source);
  if (!ok_applied) {
    applied = g_last_applied_multiplier.load(std::memory_order_relaxed);
    source = "last";
  }
  if (ok_applied) {
    g_last_applied_multiplier.store(applied, std::memory_order_relaxed);
  }
  if (applied_out != nullptr) {
    *applied_out = applied;
  }
  if (applied_source != nullptr) {
    *applied_source = source;
  }
  return ok_applied;
}

// Reads user commands from stdin like:
//   m 0.5   -> Set multiplier to 0.5 using SetDBOptions
//   q       -> quit
static void CommandThread(rocksdb::DB* db) {
  LogStderrLine(
      "[cmd] Enter commands: `m <val>` to set rl_write_multiplier, `q` to quit");
  std::string line;
  while (!g_stop.load() && std::getline(std::cin, line)) {
    if (IsQuitCommand(line)) {
      g_stop.store(true);
      break;
    }
    double m = 0.0;
    std::string error;
    if (ParseMultiplierCommand(line, &m, &error)) {
      const double clamped = ClampMultiplier(m);
      double applied = clamped;
      std::string source;
      bool ok_applied = ApplyMultiplier(db, clamped, "cmd", &applied, &source);
      std::ostringstream oss;
      oss << "[cmd] applied: cmd=" << std::fixed << std::setprecision(2)
          << clamped
          << " applied=" << std::fixed << std::setprecision(2) << applied
          << " source=" << source;
      if (!ok_applied) {
        oss << " applied_read=ERR";
      }
      LogStderrLine(oss.str());
    } else if (!error.empty()) {
      LogStderrLine("[cmd] " + error);
    } else {
      LogStderrLine("[cmd] unknown command: " + line);
    }
  }
}

static bool EnsureFifo(const std::string& path) {
  if (path.empty()) {
    return true;
  }
  struct stat st {};
  if (::stat(path.c_str(), &st) == 0) {
    if (!S_ISFIFO(st.st_mode)) {
      LogStderrLine("[fifo] path exists but is not a FIFO: " + path);
      return false;
    }
  } else {
    if (::mkfifo(path.c_str(), 0666) != 0 && errno != EEXIST) {
      LogStderrLine(std::string("[fifo] mkfifo failed: ") +
                    std::strerror(errno));
      return false;
    }
  }
  if (::chmod(path.c_str(), 0666) != 0) {
    LogStderrLine(std::string("[fifo] chmod failed: ") +
                  std::strerror(errno));
    return false;
  }
  return true;
}

static void FifoCommandThread(rocksdb::DB* db, const std::string& fifo_path) {
  // FIFO command reader with latest-only apply:
  // if multiple `m` commands are queued, apply only the most recent one.
  g_fifo_thread_running.store(true);
  LogStderrLine("[fifo] listening on " + fifo_path);
  std::string rxbuf;
  while (!g_stop.load()) {
    int fd = ::open(fifo_path.c_str(), O_RDONLY | O_NONBLOCK);
    if (fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      LogStderrLine(std::string("[fifo] open failed: ") +
                    std::strerror(errno));
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    while (!g_stop.load()) {
      struct pollfd pfd {};
      pfd.fd = fd;
      pfd.events = POLLIN;
      int pr = ::poll(&pfd, 1, 200);
      if (pr < 0) {
        if (errno == EINTR) {
          continue;
        }
        LogStderrLine(std::string("[fifo] poll failed: ") +
                      std::strerror(errno));
        break;
      }
      if (pr == 0) {
        continue;
      }
      const bool has_readable = (pfd.revents & POLLIN) != 0;
      const bool has_terminal = (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0;
      if (!has_readable) {
        if (has_terminal) {
          break;
        }
        continue;
      }

      // Drain currently available bytes from FIFO in non-blocking mode.
      bool saw_eof = false;
      while (!g_stop.load()) {
        char buf[4096];
        ssize_t nread = ::read(fd, buf, sizeof(buf));
        if (nread < 0) {
          if (errno == EINTR) {
            continue;
          }
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
          }
          LogStderrLine(std::string("[fifo] read failed: ") +
                        std::strerror(errno));
          saw_eof = true;
          break;
        }
        if (nread == 0) {
          saw_eof = true;  // EOF
          break;
        }
        rxbuf.append(buf, static_cast<size_t>(nread));
      }

      bool saw_quit = false;
      bool has_cmd = false;
      size_t cmd_count = 0;
      double last_parsed = 0.0;
      double last_clamped = 0.0;
      std::string last_line_escaped;
      size_t newline_pos = 0;
      while ((newline_pos = rxbuf.find('\n')) != std::string::npos) {
        std::string line = rxbuf.substr(0, newline_pos);
        rxbuf.erase(0, newline_pos + 1);
        if (!line.empty() && line.back() == '\r') {
          line.pop_back();
        }
        const std::string line_trim = TrimAscii(line);
        if (line_trim.empty()) {
          continue;
        }
        const std::string line_escaped = EscapeForLog(line);
        if (IsQuitCommand(line_trim)) {
          g_stop.store(true);
          saw_quit = true;
          break;
        }
        double m = 0.0;
        std::string error;
        if (ParseMultiplierCommand(line_trim, &m, &error)) {
          has_cmd = true;
          cmd_count += 1;
          last_parsed = m;
          last_clamped = ClampMultiplier(m);
          last_line_escaped = line_escaped;
        } else {
          LogStderrLine("[fifo] unknown line=\"" + line_escaped + "\"");
        }
      }

      if (has_cmd && !saw_quit) {
        double applied = last_clamped;
        std::string source;
        bool ok_applied = ApplyMultiplier(db, last_clamped, "fifo", &applied,
                                          &source);
        std::ostringstream log_line;
        log_line << "[fifo] line=\"" << last_line_escaped << "\" parsed="
                 << std::fixed << std::setprecision(2) << last_parsed
                 << " clamped=" << std::fixed << std::setprecision(2)
                 << last_clamped << " cmd=" << std::fixed
                 << std::setprecision(2) << last_clamped
                 << " applied=" << std::fixed << std::setprecision(2)
                 << applied << " source=" << source
                 << " mode=latest_only"
                 << " batch_cmds=" << cmd_count
                 << " dropped=" << (cmd_count > 0 ? cmd_count - 1 : 0);
        if (!ok_applied) {
          log_line << " applied_read=ERR";
        }
        LogStderrLine(log_line.str());
      }

      if (g_stop.load()) {
        break;
      }
      if (saw_eof || has_terminal) {
        break;
      }
    }
    ::close(fd);
  }
  g_fifo_thread_running.store(false);
}

static int32_t ToInt32KeyId(uint64_t key_id) {
  return static_cast<int32_t>(
      key_id & static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
}

// Aligns with db_bench's fixed-size key generation style:
// encode integer bytes, then pad remaining bytes with '0'.
static std::string MakeFixedKey16(const std::string& key_prefix,
                                  int32_t key_id) {
  std::string key(16, '0');
  const size_t prefix_len = std::min<size_t>(key_prefix.size(), key.size());
  std::memcpy(&key[0], key_prefix.data(), prefix_len);
  const size_t remaining = key.size() - prefix_len;
  const uint32_t encoded_key = static_cast<uint32_t>(key_id);
  const size_t bytes_to_fill = std::min<size_t>(remaining, sizeof(encoded_key));
  for (size_t i = 0; i < bytes_to_fill; ++i) {
    const size_t shift = (bytes_to_fill - i - 1) * 8;
    key[prefix_len + i] =
        static_cast<char>((encoded_key >> static_cast<uint32_t>(shift)) & 0xFFu);
  }
  return key;
}

static std::string MakeYcsbKey(const std::string& key_prefix, bool fixed_key_16,
                               int32_t key_id) {
  return fixed_key_16 ? MakeFixedKey16(key_prefix, key_id)
                      : (key_prefix + std::to_string(key_id));
}

static void ResetYcsbLogicalMetrics() {
  g_ycsb_logical_ops_total.store(0, std::memory_order_relaxed);
  g_ycsb_logical_latency_us_total.store(0, std::memory_order_relaxed);
  for (auto& x : g_ycsb_latency_hist) {
    x.store(0, std::memory_order_relaxed);
  }
}

static void RecordYcsbLogicalOpLatencyUs(uint64_t latency_us) {
  g_ycsb_logical_ops_total.fetch_add(1, std::memory_order_relaxed);
  g_ycsb_logical_latency_us_total.fetch_add(latency_us,
                                            std::memory_order_relaxed);
  size_t bucket = static_cast<size_t>(latency_us / kYcsbLatencyBucketUs);
  if (bucket >= kYcsbLatencyBucketCount) {
    bucket = kYcsbLatencyBucketCount - 1;
  }
  g_ycsb_latency_hist[bucket].fetch_add(1, std::memory_order_relaxed);
}

static uint64_t YcsbLatencyBucketUpperBoundUs(size_t bucket) {
  if (bucket >= kYcsbLatencyBucketCount - 1) {
    return kYcsbLatencyMaxUs;
  }
  return (static_cast<uint64_t>(bucket) + 1) * kYcsbLatencyBucketUs - 1;
}

static uint64_t QuantileFromYcsbLatencyDeltas(
    const std::vector<std::pair<size_t, uint64_t>>& deltas,
    uint64_t total_count, double quantile) {
  if (total_count == 0 || deltas.empty()) {
    return 0;
  }
  if (quantile < 0.0) quantile = 0.0;
  if (quantile > 1.0) quantile = 1.0;
  uint64_t target = static_cast<uint64_t>(
      std::ceil(quantile * static_cast<double>(total_count)));
  if (target == 0) target = 1;
  uint64_t cumulative = 0;
  for (const auto& it : deltas) {
    cumulative += it.second;
    if (cumulative >= target) {
      return YcsbLatencyBucketUpperBoundUs(it.first);
    }
  }
  return YcsbLatencyBucketUpperBoundUs(deltas.back().first);
}

// Keeps a reusable data buffer and cycles through it for each value generation,
// which is the same pattern used by db_bench's RandomGenerator.
class DbBenchStyleValueGenerator {
 public:
  explicit DbBenchStyleValueGenerator(size_t value_size)
      : value_size_(value_size), pos_(0), rng_(301), dist_(0, 61) {
    const size_t data_size = std::max<size_t>(1024 * 1024, value_size_);
    data_.resize(data_size);
    static const char kAlnum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (size_t i = 0; i < data_size; ++i) {
      data_[i] = kAlnum[dist_(rng_)];
    }
  }

  rocksdb::Slice Generate() {
    if (value_size_ == 0) {
      return rocksdb::Slice();
    }
    if (pos_ + value_size_ > data_.size()) {
      pos_ = 0;
    }
    const rocksdb::Slice value(data_.data() + pos_, value_size_);
    pos_ += value_size_;
    return value;
  }

 private:
  size_t value_size_;
  size_t pos_;
  std::string data_;
  std::mt19937 rng_;
  std::uniform_int_distribution<int> dist_;
};

class ZipfGenerator {
 public:
  ZipfGenerator() = default;

  void Init(uint64_t min_key, uint64_t max_key) {
    if (max_key < min_key) {
      max_key = min_key;
    }
    base_ = static_cast<double>(min_key);
    items_ = static_cast<double>(max_key - min_key + 1);
    if (items_ < 1.0) {
      items_ = 1.0;
    }
    theta_ = 0.99;
    zeta2theta_ = Zeta(0.0, 2.0, 0.0);
    alpha_ = 1.0 / (1.0 - theta_);
    zetan_ = Zeta(0.0, items_, 0.0);
    if (zetan_ <= 0.0) {
      zetan_ = 1.0;
    }
    eta_ = (1.0 - std::pow(2.0 / items_, 1.0 - theta_)) /
           (1.0 - zeta2theta_ / zetan_);
  }

  uint64_t Next(std::mt19937_64* rng) const {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    const double u = dist(*rng);
    const double uz = u * zetan_;
    if (uz < 1.0) {
      return static_cast<uint64_t>(base_);
    }
    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return static_cast<uint64_t>(base_ + 1.0);
    }
    const double v = base_ + items_ * std::pow(eta_ * u - eta_ + 1.0, alpha_);
    const uint64_t key = static_cast<uint64_t>(v);
    const uint64_t min_key = static_cast<uint64_t>(base_);
    const uint64_t max_key = min_key + static_cast<uint64_t>(items_) - 1;
    return std::max(min_key, std::min(max_key, key));
  }

 private:
  static double Zeta(double start, double n, double initial_sum) {
    double sum = initial_sum;
    for (uint64_t i = static_cast<uint64_t>(start); i < static_cast<uint64_t>(n);
         ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i + 1), 0.99);
    }
    return sum;
  }

  double base_ = 0.0;
  double items_ = 1.0;
  double theta_ = 0.99;
  double alpha_ = 100.0;
  double zetan_ = 1.0;
  double eta_ = 0.0;
  double zeta2theta_ = 0.0;
};

enum class YcsbWorkload {
  kNone,
  kLoad,
  kLoadUniform,
  kA,
  kB,
  kC,
  kD,
  kE,
  kF,
  kR10W90,
  kR90W10,
  kR50W50,
};

static bool ParseYcsbWorkload(const std::string& value, YcsbWorkload* out) {
  const std::string w = ToLowerAscii(TrimAscii(value));
  if (w.empty() || w == "none" || w == "off") {
    *out = YcsbWorkload::kNone;
    return true;
  }
  if (w == "load") {
    *out = YcsbWorkload::kLoad;
    return true;
  }
  if (w == "load_uniform" || w == "load_uniform_random") {
    *out = YcsbWorkload::kLoadUniform;
    return true;
  }
  if (w == "a") {
    *out = YcsbWorkload::kA;
    return true;
  }
  if (w == "b") {
    *out = YcsbWorkload::kB;
    return true;
  }
  if (w == "c") {
    *out = YcsbWorkload::kC;
    return true;
  }
  if (w == "d") {
    *out = YcsbWorkload::kD;
    return true;
  }
  if (w == "e") {
    *out = YcsbWorkload::kE;
    return true;
  }
  if (w == "f") {
    *out = YcsbWorkload::kF;
    return true;
  }
  if (w == "r10w90") {
    *out = YcsbWorkload::kR10W90;
    return true;
  }
  if (w == "r90w10") {
    *out = YcsbWorkload::kR90W10;
    return true;
  }
  if (w == "r50w50") {
    *out = YcsbWorkload::kR50W50;
    return true;
  }
  return false;
}

static const char* YcsbWorkloadName(YcsbWorkload w) {
  switch (w) {
    case YcsbWorkload::kLoad:
      return "load";
    case YcsbWorkload::kLoadUniform:
      return "load_uniform";
    case YcsbWorkload::kA:
      return "a";
    case YcsbWorkload::kB:
      return "b";
    case YcsbWorkload::kC:
      return "c";
    case YcsbWorkload::kD:
      return "d";
    case YcsbWorkload::kE:
      return "e";
    case YcsbWorkload::kF:
      return "f";
    case YcsbWorkload::kR10W90:
      return "r10w90";
    case YcsbWorkload::kR90W10:
      return "r90w10";
    case YcsbWorkload::kR50W50:
      return "r50w50";
    case YcsbWorkload::kNone:
    default:
      return "none";
  }
}

static void WriterThread(rocksdb::DB* db, double write_mb_per_sec,
                         int value_size, const std::string& key_prefix,
                         bool fixed_key_16) {
  g_writer_started.store(true);
  LogStderrLine("[writer] started");
  const double target_bps = write_mb_per_sec * 1024.0 * 1024.0;
  std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<int32_t> key_dist(
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
  rocksdb::WriteOptions write_options;
  DbBenchStyleValueGenerator value_gen(static_cast<size_t>(value_size));
  uint64_t bytes_ok = 0;
  uint64_t last_report_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto last_report = start;
  while (!g_stop.load()) {
    const int32_t key_id = key_dist(rng);
    const std::string key = MakeYcsbKey(key_prefix, fixed_key_16, key_id);
    const rocksdb::Slice value = value_gen.Generate();
    rocksdb::Status s = db->Put(write_options, key, value);
    if (!s.ok()) {
      {
        std::lock_guard<std::mutex> lock(g_last_error_mu);
        g_last_error = s.ToString();
      }
      LogStderrLine("[writer] Put failed: " + s.ToString());
      std::exit(2);
    }
    g_put_ok_count.fetch_add(1, std::memory_order_relaxed);
    bytes_ok += key.size() + value.size();
    g_bytes_ok_total.store(bytes_ok, std::memory_order_relaxed);
    if (target_bps > 0.0) {
      const auto now = std::chrono::steady_clock::now();
      const double elapsed =
          std::chrono::duration<double>(now - start).count();
      const double expected = static_cast<double>(bytes_ok) / target_bps;
      if (expected > elapsed) {
        std::this_thread::sleep_for(
            std::chrono::duration<double>(expected - elapsed));
      }
    }
    const auto now = std::chrono::steady_clock::now();
    if (now - last_report >= std::chrono::seconds(1)) {
      const double elapsed_sec =
          std::chrono::duration<double>(now - last_report).count();
      const uint64_t delta_bytes = bytes_ok - last_report_bytes;
      const double mbps =
          elapsed_sec > 0.0
              ? (static_cast<double>(delta_bytes) / (1024.0 * 1024.0)) /
                    elapsed_sec
              : 0.0;
      const uint64_t ok_cnt =
          g_put_ok_count.load(std::memory_order_relaxed);
      std::ostringstream oss;
      oss << "[writer] ok_puts=" << ok_cnt << " ok_MBps=" << mbps;
      LogStderrLine(oss.str());
      last_report = now;
      last_report_bytes = bytes_ok;
    }
  }
}

static uint64_t PickYcsbKey(std::mt19937_64* rng, ZipfGenerator* zipf,
                            uint64_t min_key, uint64_t max_key,
                            bool use_uniform) {
  if (max_key < min_key) {
    max_key = min_key;
  }
  if (use_uniform) {
    std::uniform_int_distribution<uint64_t> dist(min_key, max_key);
    return dist(*rng);
  }
  (void)min_key;
  return zipf->Next(rng);
}

static uint64_t PickYcsbLatestKey(std::mt19937_64* rng, ZipfGenerator* zipf,
                                  uint64_t min_key, uint64_t max_key,
                                  bool use_uniform) {
  if (max_key < min_key) {
    max_key = min_key;
  }
  if (use_uniform) {
    std::uniform_int_distribution<uint64_t> dist(min_key, max_key);
    return dist(*rng);
  }
  const uint64_t span = max_key - min_key + 1;
  const uint64_t z = zipf->Next(rng);
  const uint64_t off = std::min<uint64_t>(span - 1, z - min_key);
  return max_key - off;
}

static void YcsbThread(rocksdb::DB* db, YcsbWorkload workload,
                       uint64_t record_count, uint64_t operation_count,
                       int value_size, const std::string& key_prefix,
                       bool fixed_key_16, bool uniform_distribution,
                       uint64_t scan_max_len, uint64_t duration_sec) {
  g_writer_started.store(true);
  LogStderrLine(std::string("[ycsb] started workload=") +
                YcsbWorkloadName(workload));

  rocksdb::ReadOptions read_options;
  rocksdb::WriteOptions write_options;
  DbBenchStyleValueGenerator value_gen(static_cast<size_t>(value_size));
  std::string tmp_value;
  std::mt19937_64 rng{std::random_device{}()};
  ZipfGenerator zipf;
  const uint64_t max_key_id_i32 =
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
  const uint64_t init_records =
      std::max<uint64_t>(1, std::min<uint64_t>(record_count, max_key_id_i32));
  zipf.Init(1, init_records);
  uint64_t max_inserted_key = init_records;

  auto DoPut = [&](uint64_t key_id) {
    const std::string key =
        MakeYcsbKey(key_prefix, fixed_key_16, ToInt32KeyId(key_id));
    const rocksdb::Slice value = value_gen.Generate();
    rocksdb::Status s = db->Put(write_options, key, value);
    if (!s.ok()) {
      LogStderrLine("[ycsb] Put failed: " + s.ToString());
      std::exit(2);
    }
    g_put_ok_count.fetch_add(1, std::memory_order_relaxed);
    g_bytes_ok_total.fetch_add(key.size() + value.size(),
                               std::memory_order_relaxed);
  };

  auto DoRead = [&](uint64_t key_id) {
    const std::string key =
        MakeYcsbKey(key_prefix, fixed_key_16, ToInt32KeyId(key_id));
    rocksdb::Status s = db->Get(read_options, key, &tmp_value);
    if (!s.ok() && !s.IsNotFound()) {
      LogStderrLine("[ycsb] Get failed: " + s.ToString());
    }
  };

  auto DoScan = [&](uint64_t key_id, uint64_t scan_len) {
    const std::string key =
        MakeYcsbKey(key_prefix, fixed_key_16, ToInt32KeyId(key_id));
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options));
    uint64_t n = 0;
    for (it->Seek(key); n < scan_len && it->Valid(); it->Next()) {
      ++n;
    }
  };

  if (workload == YcsbWorkload::kLoad) {
    const auto deadline =
        (duration_sec > 0)
            ? (std::chrono::steady_clock::now() + std::chrono::seconds(duration_sec))
            : std::chrono::steady_clock::time_point::max();
    const uint64_t end =
        std::max<uint64_t>(1, std::min<uint64_t>(record_count, max_key_id_i32));
    uint64_t next_key_id = 1;
    while (!g_stop.load()) {
      if (duration_sec > 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
          LogStderrLine("[ycsb] load duration completed");
          break;
        }
        if (next_key_id > max_key_id_i32) {
          LogStderrLine("[ycsb] load reached max key id before duration completed");
          break;
        }
      } else if (next_key_id > end) {
        LogStderrLine("[ycsb] load completed");
        break;
      }
      const auto op_start = std::chrono::steady_clock::now();
      DoPut(next_key_id++);
      const auto latency_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - op_start)
              .count());
      RecordYcsbLogicalOpLatencyUs(latency_us);
    }
    g_stop.store(true);
    return;
  }

  if (workload == YcsbWorkload::kLoadUniform) {
    const auto deadline =
        (duration_sec > 0)
            ? (std::chrono::steady_clock::now() + std::chrono::seconds(duration_sec))
            : std::chrono::steady_clock::time_point::max();
    const uint64_t max_key =
        std::max<uint64_t>(1, std::min<uint64_t>(record_count, max_key_id_i32));
    const uint64_t total_ops = std::max<uint64_t>(1, operation_count);
    std::uniform_int_distribution<uint64_t> key_dist(1, max_key);
    uint64_t op = 0;
    while (!g_stop.load()) {
      if (duration_sec > 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
          LogStderrLine("[ycsb] load_uniform duration completed");
          break;
        }
      } else if (op >= total_ops) {
        LogStderrLine("[ycsb] load_uniform operation_count completed");
        break;
      }
      const auto op_start = std::chrono::steady_clock::now();
      DoPut(key_dist(rng));
      const auto latency_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - op_start)
              .count());
      RecordYcsbLogicalOpLatencyUs(latency_us);
      ++op;
    }
    g_stop.store(true);
    return;
  }

  const uint64_t total_ops = std::max<uint64_t>(1, operation_count);
  std::uniform_int_distribution<int> op100(0, 99);
  std::uniform_int_distribution<uint64_t> scan_len_dist(
      1, std::max<uint64_t>(1, scan_max_len));
  uint64_t op = 0;
  const auto deadline =
      (duration_sec > 0)
          ? (std::chrono::steady_clock::now() + std::chrono::seconds(duration_sec))
          : std::chrono::steady_clock::time_point::max();

  while (!g_stop.load()) {
    if (duration_sec > 0) {
      if (std::chrono::steady_clock::now() >= deadline) {
        break;
      }
    } else if (op >= total_ops) {
      break;
    }
    const bool use_latest =
        (workload == YcsbWorkload::kD || workload == YcsbWorkload::kE);
    const uint64_t key_id =
        use_latest ? PickYcsbLatestKey(&rng, &zipf, 1, max_inserted_key,
                                       uniform_distribution)
                   : PickYcsbKey(&rng, &zipf, 1, max_inserted_key,
                                 uniform_distribution);

    const auto op_start = std::chrono::steady_clock::now();
    const int next = op100(rng);
    if (workload == YcsbWorkload::kA) {
      if (next < 50) {
        DoRead(key_id);
      } else {
        DoPut(key_id);
      }
    } else if (workload == YcsbWorkload::kB) {
      if (next < 95) {
        DoRead(key_id);
      } else {
        DoPut(key_id);
      }
    } else if (workload == YcsbWorkload::kC) {
      DoRead(key_id);
    } else if (workload == YcsbWorkload::kD) {
      if (next < 95) {
        DoRead(key_id);
      } else {
        if (max_inserted_key < max_key_id_i32) {
          ++max_inserted_key;
        }
        DoPut(max_inserted_key);
      }
    } else if (workload == YcsbWorkload::kE) {
      if (next < 95) {
        DoScan(key_id, scan_len_dist(rng));
      } else {
        if (max_inserted_key < max_key_id_i32) {
          ++max_inserted_key;
        }
        DoPut(max_inserted_key);
      }
    } else if (workload == YcsbWorkload::kF) {
      if (next < 50) {
        DoRead(key_id);
      } else {
        DoRead(key_id);
        DoPut(key_id);
      }
    } else if (workload == YcsbWorkload::kR10W90) {
      if (next < 10) {
        DoRead(key_id);
      } else {
        DoPut(key_id);
      }
    } else if (workload == YcsbWorkload::kR90W10) {
      if (next < 90) {
        DoRead(key_id);
      } else {
        DoPut(key_id);
      }
    } else if (workload == YcsbWorkload::kR50W50) {
      if (next < 50) {
        DoRead(key_id);
      } else {
        DoPut(key_id);
      }
    }
    const auto latency_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - op_start)
            .count());
    RecordYcsbLogicalOpLatencyUs(latency_us);
    ++op;
  }
  LogStderrLine("[ycsb] run completed");
  g_stop.store(true);
}

int main(int argc, char** argv) {
  if (argc < 4) {
    std::ostringstream oss;
    oss << "Usage:\n"
        << "  " << argv[0] << " <db_path> <options_file> <csv_out>"
        << " [--write_mb_per_sec=<float>]"
        << " [--value_size=<int>]"
        << " [--key_prefix=<string>]"
        << " [--fixed_key_16=<0|1>]"
        << " [--create_if_missing=<0|1>]"
        << " [--cmd_fifo=/tmp/rl_cmd.fifo]\n\n"
        << " [--ycsb_workload=<load|load_uniform|a|b|c|d|e|f|r10w90|r90w10|r50w50>]"
        << " [--ycsb_record_count=<uint64>]"
        << " [--ycsb_operation_count=<uint64>]"
        << " [--ycsb_duration_sec=<uint64>]"
        << " [--ycsb_uniform_distribution=<0|1>]"
        << " [--ycsb_scan_max_len=<uint64>]\n\n"
        << "Example:\n"
        << "  " << argv[0]
        << " /mnt/sda1/rlrocksdb_log rl_options.ini rl_metrics.csv\n";
    LogStderrBlock(oss.str());
    return 1;
  }

  const std::string db_path = argv[1];
  const std::string options_file = argv[2];
  const std::string csv_out = argv[3];
  double write_mb_per_sec = 0.0;
  int value_size = 1024;
  std::string key_prefix = "k";
  int fixed_key_16_flag = 1;
  std::string cmd_fifo;
  int create_if_missing_flag = 1;
  YcsbWorkload ycsb_workload = YcsbWorkload::kNone;
  uint64_t ycsb_record_count = 100000;
  uint64_t ycsb_operation_count = 100000;
  uint64_t ycsb_duration_sec = 0;
  int ycsb_uniform_distribution_flag = 0;
  uint64_t ycsb_scan_max_len = 100;
  for (int i = 4; i < argc; ++i) {
    const std::string arg = argv[i];
    const std::string write_prefix = "--write_mb_per_sec=";
    const std::string value_prefix = "--value_size=";
    const std::string key_prefix_arg = "--key_prefix=";
    const std::string fixed_key_arg = "--fixed_key_16=";
    const std::string create_prefix = "--create_if_missing=";
    const std::string cmd_fifo_prefix = "--cmd_fifo=";
    const std::string ycsb_workload_prefix = "--ycsb_workload=";
    const std::string ycsb_record_prefix = "--ycsb_record_count=";
    const std::string ycsb_op_prefix = "--ycsb_operation_count=";
    const std::string ycsb_duration_prefix = "--ycsb_duration_sec=";
    const std::string ycsb_uniform_prefix = "--ycsb_uniform_distribution=";
    const std::string ycsb_scan_prefix = "--ycsb_scan_max_len=";
    if (arg.rfind(write_prefix, 0) == 0) {
      write_mb_per_sec = std::stod(arg.substr(write_prefix.size()));
    } else if (arg.rfind(value_prefix, 0) == 0) {
      value_size = std::stoi(arg.substr(value_prefix.size()));
    } else if (arg.rfind(key_prefix_arg, 0) == 0) {
      key_prefix = arg.substr(key_prefix_arg.size());
    } else if (arg.rfind(fixed_key_arg, 0) == 0) {
      fixed_key_16_flag = std::stoi(arg.substr(fixed_key_arg.size()));
    } else if (arg.rfind(create_prefix, 0) == 0) {
      create_if_missing_flag = std::stoi(arg.substr(create_prefix.size()));
    } else if (arg.rfind(cmd_fifo_prefix, 0) == 0) {
      cmd_fifo = arg.substr(cmd_fifo_prefix.size());
    } else if (arg.rfind(ycsb_workload_prefix, 0) == 0) {
      const std::string value = arg.substr(ycsb_workload_prefix.size());
      if (!ParseYcsbWorkload(value, &ycsb_workload)) {
        LogStderrLine("Unknown ycsb workload: " + value);
        return 1;
      }
    } else if (arg.rfind(ycsb_record_prefix, 0) == 0) {
      ycsb_record_count = static_cast<uint64_t>(
          std::stoull(arg.substr(ycsb_record_prefix.size())));
    } else if (arg.rfind(ycsb_op_prefix, 0) == 0) {
      ycsb_operation_count = static_cast<uint64_t>(
          std::stoull(arg.substr(ycsb_op_prefix.size())));
    } else if (arg.rfind(ycsb_duration_prefix, 0) == 0) {
      ycsb_duration_sec = static_cast<uint64_t>(
          std::stoull(arg.substr(ycsb_duration_prefix.size())));
    } else if (arg.rfind(ycsb_uniform_prefix, 0) == 0) {
      ycsb_uniform_distribution_flag =
          std::stoi(arg.substr(ycsb_uniform_prefix.size()));
    } else if (arg.rfind(ycsb_scan_prefix, 0) == 0) {
      ycsb_scan_max_len = static_cast<uint64_t>(
          std::stoull(arg.substr(ycsb_scan_prefix.size())));
    } else {
      LogStderrLine("Unknown arg: " + arg);
      return 1;
    }
  }
  const bool fixed_key_16 = (fixed_key_16_flag != 0);
  if (fixed_key_16 && key_prefix.size() > 16) {
    LogStderrLine("[startup] key_prefix length > 16 with fixed_key_16=1");
    return 1;
  }
  const bool ycsb_uniform_distribution = (ycsb_uniform_distribution_flag != 0);
  if (ycsb_workload != YcsbWorkload::kNone && write_mb_per_sec > 0.0) {
    LogStderrLine(
        "[startup] both writer and ycsb requested; ycsb mode will be used");
  }
  ResetYcsbLogicalMetrics();

  std::signal(SIGINT, OnSigInt);

  rocksdb::DB* db = nullptr;
  rocksdb::Options options;
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;

  rocksdb::ConfigOptions config_opts;
  config_opts.ignore_unknown_options = false; // fail fast if options file has typos

  auto s = rocksdb::LoadOptionsFromFile(config_opts, options_file, &options, &cf_descs);
  if (!s.ok()) {
    LogStderrLine("LoadOptionsFromFile failed: " + s.ToString());
    return 2;
  }
  if (!options.statistics) {
    options.statistics = rocksdb::CreateDBStatistics();
  }
  options.create_if_missing = (create_if_missing_flag != 0);
  double rl_write_m_min = options.rl_write_m_min;
  const double kMinAllowed = 0.01;
  const double kMaxAllowed = 1.0;
  if (rl_write_m_min < kMinAllowed) {
    LogStderrLine("[startup] rl_write_m_min too low; clamping to 0.01");
    rl_write_m_min = kMinAllowed;
  } else if (rl_write_m_min > kMaxAllowed) {
    LogStderrLine("[startup] rl_write_m_min too high; clamping to 1.00");
    rl_write_m_min = kMaxAllowed;
  }
  g_write_m_min.store(rl_write_m_min, std::memory_order_relaxed);

  // Open with DBOptions + CF options from the file.
  rocksdb::DBOptions db_options(options);
  s = rocksdb::DB::Open(db_options, db_path, cf_descs, &cf_handles, &db);
  if (!s.ok()) {
    LogStderrLine("DB::Open failed: " + s.ToString());
    return 3;
  }

  std::ofstream fout(csv_out);
  if (!fout) {
    LogStderrLine("Failed to open csv_out: " + csv_out);
    delete db;
    return 4;
  }

  {
    std::ostringstream oss;
    oss << "[startup] db_path=" << db_path
        << " create_if_missing=" << options.create_if_missing
        << " error_if_exists=" << options.error_if_exists
        << " statistics=" << (options.statistics ? "on" : "off")
        << " wal=enabled"
        << " read_write=yes";
    LogStderrLine(oss.str());
  }
  {
    std::ostringstream oss;
    oss << "[startup] rl_write_m_min=" << std::fixed << std::setprecision(3)
        << rl_write_m_min;
    LogStderrLine(oss.str());
  }
  uint64_t training_enabled = 0;
  uint64_t metrics_enabled = 0;
  if (GetUInt64Prop(db, "rocksdb.rl.training_enabled", &training_enabled)) {
    LogStderrLine(std::string("[startup] rl_training_enabled=") +
                  (training_enabled ? "true" : "false"));
  } else {
    LogStderrLine("[startup] rl_training_enabled property not available");
  }
  if (GetUInt64Prop(db, "rocksdb.rl.metrics_enabled", &metrics_enabled)) {
    LogStderrLine(std::string("[startup] rl_metrics_enabled=") +
                  (metrics_enabled ? "true" : "false"));
  } else {
    LogStderrLine("[startup] rl_metrics_enabled property not available");
  }
  const auto db_opts = db->GetDBOptions();
  LogStderrLine(std::string("[startup] rl_enable_training_instrumentation=") +
                (db_opts.rl_enable_training_instrumentation ? "true"
                                                           : "false"));
  std::string options_dump;
  if (db->GetProperty("rocksdb.options", &options_dump)) {
    LogStderrBlock("[startup] rocksdb.options:\n" + options_dump);
  } else if (db->GetProperty("rocksdb.options-statistics", &options_dump)) {
    LogStderrBlock("[startup] rocksdb.options-statistics:\n" + options_dump);
  } else {
    LogStderrLine("[startup] options property not available");
  }
  {
    std::ostringstream oss;
    oss << "[startup] mode="
        << (ycsb_workload != YcsbWorkload::kNone ? "ycsb" : "writer")
        << " writer=" << (write_mb_per_sec > 0.0 ? "on" : "off")
        << " target_mb_per_sec=" << write_mb_per_sec
        << " value_size=" << value_size
        << " key_prefix=" << key_prefix
        << " fixed_key_16=" << (fixed_key_16 ? "1" : "0");
    LogStderrLine(oss.str());
  }
  if (ycsb_workload != YcsbWorkload::kNone) {
    std::ostringstream oss;
    oss << "[startup] ycsb_workload=" << YcsbWorkloadName(ycsb_workload)
        << " ycsb_record_count=" << ycsb_record_count
        << " ycsb_operation_count=" << ycsb_operation_count
        << " ycsb_duration_sec=" << ycsb_duration_sec
        << " ycsb_uniform_distribution=" << (ycsb_uniform_distribution ? "1" : "0")
        << " ycsb_scan_max_len=" << ycsb_scan_max_len;
    LogStderrLine(oss.str());
  }
  if (!cmd_fifo.empty()) {
    LogStderrLine("[startup] cmd_fifo=" + cmd_fifo);
  }

  // CSV header
  fout << "ts,"
       << "l0_file_count,"
       << "imm_memtable_bytes,"
       << "write_lat_p99_us,"
       << "write_in_bytes_per_sec,"
       << "write_multiplier_prev,"
       << "write_multiplier_cmd,"
       << "write_multiplier_applied,"
       << "true_pending_flush_bytes,"
       << "true_pending_compaction_bytes,"
       << "is_write_stopped,"
       << "actual_delayed_write_rate_bps,"
       << "compaction_pending,"
       << "memtable_flush_pending,"
       << "num_immutable_mem_table,"
       << "estimate_pending_compaction_bytes,"
       << "write_stall_stop_count,"
       << "write_stall_delay_count,"
       << "write_stall_total_count,"
       << "write_stall_delta_count,"
       << "write_stall_hist_count,"
       << "write_stall_hist_delta_count,"
       << "ycsb_logical_ops_total,"
       << "ycsb_logical_ops_sec,"
       << "ycsb_logical_avg_lat_us,"
       << "ycsb_logical_p99_us,"
       << "ycsb_logical_p999_us,"
       << "ycsb_logical_p9999_us\n";
  fout.flush();

  std::thread fifo_thr;
  if (!cmd_fifo.empty()) {
    if (!EnsureFifo(cmd_fifo)) {
      LogStderrLine("[fifo] failed to create fifo: " + cmd_fifo);
      delete db;
      return 5;
    }
    fifo_thr = std::thread(FifoCommandThread, db, cmd_fifo);
  }

  // Start command thread (optional)
  std::thread cmd_thr(CommandThread, db);
  std::thread writer_thr;
  if (ycsb_workload != YcsbWorkload::kNone) {
    writer_thr = std::thread(
        YcsbThread, db, ycsb_workload, ycsb_record_count, ycsb_operation_count,
        value_size, key_prefix, fixed_key_16, ycsb_uniform_distribution,
        ycsb_scan_max_len, ycsb_duration_sec);
  } else if (write_mb_per_sec > 0.0) {
    writer_thr = std::thread(WriterThread, db, write_mb_per_sec, value_size,
                             key_prefix, fixed_key_16);
  }
  if (writer_thr.joinable()) {
    const auto writer_wait_start = std::chrono::steady_clock::now();
    while (!g_writer_started.load() &&
           std::chrono::steady_clock::now() - writer_wait_start <
               std::chrono::seconds(2)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!g_writer_started.load()) {
      LogStderrLine("[writer] did not start");
      std::exit(1);
    }
  }

  // Poll period: use 500ms for MVP to match sampler
  const auto period = std::chrono::milliseconds(500);
  auto last_builtin_report = std::chrono::steady_clock::now();
  uint64_t last_stall_total = 0;
  uint64_t last_stall_hist = 0;
  bool have_stall_totals = false;
  bool have_stall_hist = false;
  auto last_poll = std::chrono::steady_clock::now();
  uint64_t last_ycsb_logical_ops_total = 0;
  uint64_t last_ycsb_logical_latency_us_total = 0;
  std::vector<uint64_t> last_ycsb_latency_hist;
  if (ycsb_workload != YcsbWorkload::kNone) {
    last_ycsb_latency_hist.assign(kYcsbLatencyBucketCount, 0);
  }

  while (!g_stop.load()) {
    const auto tick_now = std::chrono::steady_clock::now();
    const double tick_elapsed_sec =
        std::chrono::duration<double>(tick_now - last_poll).count();
    last_poll = tick_now;
    const std::string ts = NowIsoMs();

    uint64_t l0 = 0, imm = 0, p99 = 0, win = 0, mprev_u64 = 0;
    double mprev_d = 0.0;
    // m_cmd is last valid command (default 1.0 before first command).
    double mcmd_d = g_last_cmd_multiplier.load(std::memory_order_relaxed);
    double mapplied_d =
        g_last_applied_multiplier.load(std::memory_order_relaxed);
    uint64_t tflush = 0, tcomp = 0;
    uint64_t is_write_stopped = 0;
    double delayed_write_rate_bps = 0.0;
    uint64_t compaction_pending = 0;
    uint64_t memtable_flush_pending = 0;
    uint64_t num_immutable_mem_table = 0;
    uint64_t estimate_pending_compaction_bytes = 0;
    uint64_t stall_stops = 0;
    uint64_t stall_delays = 0;
    uint64_t stall_total = 0;
    uint64_t stall_delta = 0;
    uint64_t stall_hist_count = 0;
    uint64_t stall_hist_delta = 0;
    bool ok_stall_map = false;
    bool ok_stall_total = false;
    bool ok_stall_hist = false;
    bool ok_ycsb_ops_total = false;
    bool ok_ycsb_ops_sec = false;
    bool ok_ycsb_avg_lat = false;
    bool ok_ycsb_p99 = false;
    bool ok_ycsb_p999 = false;
    bool ok_ycsb_p9999 = false;
    uint64_t ycsb_ops_total = 0;
    double ycsb_ops_sec = 0.0;
    double ycsb_avg_lat_us = 0.0;
    uint64_t ycsb_p99_us = 0;
    uint64_t ycsb_p999_us = 0;
    uint64_t ycsb_p9999_us = 0;

    bool ok_l0   = GetUInt64Prop(db, "rocksdb.rl.l0_file_count", &l0);
    bool ok_imm  = GetUInt64Prop(db, "rocksdb.rl.imm_memtable_bytes", &imm);
    bool ok_p99  = GetUInt64Prop(db, "rocksdb.rl.write_lat_p99_us", &p99);
    bool ok_win  = GetUInt64Prop(db, "rocksdb.rl.write_in_bytes_per_sec", &win);

    // multiplier might be integer-string or floating-string depending on implementation.
    bool ok_mprev = GetDoubleProp(db, "rocksdb.rl.write_multiplier_prev", &mprev_d);
    if (!ok_mprev) {
      // fallback attempt
      bool ok_mprev2 = GetUInt64Prop(db, "rocksdb.rl.write_multiplier_prev", &mprev_u64);
      if (ok_mprev2) {
        mprev_d = static_cast<double>(mprev_u64);
        ok_mprev = true;
      }
    }
    std::string applied_source;
    bool ok_mapplied = ReadAppliedMultiplier(db, &mapplied_d, &applied_source);
    if (ok_mapplied) {
      g_last_applied_multiplier.store(mapplied_d, std::memory_order_relaxed);
      g_warned_applied_read.store(false, std::memory_order_relaxed);
    } else if (!g_warned_applied_read.exchange(true)) {
      LogStderrLine(
          "[prop] rocksdb.rl.write_multiplier_current returned false; using "
          "last applied value");
    }

    bool ok_tflush =
        GetUInt64Prop(db, "rocksdb.rl.true_pending_flush_bytes", &tflush);
    bool ok_tcomp = GetUInt64Prop(db, "rocksdb.rl.true_pending_compaction_bytes",
                                  &tcomp);
    if (!ok_tflush && !g_warned_tflush_prop.exchange(true)) {
      LogStderrLine(
          "[prop] rocksdb.rl.true_pending_flush_bytes returned false (not "
          "supported / gated)");
    }
    if (!ok_tcomp && !g_warned_tcomp_prop.exchange(true)) {
      LogStderrLine(
          "[prop] rocksdb.rl.true_pending_compaction_bytes returned false "
          "(not supported / gated)");
    }

    bool ok_write_stopped =
        GetBoolProp(db, "rocksdb.is-write-stopped", &is_write_stopped);
    bool ok_delayed_rate =
        GetDoubleProp(db, "rocksdb.actual-delayed-write-rate", &delayed_write_rate_bps);
    bool ok_compaction_pending =
        GetBoolProp(db, "rocksdb.compaction-pending", &compaction_pending);
    bool ok_flush_pending =
        GetBoolProp(db, "rocksdb.mem-table-flush-pending", &memtable_flush_pending);
    bool ok_imm_count =
        GetUInt64Prop(db, "rocksdb.num-immutable-mem-table", &num_immutable_mem_table);
    bool ok_est_pending =
        GetUInt64Prop(db, "rocksdb.estimate-pending-compaction-bytes",
                      &estimate_pending_compaction_bytes);
    if (!ok_write_stopped) is_write_stopped = 0;
    if (!ok_delayed_rate) delayed_write_rate_bps = 0.0;
    if (!ok_compaction_pending) compaction_pending = 0;
    if (!ok_flush_pending) memtable_flush_pending = 0;
    if (!ok_imm_count) num_immutable_mem_table = 0;
    if (!ok_est_pending) estimate_pending_compaction_bytes = 0;

    bool stall_used_cf = false;
    ok_stall_map = GetWriteStallTotals(db, &stall_stops, &stall_delays, &stall_used_cf);
    if (ok_stall_map && stall_used_cf &&
        !g_warned_stall_cf_fallback.exchange(true)) {
      LogStderrLine("[prop] db-write-stall-stats unavailable; using cf-write-stall-stats");
    }
    if (ok_stall_map) {
      stall_total = stall_stops + stall_delays;
      ok_stall_total = true;
    }
    if (!ok_stall_total && !g_warned_stall_prop.exchange(true)) {
      LogStderrLine("[prop] write stall map stats unavailable; stop/delay columns will be empty");
    }
    if (ok_stall_total) {
      if (have_stall_totals) {
        stall_delta = (stall_total >= last_stall_total)
                          ? (stall_total - last_stall_total)
                          : 0;
      } else {
        stall_delta = 0;
        have_stall_totals = true;
      }
      last_stall_total = stall_total;
    }

    auto stats = db->GetDBOptions().statistics;
    if (stats) {
      rocksdb::HistogramData hist;
      stats->histogramData(rocksdb::WRITE_STALL, &hist);
      stall_hist_count = hist.count;
      ok_stall_hist = true;
      if (have_stall_hist) {
        stall_hist_delta = (stall_hist_count >= last_stall_hist)
                               ? (stall_hist_count - last_stall_hist)
                               : 0;
      } else {
        stall_hist_delta = 0;
        have_stall_hist = true;
      }
      last_stall_hist = stall_hist_count;
    }

    if (ycsb_workload != YcsbWorkload::kNone) {
      ycsb_ops_total = g_ycsb_logical_ops_total.load(std::memory_order_relaxed);
      const uint64_t ycsb_latency_us_total =
          g_ycsb_logical_latency_us_total.load(std::memory_order_relaxed);
      uint64_t ycsb_ops_delta = 0;
      uint64_t ycsb_latency_us_delta = 0;
      if (ycsb_ops_total >= last_ycsb_logical_ops_total) {
        ycsb_ops_delta = ycsb_ops_total - last_ycsb_logical_ops_total;
      }
      if (ycsb_latency_us_total >= last_ycsb_logical_latency_us_total) {
        ycsb_latency_us_delta =
            ycsb_latency_us_total - last_ycsb_logical_latency_us_total;
      }
      last_ycsb_logical_ops_total = ycsb_ops_total;
      last_ycsb_logical_latency_us_total = ycsb_latency_us_total;
      ok_ycsb_ops_total = true;

      if (tick_elapsed_sec > 0.0) {
        ycsb_ops_sec = static_cast<double>(ycsb_ops_delta) / tick_elapsed_sec;
        ok_ycsb_ops_sec = true;
      }
      if (ycsb_ops_delta > 0) {
        ycsb_avg_lat_us =
            static_cast<double>(ycsb_latency_us_delta) / ycsb_ops_delta;
        ok_ycsb_avg_lat = true;
      }

      uint64_t ycsb_hist_total_delta = 0;
      std::vector<std::pair<size_t, uint64_t>> ycsb_hist_non_zero;
      ycsb_hist_non_zero.reserve(256);
      for (size_t i = 0; i < kYcsbLatencyBucketCount; ++i) {
        const uint64_t cur =
            g_ycsb_latency_hist[i].load(std::memory_order_relaxed);
        const uint64_t prev = last_ycsb_latency_hist[i];
        uint64_t delta = 0;
        if (cur >= prev) {
          delta = cur - prev;
        }
        last_ycsb_latency_hist[i] = cur;
        if (delta > 0) {
          ycsb_hist_total_delta += delta;
          ycsb_hist_non_zero.emplace_back(i, delta);
        }
      }

      if (ycsb_hist_total_delta > 0) {
        ycsb_p99_us = QuantileFromYcsbLatencyDeltas(
            ycsb_hist_non_zero, ycsb_hist_total_delta, 0.99);
        ycsb_p999_us = QuantileFromYcsbLatencyDeltas(
            ycsb_hist_non_zero, ycsb_hist_total_delta, 0.999);
        ycsb_p9999_us = QuantileFromYcsbLatencyDeltas(
            ycsb_hist_non_zero, ycsb_hist_total_delta, 0.9999);
        ok_ycsb_p99 = true;
        ok_ycsb_p999 = true;
        ok_ycsb_p9999 = true;
      }
    }

    // Print to stdout (human-friendly)
    std::ostringstream line;
    line << ts
         << " l0=" << (ok_l0 ? std::to_string(l0) : "NA")
         << " imm=" << (ok_imm ? std::to_string(imm) : "NA")
         << " p99_us=" << (ok_p99 ? std::to_string(p99) : "NA")
         << " w_in_Bps=" << (ok_win ? std::to_string(win) : "NA")
         << " m_prev=" << (ok_mprev ? std::to_string(mprev_d) : "NA")
         << " tflush=" << (ok_tflush ? std::to_string(tflush) : "NA")
         << " tcomp=" << (ok_tcomp ? std::to_string(tcomp) : "NA")
         << " write_stopped=" << (ok_write_stopped ? std::to_string(is_write_stopped) : "NA")
         << " delayed_rate_bps=" << (ok_delayed_rate ? std::to_string(delayed_write_rate_bps) : "NA")
         << " compaction_pending=" << (ok_compaction_pending ? std::to_string(compaction_pending) : "NA")
         << " flush_pending=" << (ok_flush_pending ? std::to_string(memtable_flush_pending) : "NA")
         << " imm_count=" << (ok_imm_count ? std::to_string(num_immutable_mem_table) : "NA")
         << " est_comp_b=" << (ok_est_pending ? std::to_string(estimate_pending_compaction_bytes) : "NA")
         << " stall_cnt=" << (ok_stall_total ? std::to_string(stall_total) : "NA")
         << " stall_delta=" << (ok_stall_total ? std::to_string(stall_delta) : "NA")
         << " stall_hist_cnt=" << (ok_stall_hist ? std::to_string(stall_hist_count) : "NA")
         << " stall_hist_delta=" << (ok_stall_hist ? std::to_string(stall_hist_delta) : "NA")
         << " y_ops_total=" << (ok_ycsb_ops_total ? std::to_string(ycsb_ops_total) : "NA")
         << " y_ops_sec=" << (ok_ycsb_ops_sec ? FormatDoubleForCsv(ycsb_ops_sec) : "NA")
         << " y_avg_lat_us=" << (ok_ycsb_avg_lat ? FormatDoubleForCsv(ycsb_avg_lat_us) : "NA")
         << " y_p99_us=" << (ok_ycsb_p99 ? std::to_string(ycsb_p99_us) : "NA")
         << " y_p999_us=" << (ok_ycsb_p999 ? std::to_string(ycsb_p999_us) : "NA")
         << " y_p9999_us=" << (ok_ycsb_p9999 ? std::to_string(ycsb_p9999_us) : "NA");
    LogStdoutLine(line.str());

    // Write to CSV (use empty fields if missing)
    fout << ts << ","
         << (ok_l0 ? std::to_string(l0) : "") << ","
         << (ok_imm ? std::to_string(imm) : "") << ","
         << (ok_p99 ? std::to_string(p99) : "") << ","
         << (ok_win ? std::to_string(win) : "") << ","
         << (ok_mprev ? std::to_string(mprev_d) : "") << ","
         << FormatDoubleForCsv(mcmd_d) << ","
         << FormatDoubleForCsv(mapplied_d) << ","
         << (ok_tflush ? std::to_string(tflush) : "") << ","
         << (ok_tcomp ? std::to_string(tcomp) : "") << ","
         << std::to_string(is_write_stopped) << ","
         << FormatDoubleForCsv(delayed_write_rate_bps) << ","
         << std::to_string(compaction_pending) << ","
         << std::to_string(memtable_flush_pending) << ","
         << std::to_string(num_immutable_mem_table) << ","
         << std::to_string(estimate_pending_compaction_bytes) << ","
         << (ok_stall_map ? std::to_string(stall_stops) : "") << ","
         << (ok_stall_map ? std::to_string(stall_delays) : "") << ","
         << (ok_stall_total ? std::to_string(stall_total) : "") << ","
         << (ok_stall_total ? std::to_string(stall_delta) : "") << ","
         << (ok_stall_hist ? std::to_string(stall_hist_count) : "") << ","
         << (ok_stall_hist ? std::to_string(stall_hist_delta) : "") << ","
         << (ok_ycsb_ops_total ? std::to_string(ycsb_ops_total) : "") << ","
         << (ok_ycsb_ops_sec ? FormatDoubleForCsv(ycsb_ops_sec) : "") << ","
         << (ok_ycsb_avg_lat ? FormatDoubleForCsv(ycsb_avg_lat_us) : "") << ","
         << (ok_ycsb_p99 ? std::to_string(ycsb_p99_us) : "") << ","
         << (ok_ycsb_p999 ? std::to_string(ycsb_p999_us) : "") << ","
         << (ok_ycsb_p9999 ? std::to_string(ycsb_p9999_us) : "")
         << "\n";
    fout.flush();

    const auto now = std::chrono::steady_clock::now();
    if (now - last_builtin_report >= std::chrono::seconds(1)) {
      uint64_t mem_entries = 0;
      uint64_t mem_size = 0;
      uint64_t l0_files_builtin = 0;
      bool ok_entries = GetUInt64Prop(
          db, "rocksdb.num-entries-active-mem-table", &mem_entries);
      bool ok_mem_size =
          GetUInt64Prop(db, "rocksdb.cur-size-active-mem-table", &mem_size);
      bool ok_l0_builtin =
          GetUInt64Prop(db, "rocksdb.num-files-at-level0", &l0_files_builtin);
      std::ostringstream oss;
      oss << "[builtin] entries_active_memtable="
          << (ok_entries ? std::to_string(mem_entries) : "NA")
          << " cur_size_active_memtable="
          << (ok_mem_size ? std::to_string(mem_size) : "NA")
          << " l0_files="
          << (ok_l0_builtin ? std::to_string(l0_files_builtin) : "NA");
      LogStderrLine(oss.str());
      last_builtin_report = now;
    }

    std::this_thread::sleep_for(period);
  }

  if (cmd_thr.joinable()) cmd_thr.join();
  if (fifo_thr.joinable()) fifo_thr.join();
  if (writer_thr.joinable()) writer_thr.join();
  for (auto* handle : cf_handles) {
    delete handle;
  }
  delete db;
  return 0;
}
