// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/BufferPoolBudget.h"

#include <algorithm>
#include <fstream>
#include <initializer_list>
#include <sstream>
#include <string>
#include <utility>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace {
// The first token of a file, or `std::nullopt` if it cannot be read.
std::optional<std::string> firstToken(const std::string& path) {
  std::ifstream in{path};
  std::string token;
  if (in >> token) {
    return token;
  }
  return std::nullopt;
}

// The value of `key` in the `memory.stat` file of the cgroup directory
// `dir`, trying each of `keys` in order (v1 and v2 name some counters
// differently, and v1 has hierarchical `total_*` variants).
std::optional<size_t> cgroupStat(const std::string& dir,
                                 std::initializer_list<std::string> keys) {
  std::ifstream in{dir + "/memory.stat"};
  std::string name;
  size_t value = 0;
  std::optional<size_t> totalVariant;
  while (in >> name >> value) {
    size_t rank = 0;
    for (const auto& key : keys) {
      if (name == key) {
        if (rank == 0) {
          return value;
        }
        totalVariant = value;
      }
      ++rank;
    }
  }
  return totalVariant;
}
}  // namespace

namespace detail {
// _____________________________________________________________________________
std::optional<size_t> meminfoBytesFrom(const std::string& path,
                                       const std::string& key) {
  std::ifstream in{path};
  std::string name;
  size_t value = 0;
  std::string unit;
  while (in >> name >> value) {
    std::getline(in, unit);
    if (name == key + ":") {
      return value * 1024;
    }
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<size_t> cgroupBytesFrom(const std::string& path) {
  auto token = firstToken(path);
  if (!token.has_value() || token.value() == "max") {
    return std::nullopt;
  }
  try {
    const auto value = std::stoull(token.value());
    // cgroup v1 writes `PAGE_COUNTER_MAX` (just below 2^63) for no limit.
    if (value >= (uint64_t{1} << 62)) {
      return std::nullopt;
    }
    return static_cast<size_t>(value);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

// _____________________________________________________________________________
std::optional<double> psiSomeAvg10From(const std::string& path) {
  std::ifstream in{path};
  std::string line;
  while (std::getline(in, line)) {
    if (line.rfind("some ", 0) != 0) {
      continue;
    }
    std::istringstream fields{line.substr(5)};
    std::string field;
    while (fields >> field) {
      if (field.rfind("avg10=", 0) == 0) {
        try {
          return std::stod(field.substr(6));
        } catch (const std::exception&) {
          return std::nullopt;
        }
      }
    }
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<CgroupLocation> ownCgroupLocation(
    const std::string& procSelfCgroupPath, const std::string& version2Root,
    const std::string& version1Root) {
  std::ifstream in{procSelfCgroupPath};
  std::string line;
  std::optional<CgroupLocation> version1;
  while (std::getline(in, line)) {
    // Each line is `<id>:<controllers>:<path>`; the path itself starts at
    // the second colon.
    const size_t firstColon = line.find(':');
    const size_t secondColon = firstColon == std::string::npos
                                   ? firstColon
                                   : line.find(':', firstColon + 1);
    if (secondColon == std::string::npos) {
      continue;
    }
    const std::string controllers =
        line.substr(firstColon + 1, secondColon - firstColon - 1);
    const std::string path = line.substr(secondColon + 1);
    if (line.rfind("0::", 0) == 0) {
      return CgroupLocation{version2Root + path, version2Root, true};
    }
    // A v1 hierarchy lists its controllers comma-separated, e.g.
    // `9:memory:/docker/...` or `5:cpu,memory:/...`.
    for (size_t begin = 0; begin <= controllers.size();) {
      const size_t comma = controllers.find(',', begin);
      const std::string controller = controllers.substr(
          begin, comma == std::string::npos ? comma : comma - begin);
      if (controller == "memory") {
        version1 = CgroupLocation{version1Root + path, version1Root, false};
      }
      if (comma == std::string::npos) {
        break;
      }
      begin = comma + 1;
    }
  }
  return version1;
}

// _____________________________________________________________________________
CgroupLimits cgroupLimitsFrom(const CgroupLocation& location) {
  CgroupLimits result;
  const std::string limitFile =
      location.version2_ ? "/memory.max" : "/memory.limit_in_bytes";
  const std::string usageFile =
      location.version2_ ? "/memory.current" : "/memory.usage_in_bytes";
  std::string current = location.dir_;
  while (current.size() >= location.root_.size()) {
    std::optional<size_t> limit = cgroupBytesFrom(current + limitFile);
    if (location.version2_) {
      // `memory.high` is the soft throttle point: the effective ceiling.
      auto high = cgroupBytesFrom(current + "/memory.high");
      if (high.has_value() &&
          (!limit.has_value() || high.value() < limit.value())) {
        limit = high;
      }
    }
    if (limit.has_value()) {
      // The usage includes the cgroup's page cache, which fills up to the
      // limit but is reclaimable. Count it as available like
      // `MemAvailable` does: the inactive file pages fully, the active ones
      // half.
      const size_t used = cgroupBytesFrom(current + usageFile).value_or(0);
      const size_t reclaimable =
          cgroupStat(current, {"inactive_file", "total_inactive_file"})
              .value_or(0) +
          cgroupStat(current, {"active_file", "total_active_file"})
                  .value_or(0) /
              2;
      const size_t usedNotReclaimable =
          used > reclaimable ? used - reclaimable : 0;
      const size_t headroom = limit.value() > usedNotReclaimable
                                  ? limit.value() - usedNotReclaimable
                                  : 0;
      result.headroom_ =
          std::min(result.headroom_.value_or(headroom), headroom);
      result.limit_ =
          std::min(result.limit_.value_or(limit.value()), limit.value());
    }
    if (current == location.root_) {
      break;
    }
    current = current.substr(0, current.rfind('/'));
  }
  return result;
}
}  // namespace detail

namespace {
// The cgroup limits of this process on this machine.
detail::CgroupLimits ownCgroupLimits() {
  auto location = detail::ownCgroupLocation(
      "/proc/self/cgroup", "/sys/fs/cgroup", "/sys/fs/cgroup/memory");
  if (!location.has_value()) {
    return {};
  }
  return detail::cgroupLimitsFrom(location.value());
}
}  // namespace

// _____________________________________________________________________________
BufferPoolSignals BufferPoolSignals::system() {
  BufferPoolSignals signals;
  signals.availableBytes = []() -> std::optional<size_t> {
    auto available = detail::meminfoBytesFrom("/proc/meminfo", "MemAvailable");
    auto headroom = ownCgroupLimits().headroom_;
    if (available.has_value() && headroom.has_value()) {
      return std::min(available.value(), headroom.value());
    }
    return available.has_value() ? available : headroom;
  };
  signals.pressurePercent = []() -> std::optional<double> {
    if (auto location = detail::ownCgroupLocation(
            "/proc/self/cgroup", "/sys/fs/cgroup", "/sys/fs/cgroup/memory");
        location.has_value() && location->version2_) {
      if (auto psi =
              detail::psiSomeAvg10From(location->dir_ + "/memory.pressure");
          psi.has_value()) {
        return psi;
      }
    }
    return detail::psiSomeAvg10From("/proc/pressure/memory");
  };
  signals.totalBytes = []() -> std::optional<size_t> {
    auto total = detail::meminfoBytesFrom("/proc/meminfo", "MemTotal");
    auto limit = ownCgroupLimits().limit_;
    if (total.has_value() && limit.has_value()) {
      return std::min(total.value(), limit.value());
    }
    return total.has_value() ? total : limit;
  };
  return signals;
}

// _____________________________________________________________________________
size_t bufferPoolBudgetShare(size_t totalBytes, size_t partBytes,
                             size_t wholeBytes) {
  if (wholeBytes == 0) {
    return 0;
  }
  return static_cast<size_t>(static_cast<double>(totalBytes) *
                             static_cast<double>(partBytes) /
                             static_cast<double>(wholeBytes));
}

// _____________________________________________________________________________
size_t nextBufferPoolTarget(size_t target, size_t loaded, size_t ceiling,
                            size_t floor, size_t reserve, size_t step,
                            std::optional<size_t> available,
                            std::optional<double> pressurePercent,
                            double pressureThreshold) {
  const bool pressure = pressurePercent.has_value() &&
                        pressurePercent.value() > pressureThreshold;
  const bool lowMemory = available.has_value() && available.value() < reserve;
  if (pressure || lowMemory) {
    // Lowering a target above what is loaded would free nothing.
    const size_t base = std::min(target, loaded);
    const size_t deficit = lowMemory ? reserve - available.value() : 0;
    const size_t cut = std::max(deficit, base / 10);
    target = base > cut ? base - cut : 0;
  } else if (available.has_value() && available.value() > reserve &&
             available.value() - reserve > step) {
    target = target + step;
  }
  const size_t lower = std::min(floor, ceiling);
  return std::clamp(target, lower, ceiling);
}

// _____________________________________________________________________________
BufferPoolBudgetController::BufferPoolBudgetController(
    BufferPoolSignals signals, BufferPoolBudgetConfig config)
    : signals_{std::move(signals)},
      config_{config},
      target_{config.floorBytes} {}

// _____________________________________________________________________________
BufferPoolBudgetController::~BufferPoolBudgetController() {
  std::thread thread;
  {
    std::lock_guard lock{mutex_};
    groups_.clear();
    ++generation_;
    thread = std::move(thread_);
  }
  wakeUp_.notify_all();
  if (thread.joinable()) {
    thread.join();
  }
}

// _____________________________________________________________________________
BufferPoolBudgetController& BufferPoolBudgetController::global() {
  // Intentionally leaked: a vocabulary may unregister its pools during static
  // destruction, after a static controller would already be gone.
  static auto* controller = new BufferPoolBudgetController{
      BufferPoolSignals::system(), BufferPoolBudgetConfig{}};
  return *controller;
}

// _____________________________________________________________________________
std::unique_ptr<BufferPoolBudgetController::Registration>
BufferPoolBudgetController::add(std::vector<BufferPool*> pools) {
  std::lock_guard lock{mutex_};
  const size_t id = nextId_++;
  groups_.push_back(Group{id, std::move(pools)});
  if (!thread_.joinable()) {
    thread_ =
        std::thread{[this, generation = generation_]() { run(generation); }};
  }
  return std::unique_ptr<Registration>{new Registration{*this, id}};
}

// _____________________________________________________________________________
BufferPoolBudgetController::Registration::~Registration() {
  controller_.remove(id_);
}

// _____________________________________________________________________________
void BufferPoolBudgetController::remove(size_t id) {
  std::thread thread;
  {
    std::lock_guard lock{mutex_};
    groups_.erase(std::remove_if(groups_.begin(), groups_.end(),
                                 [id](const Group& g) { return g.id_ == id; }),
                  groups_.end());
    if (groups_.empty()) {
      // No pools left: stop the thread (a later `add` starts a new one).
      ++generation_;
      thread = std::move(thread_);
    }
  }
  wakeUp_.notify_all();
  if (thread.joinable()) {
    thread.join();
  }
}

// _____________________________________________________________________________
void BufferPoolBudgetController::run(size_t generation) {
  std::unique_lock lock{mutex_};
  while (generation_ == generation) {
    wakeUp_.wait_for(lock, config_.period,
                     [&]() { return generation_ != generation; });
    if (generation_ != generation) {
      break;
    }
    tickLocked();
  }
}

// _____________________________________________________________________________
void BufferPoolBudgetController::tick() {
  std::lock_guard lock{mutex_};
  tickLocked();
}

// _____________________________________________________________________________
size_t BufferPoolBudgetController::reserveBytes() const {
  if (size_t reserve = reserveOverride_.load(); reserve > 0) {
    return reserve;
  }
  if (config_.reserveBytes > 0) {
    return config_.reserveBytes;
  }
  const size_t total =
      signals_.totalBytes ? signals_.totalBytes().value_or(0) : 0;
  // Relative to the memory this process may use, so that a tight memory
  // limit is not eaten up by a fixed minimum (#5537: a 2 GiB minimum left the
  // pool at about 1.6 GiB under a limit with about 4 GiB of headroom).
  return std::max(256 * BufferPoolBudgetConfig::MIB, total / 20);
}

// _____________________________________________________________________________
void BufferPoolBudgetController::tickLocked() {
  size_t fileBytes = 0;
  size_t loaded = 0;
  for (const auto& group : groups_) {
    for (const BufferPool* pool : group.pools_) {
      fileBytes += pool->numPages() * pool->pageSize();
      loaded += pool->loadedBytes();
    }
  }
  if (fileBytes == 0) {
    return;
  }
  const auto available =
      signals_.availableBytes ? signals_.availableBytes() : std::nullopt;
  const auto pressure =
      signals_.pressurePercent ? signals_.pressurePercent() : std::nullopt;
  // The pools can never use more than their files, nor more than what is
  // loaded plus what is still free.
  size_t ceiling = fileBytes;
  if (size_t explicitCeiling = explicitCeiling_.load(); explicitCeiling > 0) {
    ceiling = std::min(ceiling, explicitCeiling);
  }
  if (available.has_value()) {
    ceiling = std::min(ceiling, loaded + available.value());
  }
  const size_t target = nextBufferPoolTarget(
      target_.load(), loaded, ceiling, config_.floorBytes, reserveBytes(),
      config_.stepBytes, available, pressure, config_.pressureThreshold);
  target_.store(target);

  // Split the target in proportion to the file sizes (as for a fixed budget),
  // then evict towards it in a bounded chunk.
  size_t evictPages = config_.maxEvictBytesPerTick / BufferPool::hostPageSize();
  for (const auto& group : groups_) {
    for (BufferPool* pool : group.pools_) {
      const size_t poolBytes = pool->numPages() * pool->pageSize();
      pool->setBudget(bufferPoolBudgetShare(target, poolBytes, fileBytes));
    }
  }
  for (const auto& group : groups_) {
    for (BufferPool* pool : group.pools_) {
      if (evictPages == 0) {
        break;
      }
      evictPages -= std::min(evictPages, pool->evictTowardsBudget(evictPages));
    }
  }

  // Log changes, at most once per second.
  const auto now = std::chrono::steady_clock::now();
  if (target != lastLoggedTarget_ &&
      now - lastLog_ >= std::chrono::seconds{1}) {
    AD_LOG_INFO << "Vocabulary buffer pool target: " << target / (1 << 20)
                << " MiB (loaded " << loaded / (1 << 20) << " MiB, available "
                << (available.has_value()
                        ? std::to_string(available.value() / (1 << 20)) + " MiB"
                        : std::string{"unknown"})
                << ", memory pressure "
                << (pressure.has_value()
                        ? std::to_string(pressure.value()) + " %"
                        : std::string{"unknown"})
                << ")" << std::endl;
    lastLoggedTarget_ = target;
    lastLog_ = now;
  }
}

}  // namespace ad_utility
