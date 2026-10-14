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
#include <sstream>
#include <string>
#include <utility>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace {
// The value of `key` (in kB) in `/proc/meminfo`, in bytes.
std::optional<size_t> meminfoBytes(const std::string& key) {
  std::ifstream in{"/proc/meminfo"};
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

// The first token of a file, or `std::nullopt` if it cannot be read.
std::optional<std::string> firstToken(const std::string& path) {
  std::ifstream in{path};
  std::string token;
  if (in >> token) {
    return token;
  }
  return std::nullopt;
}

// A byte count from a cgroup file (`memory.max`, `memory.high`,
// `memory.current`); `std::nullopt` for "max" (no limit) or if not readable.
std::optional<size_t> cgroupBytes(const std::string& path) {
  auto token = firstToken(path);
  if (!token.has_value() || token.value() == "max") {
    return std::nullopt;
  }
  try {
    return static_cast<size_t>(std::stoull(token.value()));
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

// The value of `key` in the `memory.stat` file of the cgroup directory `dir`.
std::optional<size_t> cgroupStat(const std::string& dir,
                                 const std::string& key) {
  std::ifstream in{dir + "/memory.stat"};
  std::string name;
  size_t value = 0;
  while (in >> name >> value) {
    if (name == key) {
      return value;
    }
  }
  return std::nullopt;
}

// The directory of this process's cgroup (v2), or `std::nullopt` (no unified
// hierarchy, e.g. not Linux or cgroup v1).
std::optional<std::string> ownCgroupDir() {
  std::ifstream in{"/proc/self/cgroup"};
  std::string line;
  while (std::getline(in, line)) {
    if (line.rfind("0::", 0) == 0) {
      return "/sys/fs/cgroup" + line.substr(3);
    }
  }
  return std::nullopt;
}

// The smallest headroom (limit minus usage) and the smallest limit over this
// process's cgroup and its ancestors. A limit can be set at any level (e.g. on
// a systemd scope or on a container's slice).
struct CgroupLimits {
  std::optional<size_t> headroom_;
  std::optional<size_t> limit_;
};
CgroupLimits cgroupLimits() {
  CgroupLimits result;
  auto dir = ownCgroupDir();
  if (!dir.has_value()) {
    return result;
  }
  std::string current = dir.value();
  while (current.size() >= std::string{"/sys/fs/cgroup"}.size()) {
    std::optional<size_t> limit = cgroupBytes(current + "/memory.max");
    auto high = cgroupBytes(current + "/memory.high");
    if (high.has_value() &&
        (!limit.has_value() || high.value() < limit.value())) {
      limit = high;
    }
    if (limit.has_value()) {
      // `memory.current` includes the cgroup's page cache, which fills up to
      // the limit but is reclaimable. Count it as available like
      // `MemAvailable` does: the inactive file pages fully, the active ones
      // half.
      const size_t used = cgroupBytes(current + "/memory.current").value_or(0);
      const size_t reclaimable =
          cgroupStat(current, "inactive_file").value_or(0) +
          cgroupStat(current, "active_file").value_or(0) / 2;
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
    if (current == "/sys/fs/cgroup") {
      break;
    }
    current = current.substr(0, current.rfind('/'));
  }
  return result;
}

// PSI `some avg10` (percent) from a pressure file.
std::optional<double> psiSomeAvg10(const std::string& path) {
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
}  // namespace

// _____________________________________________________________________________
BufferPoolSignals BufferPoolSignals::system() {
  BufferPoolSignals signals;
  signals.availableBytes = []() -> std::optional<size_t> {
    auto available = meminfoBytes("MemAvailable");
    auto headroom = cgroupLimits().headroom_;
    if (available.has_value() && headroom.has_value()) {
      return std::min(available.value(), headroom.value());
    }
    return available.has_value() ? available : headroom;
  };
  signals.pressurePercent = []() -> std::optional<double> {
    if (auto dir = ownCgroupDir(); dir.has_value()) {
      if (auto psi = psiSomeAvg10(dir.value() + "/memory.pressure");
          psi.has_value()) {
        return psi;
      }
    }
    return psiSomeAvg10("/proc/pressure/memory");
  };
  signals.totalBytes = []() -> std::optional<size_t> {
    auto total = meminfoBytes("MemTotal");
    auto limit = cgroupLimits().limit_;
    if (total.has_value() && limit.has_value()) {
      return std::min(total.value(), limit.value());
    }
    return total.has_value() ? total : limit;
  };
  return signals;
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
  return std::max(2 * BufferPoolBudgetConfig::GiB, total / 20);
}

// _____________________________________________________________________________
void BufferPoolBudgetController::tickLocked() {
  size_t fileBytes = 0;
  size_t loaded = 0;
  for (const auto& group : groups_) {
    for (const BufferPool* pool : group.pools_) {
      fileBytes += pool->numPages() * BufferPool::pageSize;
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
  size_t evictPages = config_.maxEvictBytesPerTick / BufferPool::pageSize;
  for (const auto& group : groups_) {
    for (BufferPool* pool : group.pools_) {
      const size_t poolBytes = pool->numPages() * BufferPool::pageSize;
      pool->setBudget(static_cast<size_t>(static_cast<double>(target) *
                                          static_cast<double>(poolBytes) /
                                          static_cast<double>(fileBytes)));
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
