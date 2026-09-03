// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#ifdef __linux__
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/MonomorphicSerializers.h"
#include "engine/export_prototypes/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Invariants.h"
#include "util/http/MediaTypes.h"

// _____________________________________________________________________________
// Tracks scalar operator-new allocations made during benchmark loops.
struct AllocationTracker {
  static inline std::atomic<bool> enabled_{false};
  static inline std::atomic<size_t> count_{0};
  static inline std::atomic<size_t> bytes_{0};

  static void start() noexcept {
    count_.store(0, std::memory_order_seq_cst);
    bytes_.store(0, std::memory_order_seq_cst);
    enabled_.store(true, std::memory_order_seq_cst);
  }

  static void stop() noexcept {
    enabled_.store(false, std::memory_order_seq_cst);
  }

  static size_t getCount() noexcept {
    return count_.load(std::memory_order_seq_cst);
  }

  static size_t getBytes() noexcept {
    return bytes_.load(std::memory_order_seq_cst);
  }
};

// Global new/delete instrumentation
void* operator new(std::size_t size) {
  if (AllocationTracker::enabled_.load(std::memory_order_relaxed)) {
    AllocationTracker::count_.fetch_add(1, std::memory_order_relaxed);
    AllocationTracker::bytes_.fetch_add(size, std::memory_order_relaxed);
  }
  void* ptr = std::malloc(size);
  if (!ptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

void operator delete(void* ptr) noexcept {
  std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept {
  std::free(ptr);
}

namespace ad_benchmark {
namespace {

using namespace ql::serialization;
using namespace ql::export_formatting;

// _____________________________________________________________________________
// Hardware Performance Counter Monitor (Linux perf_event_open)
// Measures Hardware CPU Cycles, Instructions, Branch Instructions, and Branch Misses.
class PerfCounterMonitor {
 public:
  struct Metrics {
    uint64_t cycles = 0;
    uint64_t instructions = 0;
    uint64_t branches = 0;
    uint64_t branchMisses = 0;
    double ipc = 0.0;
    double branchMissRate = 0.0;
    bool available = false;
  };

 private:
#ifdef __linux__
  int fdCycles_ = -1;
  int fdInstructions_ = -1;
  int fdBranches_ = -1;
  int fdBranchMisses_ = -1;
  bool isAvailable_ = false;

  static int openPerfEvent(uint32_t type, uint64_t config) noexcept {
    struct perf_event_attr pe;
    std::memset(&pe, 0, sizeof(struct perf_event_attr));
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = config;
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;
    return static_cast<int>(syscall(__NR_perf_event_open, &pe, 0, -1, -1, 0));
  }
#endif

 public:
  PerfCounterMonitor() noexcept {
#ifdef __linux__
    fdCycles_ = openPerfEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
    fdInstructions_ = openPerfEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
    fdBranches_ = openPerfEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS);
    fdBranchMisses_ = openPerfEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
    isAvailable_ = (fdCycles_ >= 0 && fdInstructions_ >= 0 && fdBranches_ >= 0);
#endif
  }

  ~PerfCounterMonitor() noexcept {
#ifdef __linux__
    if (fdCycles_ >= 0) close(fdCycles_);
    if (fdInstructions_ >= 0) close(fdInstructions_);
    if (fdBranches_ >= 0) close(fdBranches_);
    if (fdBranchMisses_ >= 0) close(fdBranchMisses_);
#endif
  }

  void start() noexcept {
#ifdef __linux__
    if (!isAvailable_) return;
    ioctl(fdCycles_, PERF_EVENT_IOC_RESET, 0);
    ioctl(fdInstructions_, PERF_EVENT_IOC_RESET, 0);
    ioctl(fdBranches_, PERF_EVENT_IOC_RESET, 0);
    if (fdBranchMisses_ >= 0) ioctl(fdBranchMisses_, PERF_EVENT_IOC_RESET, 0);

    ioctl(fdCycles_, PERF_EVENT_IOC_ENABLE, 0);
    ioctl(fdInstructions_, PERF_EVENT_IOC_ENABLE, 0);
    ioctl(fdBranches_, PERF_EVENT_IOC_ENABLE, 0);
    if (fdBranchMisses_ >= 0) ioctl(fdBranchMisses_, PERF_EVENT_IOC_ENABLE, 0);
#endif
  }

  [[nodiscard]] Metrics stop() noexcept {
    Metrics m;
#ifdef __linux__
    if (!isAvailable_) return m;
    ioctl(fdCycles_, PERF_EVENT_IOC_DISABLE, 0);
    ioctl(fdInstructions_, PERF_EVENT_IOC_DISABLE, 0);
    ioctl(fdBranches_, PERF_EVENT_IOC_DISABLE, 0);
    if (fdBranchMisses_ >= 0) ioctl(fdBranchMisses_, PERF_EVENT_IOC_DISABLE, 0);

    if (read(fdCycles_, &m.cycles, sizeof(m.cycles)) > 0 &&
        read(fdInstructions_, &m.instructions, sizeof(m.instructions)) > 0 &&
        read(fdBranches_, &m.branches, sizeof(m.branches)) > 0) {
      m.available = true;
      if (fdBranchMisses_ >= 0) {
        [[maybe_unused]] auto res =
            read(fdBranchMisses_, &m.branchMisses, sizeof(m.branchMisses));
      }
      if (m.cycles > 0) {
        m.ipc = static_cast<double>(m.instructions) / static_cast<double>(m.cycles);
      }
      if (m.branches > 0) {
        m.branchMissRate =
            static_cast<double>(m.branchMisses) / static_cast<double>(m.branches) * 100.0;
      }
    }
#endif
    return m;
  }
};

// _____________________________________________________________________________
// Generate synthetic datasets for 1,000,000 rows across diverse query schemas.
constexpr size_t NUM_BENCHMARK_ROWS = 1'000'000;

struct DatasetStorage {
  // Keep the string pool so that the views remain valid across runs.
  std::vector<std::string> stringPool_;

    // Reserved capacity for rows of each schema
  std::vector<std::array<CellValue, 3>> tripleRows_;
  std::vector<std::array<CellValue, 3>> metricRows_;
  std::vector<std::array<CellValue, 4>> relationalRows_;
  std::vector<std::array<CellValue, 3>> graphEdgeRows_;

  // Store typed tuples for direct monomorphic testing.
  std::vector<std::tuple<std::string_view, std::string_view, std::string_view>>
      tripleTuples_;
  std::vector<std::tuple<std::string_view, std::string_view, int64_t>>
      metricTuples_;
  std::vector<std::tuple<std::string_view, std::string_view, int64_t, double>>
      relationalTuples_;
};

DatasetStorage generateBenchmarkDataset(size_t numRows) {
  DatasetStorage data;
  data.stringPool_.reserve(numRows * 4);
  data.tripleRows_.reserve(numRows);
  data.metricRows_.reserve(numRows);
  data.relationalRows_.reserve(numRows);
  data.graphEdgeRows_.reserve(numRows);

  data.tripleTuples_.reserve(numRows);
  data.metricTuples_.reserve(numRows);
  data.relationalTuples_.reserve(numRows);

  // Add common IRIs.
  data.stringPool_.push_back("<http://www.w3.org/2000/01/rdf-schema#label>");
  data.stringPool_.push_back("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
  data.stringPool_.push_back("<http://example.org/prop/population>");
  data.stringPool_.push_back("<http://example.org/prop/areaSqKm>");

  std::string_view predLabel = data.stringPool_[0];
  std::string_view predType = data.stringPool_[1];
  std::string_view predPop = data.stringPool_[2];
  std::string_view predArea = data.stringPool_[3];

  for (size_t i = 0; i < numRows; ++i) {
    
    data.stringPool_.push_back("<http://example.org/entity/Q" + std::to_string(i) + ">");
    std::string_view subj = data.stringPool_.back();

    
    data.stringPool_.push_back("\"Metropolitan City Name " + std::to_string(i) + "\"@en");
    std::string_view literal = data.stringPool_.back();

    
    data.stringPool_.push_back("<http://example.org/class/City>");
    std::string_view classCity = data.stringPool_.back();

    int64_t popVal = static_cast<int64_t>(100'000 + (i % 5'000'000));
    double areaVal = 12.5 + static_cast<double>(i % 500) * 0.75;

    // 1. Triple Schema: <IRI, IRI, LITERAL>
    data.tripleRows_.push_back({CellValue::makeIri(subj), CellValue::makeIri(predLabel),
                                CellValue::makeLiteral(literal)});
    data.tripleTuples_.push_back({subj, predLabel, literal});

    // 2. Metric Schema: <IRI, IRI, INT>
    data.metricRows_.push_back({CellValue::makeIri(subj), CellValue::makeIri(predPop),
                                CellValue::makeInt(popVal)});
    data.metricTuples_.push_back({subj, predPop, popVal});

    // 3. Relational 4-Column Schema: <IRI, LITERAL, INT, DOUBLE>
    data.relationalRows_.push_back(
        {CellValue::makeIri(subj), CellValue::makeLiteral(literal),
         CellValue::makeInt(popVal), CellValue::makeDouble(areaVal)});
    data.relationalTuples_.push_back({subj, literal, popVal, areaVal});

    // 4. Graph Edge Schema: <IRI, IRI, IRI>
    data.graphEdgeRows_.push_back({CellValue::makeIri(subj), CellValue::makeIri(predType),
                                   CellValue::makeIri(classCity)});
  }

  return data;
}

// _____________________________________________________________________________
// Benchmark dynamic per-cell dispatch against monomorphic template serializers.
class MonomorphicSerializerBenchmark : public BenchmarkInterface {
 private:
  DatasetStorage data_;
  PerfCounterMonitor perfMonitor_;

 public:
  MonomorphicSerializerBenchmark() {
    std::cout << "Initializing MonomorphicSerializerBenchmark (1,000,000 Rows)..."
              << std::endl;
    data_ = generateBenchmarkDataset(NUM_BENCHMARK_ROWS);
    std::cout << "Synthetic dataset generation complete." << std::endl;
  }

  std::string name() const final {
    return "Monomorphic Template Serialization vs Dynamic Dispatch (1,000,000 Rows)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

        // Byte-counting sink that discards serialized output.
    size_t bytesSink = 0;
    auto nullSink = [&](std::string_view chunk) { bytesSink += chunk.size(); };

    // =========================================================================
    // SECTION 1: Standard RDF Triples Schema <IRI, IRI, LITERAL>
    // =========================================================================
    {
      auto& group = results.addGroup("Schema: <IRI, IRI, LITERAL> (1M Triples)");
      const std::vector<ColumnType> schema = {ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Literal};

      // 1. Dynamic Per-Cell Dispatch Serializer (Baseline)
      {
        DynamicRowSerializer dynamicSerializer(schema);
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "1. Dynamic Per-Cell Dispatch (CSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& row : data_.tripleRows_) {
                dynamicSerializer.serializeRow<ExportFormat::Csv>(
                    formatter, ql::span<const CellValue>(row));
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }

      // 2. Monomorphic Template Serializer (Compile-time unrolled)
      {
        using MonomorphicTriples =
            MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                     ColumnType::Literal>;
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "2. Monomorphic Template Serializer (CSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& row : data_.tripleRows_) {
                MonomorphicTriples::serializeRow<ExportFormat::Csv>(
                    formatter, ql::span<const CellValue>(row));
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }

      // 3. Monomorphic Direct Tuple Serializer (Direct typed calls)
      {
        using MonomorphicTriples =
            MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                     ColumnType::Literal>;
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "3. Monomorphic Direct Tuple Serializer (CSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& [s, p, o] : data_.tripleTuples_) {
                MonomorphicTriples::serializeRow<ExportFormat::Csv>(formatter, s, p, o);
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }

      // 4. Fast-Path Template Dispatch (Runtime Schema -> Monomorphic Spec)
      {
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "4. Fast-Path Template Dispatch (CSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              dispatchMonomorphicSerializer(
                  schema,
                  [&]<ColumnType... Types>() {
                    using Serializer = MonomorphicRowSerializer<Types...>;
                    for (const auto& row : data_.tripleRows_) {
                      Serializer::template serializeRow<ExportFormat::Csv>(
                          formatter, ql::span<const CellValue>(row));
                    }
                  });
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }
    }

    // =========================================================================
    // SECTION 2: Metric Schema <IRI, IRI, INT>
    // =========================================================================
    {
      auto& group = results.addGroup("Schema: <IRI, IRI, INT> (1M Rows)");
      const std::vector<ColumnType> schema = {ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Int};

      // 1. Dynamic Per-Cell Dispatch
      {
        DynamicRowSerializer dynamicSerializer(schema);
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "1. Dynamic Per-Cell Dispatch (TSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& row : data_.metricRows_) {
                dynamicSerializer.serializeRow<ExportFormat::Tsv>(
                    formatter, ql::span<const CellValue>(row));
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }

      // 2. Monomorphic Template Serializer
      {
        using MonomorphicMetrics =
            MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri, ColumnType::Int>;
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "2. Monomorphic Template Serializer (TSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& row : data_.metricRows_) {
                MonomorphicMetrics::serializeRow<ExportFormat::Tsv>(
                    formatter, ql::span<const CellValue>(row));
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }

      // 3. Monomorphic Direct Tuple Serializer
      {
        using MonomorphicMetrics =
            MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri, ColumnType::Int>;
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "3. Monomorphic Direct Tuple Serializer (TSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& [s, p, i] : data_.metricTuples_) {
                MonomorphicMetrics::serializeRow<ExportFormat::Tsv>(formatter, s, p, i);
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }
    }

    // =========================================================================
    // SECTION 3: Relational 4-Column Schema <IRI, LITERAL, INT, DOUBLE>
    // =========================================================================
    {
      auto& group = results.addGroup(
          "Schema: <IRI, LITERAL, INT, DOUBLE> 4-Column Relational (1M Rows)");
      const std::vector<ColumnType> schema = {ColumnType::Iri, ColumnType::Literal,
                                              ColumnType::Int, ColumnType::Double};

      // 1. Dynamic Per-Cell Dispatch
      {
        DynamicRowSerializer dynamicSerializer(schema);
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "1. Dynamic Per-Cell Dispatch (CSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& row : data_.relationalRows_) {
                dynamicSerializer.serializeRow<ExportFormat::Csv>(
                    formatter, ql::span<const CellValue>(row));
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }

      // 2. Monomorphic Template Serializer
      {
        using MonomorphicRelational =
            MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Literal,
                                     ColumnType::Int, ColumnType::Double>;
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "2. Monomorphic Template Serializer (CSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& row : data_.relationalRows_) {
                MonomorphicRelational::serializeRow<ExportFormat::Csv>(
                    formatter, ql::span<const CellValue>(row));
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }

      // 3. Monomorphic Direct Tuple Serializer
      {
        using MonomorphicRelational =
            MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Literal,
                                     ColumnType::Int, ColumnType::Double>;
        PerfCounterMonitor::Metrics perf;
        size_t totalBytes = 0;

        auto& m = group.addMeasurement(
            "3. Monomorphic Direct Tuple Serializer (CSV)", [&]() {
              AllocationTracker::start();
              perfMonitor_.start();

              FastExportStreamFormatter formatter(nullSink);
              for (const auto& [c0, c1, c2, c3] : data_.relationalTuples_) {
                MonomorphicRelational::serializeRow<ExportFormat::Csv>(formatter, c0, c1,
                                                                      c2, c3);
              }
              auto summary = std::move(formatter).finalize();

              perf = perfMonitor_.stop();
              AllocationTracker::stop();
              totalBytes = summary.totalBytesWritten_;
              return totalBytes;
            });

        recordMetrics(m, NUM_BENCHMARK_ROWS, totalBytes, perf,
                      AllocationTracker::getCount());
      }
    }

    return results;
  }

 private:
  static void recordMetrics(ResultEntry& m, size_t rowCount, size_t totalBytes,
                            const PerfCounterMonitor::Metrics& perf,
                            size_t heapAllocations) {
    m.metadata().addKeyValuePair("total-rows", rowCount);
    m.metadata().addKeyValuePair("total-bytes-mb",
                                 static_cast<double>(totalBytes) / (1024.0 * 1024.0));
    m.metadata().addKeyValuePair("heap-allocations", heapAllocations);

    if (perf.available) {
      m.metadata().addKeyValuePair("hw-instructions", perf.instructions);
      m.metadata().addKeyValuePair("hw-cycles", perf.cycles);
      m.metadata().addKeyValuePair("instructions-per-cycle (IPC)", perf.ipc);
      m.metadata().addKeyValuePair("hw-branches", perf.branches);
      m.metadata().addKeyValuePair("hw-branch-misses", perf.branchMisses);
      m.metadata().addKeyValuePair("branch-miss-rate-pct", perf.branchMissRate);
    }
  }
};

AD_REGISTER_BENCHMARK(MonomorphicSerializerBenchmark);

}  // namespace
}  // namespace ad_benchmark
