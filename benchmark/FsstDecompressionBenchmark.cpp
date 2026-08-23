// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>

#include <vector>
#include <string>
#include <string_view>
#include <chrono>

#include "../benchmark/infrastructure/Benchmark.h"
#include "util/FsstCompressor.h"
#include "util/Random.h"

namespace ad_benchmark {

class FsstDecompressionBenchmark : public BenchmarkInterface {
 private:
  std::vector<std::string> shortWords_;
  std::vector<std::string> mediumWords_;
  std::vector<std::string> longWords_;

  // Pre-compressed word vectors and decoders for N = 1, 2, 3 stages
  std::vector<std::string_view> compressed1_;
  std::vector<std::string_view> compressed2_;
  std::vector<std::string_view> compressed3_;

  std::vector<std::shared_ptr<std::string>> buffers1_;
  std::vector<std::shared_ptr<std::string>> buffers2_;
  std::vector<std::shared_ptr<std::string>> buffers3_;

  std::optional<FsstRepeatedDecoder<1>> decoder1_;
  std::optional<FsstRepeatedDecoder<2>> decoder2_;
  std::optional<FsstRepeatedDecoder<3>> decoder3_;

 public:
  FsstDecompressionBenchmark() {
    // Generate synthetic SPARQL/RDF-like vocabulary words
    ad_utility::RandomCustomStringGenerator gen{"abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    for (size_t i = 0; i < 5000; ++i) {
      shortWords_.push_back("http://example.org/p/" + gen(15));
      mediumWords_.push_back("http://www.wikidata.org/entity/Q" + gen(45));
      longWords_.push_back("http://en.wikipedia.org/wiki/Special_Entity_Resource_Descriptor_" + gen(180));
    }

    auto setupDecoders = [](const std::vector<std::string>& words, auto& decoderOpt,
                            auto& compressedViews, auto& storageBuffers) {
      constexpr size_t N = std::decay_t<decltype(*decoderOpt)>::Decoders{}.size();
      std::vector<std::string_view> current;
      current.reserve(words.size());
      for (const auto& w : words) {
        current.emplace_back(w);
      }
      typename std::decay_t<decltype(*decoderOpt)>::Decoders decoders{};
      storageBuffers.reserve(N);
      for (size_t stage = 0; stage < N; ++stage) {
        auto [buf, nextViews, dec] = FsstEncoder::compressAll(current);
        current.assign(nextViews.begin(), nextViews.end());
        decoders[stage] = std::move(dec);
        storageBuffers.push_back(std::move(buf));
      }
      decoderOpt.emplace(decoders);
      compressedViews = current;
    };

    decoder1_.emplace();
    decoder2_.emplace();
    decoder3_.emplace();

    setupDecoders(mediumWords_, decoder1_, compressed1_, buffers1_);
    setupDecoders(mediumWords_, decoder2_, compressed2_, buffers2_);
    setupDecoders(mediumWords_, decoder3_, compressed3_, buffers3_);
  }

  BenchmarkResults run() const override {
    BenchmarkResults results;
    auto& group = results.addResultGroup("FSST Repeated Decompression Benchmarks (5,000 words)");

    // 1. Single-stage (N=1)
    group.addMeasurement("N=1: decompress() [single-stage allocation]", [&]() {
      size_t totalBytes = 0;
      for (const auto& c : compressed1_) {
        std::string s = decoder1_->decompress(c);
        totalBytes += s.size();
      }
      return totalBytes;
    });

    group.addMeasurement("N=1: decompressInto() [direct zero-scratch]", [&]() {
      size_t totalBytes = 0;
      std::string outBuf(FsstDecoder::maxExpansionFactor * 100, '\0');
      for (const auto& c : compressed1_) {
        size_t n = decoder1_->decompressInto(c, ql::span<char>{outBuf.data(), outBuf.size()});
        totalBytes += n;
      }
      return totalBytes;
    });

    // 2. Two-stage (N=2)
    group.addMeasurement("N=2: decompress() [upfront 2-buffer ping-pong]", [&]() {
      size_t totalBytes = 0;
      for (const auto& c : compressed2_) {
        std::string s = decoder2_->decompress(c);
        totalBytes += s.size();
      }
      return totalBytes;
    });

    group.addMeasurement("N=2: decompressInto() [reused persistent scratch]", [&]() {
      size_t totalBytes = 0;
      std::string scratch;
      std::string outBuf(FsstRepeatedDecoder<2>::maxDecompressedSize(std::string_view(mediumWords_.front())), '\0');
      for (const auto& c : compressed2_) {
        size_t n = decoder2_->decompressInto(c, ql::span<char>{outBuf.data(), outBuf.size()}, scratch);
        totalBytes += n;
      }
      return totalBytes;
    });

    // 3. Three-stage (N=3)
    group.addMeasurement("N=3: decompress() [upfront 2-buffer ping-pong]", [&]() {
      size_t totalBytes = 0;
      for (const auto& c : compressed3_) {
        std::string s = decoder3_->decompress(c);
        totalBytes += s.size();
      }
      return totalBytes;
    });

    group.addMeasurement("N=3: decompressInto() [reused persistent scratch]", [&]() {
      size_t totalBytes = 0;
      std::string scratch;
      std::string outBuf(FsstRepeatedDecoder<3>::maxDecompressedSize(std::string_view(mediumWords_.front())), '\0');
      for (const auto& c : compressed3_) {
        size_t n = decoder3_->decompressInto(c, ql::span<char>{outBuf.data(), outBuf.size()}, scratch);
        totalBytes += n;
      }
      return totalBytes;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(FsstDecompressionBenchmark);

}  // namespace ad_benchmark
