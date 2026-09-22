//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PREFIXCOMPRESSOR_H
#define QLEVER_PREFIXCOMPRESSOR_H

#include <algorithm>
#include <array>
#include <string>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeVector.h"
#include "util/StringUtils.h"

// TODO<joka921> Include the relevant constants directly here.

/// Compression and decompression of words given a codebook of common prefixes.
/// The maximum number of prefixes is `NUM_COMPRESSION_PREFIXES` (currently
/// 126).
class PrefixCompressor {
 private:
  // Simple class for a prefix and its code as members of the codebook.
  struct PrefixCode {
    PrefixCode() = default;
    PrefixCode(char code, std::string prefix)
        : code_(1, code), prefix_(std::move(prefix)) {}

    std::string code_;
    std::string prefix_;
    AD_SERIALIZE_FRIEND_FUNCTION(PrefixCode) {
      serializer | arg.code_;
      serializer | arg.prefix_;
    }
  };

  // List of all prefixes, sorted descending by the length
  // of the prefixes. Used for lookup when compressing.
  std::vector<PrefixCode> codeToPrefix_{};

  // maps (numeric) keys to the prefix they encode.
  // currently only 128 prefixes are supported.
  std::array<std::string, NUM_COMPRESSION_PREFIXES> prefixToCode_{""};

  AD_SERIALIZE_FRIEND_FUNCTION(PrefixCompressor) {
    serializer | arg.codeToPrefix_;
    serializer | arg.prefixToCode_;
  }

  // The codebook prefix that the leading `code` byte of a compressed word
  // expands to, or empty when `code` is not a known prefix code (the byte is
  // then dropped, mirroring `decompress`).
  std::string_view prefixForCode(char code) const {
    auto idx = static_cast<uint8_t>(code) - MIN_COMPRESSION_PREFIX;
    if (idx >= 0 && idx < NUM_COMPRESSION_PREFIXES) {
      return prefixToCode_[idx];
    }
    return {};
  }

 public:
  // Compress the given `word`. Note: This iterates over all prefixes in the
  // codebook, and it is currently not a bottleneck in the IndexBuilder.
  [[nodiscard]] std::string compress(std::string_view word) const {
    for (const auto& p : codeToPrefix_) {
      if (ql::starts_with(word, p.prefix_)) {
        return p.code_ + std::string_view(word).substr(p.prefix_.size());
      }
    }
    return static_cast<char>(NO_PREFIX_CHAR) + word;
  }

  // Decompress the given `compressedWord`.
  [[nodiscard]] std::string decompress(std::string_view compressedWord) const {
    AD_CONTRACT_CHECK(!compressedWord.empty());
    return std::string(prefixForCode(compressedWord[0])) +
           compressedWord.substr(1);
  }

  // Return the exact decompressed size of `compressedWord`: the leading code
  // byte is replaced by its codebook prefix (or dropped for an unknown code),
  // the remaining bytes are copied verbatim. Empty input has bound 0, which
  // lets the buffered decode paths (`scanAll`, `lookupBatch`) represent empty
  // words without invoking the decoder.
  [[nodiscard]] size_t maxDecompressedSize(
      std::string_view compressedWord) const {
    if (compressedWord.empty()) {
      return 0;
    }
    return prefixForCode(compressedWord[0]).size() + compressedWord.size() - 1;
  }

  // Decompress `compressedWord` into `out`, which must hold at least
  // `maxDecompressedSize(compressedWord)` bytes. Return the number of bytes
  // written.
  [[nodiscard]] size_t decompressInto(std::string_view compressedWord,
                                      ql::span<char> out) const {
    const size_t bound = maxDecompressedSize(compressedWord);
    AD_CONTRACT_CHECK(out.size() >= bound);
    if (bound == 0) {
      return 0;
    }
    const std::string_view prefix = prefixForCode(compressedWord[0]);
    const std::string_view rest = compressedWord.substr(1);
    std::copy(prefix.begin(), prefix.end(), out.begin());
    std::copy(rest.begin(), rest.end(), out.begin() + prefix.size());
    return bound;
  }

  // From the given list of prefixes, build the internal data structure for
  // efficient lookup. The prefixes do not have to be in any specific order. The
  // type of `prefixes` can be any type for which `for (const string& el :
  // prefixes) {...}` works.
  // TODO<joka921> Make this a part of the constructor, as soon as we have
  // integrated this code into qlever.
  template <typename StringRange>
  void buildCodebook(const StringRange& prefixes) {
    for (auto& el : prefixToCode_) {
      el = "";
    }

    codeToPrefix_.clear();
    unsigned char prefixIdx = 0;
    for (const auto& fulltext : prefixes) {
      if (prefixIdx >= NUM_COMPRESSION_PREFIXES) {
        AD_THROW(absl::StrCat(
            "More than ", NUM_COMPRESSION_PREFIXES,
            " prefixes have been specified. This should never happen"));
      }
      prefixToCode_[prefixIdx] = fulltext;
      codeToPrefix_.emplace_back(prefixIdx + MIN_COMPRESSION_PREFIX, fulltext);
      prefixIdx++;
    }

    // if longest strings come first we correctly handle overlapping prefixes
    auto pred = [](const PrefixCode& a, const PrefixCode& b) {
      return a.prefix_.size() > b.prefix_.size();
    };
    std::sort(codeToPrefix_.begin(), codeToPrefix_.end(), pred);
  }

  const auto& prefixToCode() const { return prefixToCode_; }
};

#endif  // QLEVER_PREFIXCOMPRESSOR_H
