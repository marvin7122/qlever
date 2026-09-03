// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "index/CompressedRelation.h"
#include "util/Exception.h"

namespace ql::index::v2 {

// _____________________________________________________________________________
// 8-bit Datatype Bitmask for fast type-filtering and pruning at block granularity.
enum class DatatypeBitmask : uint8_t {
  None = 0,
  Iri = 1 << 0,
  Literal = 1 << 1,
  Integer = 1 << 2,
  Double = 1 << 3,
  Date = 1 << 4,
  BlankNode = 1 << 5,
  VocabWord = 1 << 6,
  All = 0xFF
};

[[nodiscard]] constexpr DatatypeBitmask operator|(DatatypeBitmask a,
                                                 DatatypeBitmask b) noexcept {
  return static_cast<DatatypeBitmask>(static_cast<uint8_t>(a) |
                                      static_cast<uint8_t>(b));
}

[[nodiscard]] constexpr DatatypeBitmask operator&(DatatypeBitmask a,
                                                 DatatypeBitmask b) noexcept {
  return static_cast<DatatypeBitmask>(static_cast<uint8_t>(a) &
                                      static_cast<uint8_t>(b));
}

[[nodiscard]] constexpr bool hasFlag(DatatypeBitmask mask,
                                     DatatypeBitmask flag) noexcept {
  return (static_cast<uint8_t>(mask) & static_cast<uint8_t>(flag)) != 0;
}

// _____________________________________________________________________________
// Leaflet metadata header stored per compressed index block.
struct LeafletBlockHeader {
  // Exact column boundary values within the block.
  Id col0Min_{Id::makeUndefined()};
  Id col0Max_{Id::makeUndefined()};
  Id col1Min_{Id::makeUndefined()};
  Id col1Max_{Id::makeUndefined()};

  // Exact distinct counts within the block.
  uint32_t numDistinctCol0_ = 0;
  uint32_t numDistinctCol1_ = 0;

  // 8-bit Datatype Bitmasks for each column.
  DatatypeBitmask datatypeBitmaskCol0_ = DatatypeBitmask::None;
  DatatypeBitmask datatypeBitmaskCol1_ = DatatypeBitmask::None;
};

// _____________________________________________________________________________
// V2 Augmented Block Metadata combining physical layout with leaflet headers.
struct CompressedBlockMetadataV2 {
  CompressedBlockMetadata baseMetadata_;
  LeafletBlockHeader leafletHeader_;

  [[nodiscard]] size_t numRows() const noexcept {
    return baseMetadata_.numRows_;
  }
};

// _____________________________________________________________________________
// V2 Augmented Relation Metadata recording exact integer distinct counts.
struct CompressedRelationMetadataV2 {
  CompressedRelationMetadata baseMetadata_;
  uint64_t exactDistinctCol1_ = 0;
  uint64_t exactDistinctCol2_ = 0;

  [[nodiscard]] size_t numRows() const noexcept {
    return baseMetadata_.numRows_;
  }
};

// _____________________________________________________________________________
// High-performance metadata evaluator resolving counts, min/max, and type
// filters in O(1) or O(blocks) without decompressing triple data rows.
class LeafletAggregator {
 public:
  // O(1) minimum boundary value for a sorted relation block sequence.
  [[nodiscard]] static std::optional<Id> getMinCol1(
      ql::span<const CompressedBlockMetadataV2> blocks) noexcept {
    if (blocks.empty()) {
      return std::nullopt;
    }
    return blocks.front().leafletHeader_.col1Min_;
  }

  // O(1) maximum boundary value for a sorted relation block sequence.
  [[nodiscard]] static std::optional<Id> getMaxCol1(
      ql::span<const CompressedBlockMetadataV2> blocks) noexcept {
    if (blocks.empty()) {
      return std::nullopt;
    }
    return blocks.back().leafletHeader_.col1Max_;
  }

  // O(1) exact distinct count for single-predicate relations.
  [[nodiscard]] static uint64_t getExactDistinctCol1(
      const CompressedRelationMetadataV2& metadata) noexcept {
    return metadata.exactDistinctCol1_;
  }

  [[nodiscard]] static uint64_t getExactDistinctCol2(
      const CompressedRelationMetadataV2& metadata) noexcept {
    return metadata.exactDistinctCol2_;
  }

  // O(blocks) typed count evaluator. Returns exact count for pure blocks
  // and identifies ambiguous blocks that require row-level decompression.
  struct TypedCountResult {
    uint64_t exactCount = 0;
    std::vector<size_t> ambiguousBlockIndices;
  };

  [[nodiscard]] static TypedCountResult countTypedColumn(
      ql::span<const CompressedBlockMetadataV2> blocks,
      DatatypeBitmask targetType,
      size_t columnIndex = 1) {
    TypedCountResult result;
    for (size_t i = 0; i < blocks.size(); ++i) {
      const auto& header = blocks[i].leafletHeader_;
      const auto mask = (columnIndex == 0) ? header.datatypeBitmaskCol0_
                                           : header.datatypeBitmaskCol1_;

      if (mask == targetType) {
        // Block is 100% composed of the target type -> add all rows directly.
        result.exactCount += blocks[i].numRows();
      } else if (!hasFlag(mask, targetType)) {
        // Block contains zero elements of the target type -> skip entirely.
        continue;
      } else {
        // Block contains mixed types -> must decompress and filter.
        result.ambiguousBlockIndices.push_back(i);
      }
    }
    return result;
  }
};

}  // namespace ql::index::v2
