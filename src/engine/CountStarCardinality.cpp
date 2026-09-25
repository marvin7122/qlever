// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>

#include "engine/CountStarCardinality.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Operation.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Union.h"
#include "index/CompressedRelation.h"
#include "index/Permutation.h"

namespace {
// An index scan whose exact result size can be taken from metadata: no graph
// filter is active, and no additional variables are present. Checked
// conservatively; failing either falls back to evaluating the subtree.
bool isMetadataEligibleScan(const IndexScan& scan) {
  return scan.graphsToFilter().areAllGraphsAllowed() &&
         scan.additionalVariables().empty();
}

// The exact result size of an eligible index scan, respecting LIMIT/OFFSET.
std::optional<size_t> exactSizeIfEligibleScan(const QueryExecutionTree& tree) {
  auto scan =
      std::dynamic_pointer_cast<const IndexScan>(tree.getRootOperation());
  if (!scan || !isMetadataEligibleScan(*scan)) {
    return std::nullopt;
  }
  return scan->getLimitOffset().actualSize(scan->getExactSize());
}

// The run lengths of the first column of a lazy index scan whose result is
// sorted on that column: `next()` yields each distinct value with the number of
// rows that have it, also when a run spans several blocks. Holds one block at a
// time, so memory stays bounded by the block size.
class FirstColumnRuns {
  using Blocks = CompressedRelationReader::IdTableGeneratorInputRange;
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  Blocks blocks_;
  CancellationHandle cancellationHandle_;
  std::optional<IdTable> block_;
  size_t pos_ = 0;

  // Make `block_` hold at least one unread row. Return false at the end.
  bool fillBlock() {
    while (!block_.has_value() || pos_ >= block_->numRows()) {
      auto next = blocks_.get();
      if (!next.has_value()) {
        return false;
      }
      cancellationHandle_->throwIfCancelled();
      block_ = std::move(next);
      pos_ = 0;
    }
    return true;
  }

 public:
  FirstColumnRuns(Blocks blocks, CancellationHandle cancellationHandle)
      : blocks_{std::move(blocks)},
        cancellationHandle_{std::move(cancellationHandle)} {}

  // Return the next distinct value of the first column together with the
  // number of rows that have it, or `std::nullopt` when the scan is exhausted.
  std::optional<std::pair<Id, size_t>> next() {
    if (!fillBlock()) {
      return std::nullopt;
    }
    const Id id = (*block_)(pos_, 0);
    size_t count = 0;
    while (true) {
      const auto col = block_->getColumn(0);
      const auto runEnd = std::find_if(col.begin() + pos_, col.end(),
                                       [id](Id other) { return other != id; });
      const auto newPos = static_cast<size_t>(runEnd - col.begin());
      count += newPos - pos_;
      pos_ = newPos;
      if (pos_ < block_->numRows() || !fillBlock() ||
          (*block_)(pos_, 0) != id) {
        return std::pair{id, count};
      }
    }
  }
};

// Mark the optimized-out operations for runtime information display.
void markOptimizedOut(Operation* parent, const std::shared_ptr<Operation>& left,
                      const std::shared_ptr<Operation>& right) {
  left->updateRuntimeInformationWhenOptimizedOut({});
  right->updateRuntimeInformationWhenOptimizedOut({});
  parent->updateRuntimeInformationWhenOptimizedOut(
      {left->getRuntimeInfoPointer(), right->getRuntimeInfoPointer()});
}
}  // namespace

// _____________________________________________________________________________
std::optional<size_t> computeCountStarCardinality(
    const QueryExecutionTree& tree) {
  if (auto* unionOp = dynamic_cast<Union*>(tree.getRootOperation().get())) {
    auto leftSize = exactSizeIfEligibleScan(*unionOp->leftChild());
    auto rightSize = exactSizeIfEligibleScan(*unionOp->rightChild());
    if (!leftSize.has_value() || !rightSize.has_value()) {
      return std::nullopt;
    }
    markOptimizedOut(unionOp, unionOp->leftChild()->getRootOperation(),
                     unionOp->rightChild()->getRootOperation());
    return leftSize.value() + rightSize.value();
  }

  if (auto* optional =
          dynamic_cast<OptionalJoin*>(tree.getRootOperation().get())) {
    auto children = optional->getChildren();
    if (children.size() != 2) {
      return std::nullopt;
    }
    if (!children[1]->knownEmptyResult()) {
      return std::nullopt;
    }
    auto leftSize = exactSizeIfEligibleScan(*children[0]);
    if (!leftSize.has_value()) {
      return std::nullopt;
    }
    markOptimizedOut(optional, children[0]->getRootOperation(),
                     children[1]->getRootOperation());
    return leftSize.value();
  }

  if (auto* join = dynamic_cast<Join*>(tree.getRootOperation().get())) {
    auto children = join->getChildren();
    if (children.size() != 2) {
      return std::nullopt;
    }
    // Both inputs must be index scans sorted on the join column (the planner
    // chooses such permutations for a join of two scans). Then the count is
    // computed from two lazy scans restricted to the blocks that can match
    // (the same blocks `Join` reads), without materializing the join result
    // or the per-key counts of either side.
    auto leftScan =
        std::dynamic_pointer_cast<IndexScan>(children[0]->getRootOperation());
    auto rightScan =
        std::dynamic_pointer_cast<IndexScan>(children[1]->getRootOperation());
    if (!leftScan || !rightScan) {
      return std::nullopt;
    }
    if (leftScan->numVariables() != 2 || rightScan->numVariables() != 2 ||
        !isMetadataEligibleScan(*leftScan) ||
        !isMetadataEligibleScan(*rightScan)) {
      return std::nullopt;
    }
    auto joinColumns =
        QueryExecutionTree::getJoinColumns(*children[0], *children[1]);
    if (joinColumns.size() != 1 || joinColumns[0][0] != 0 ||
        joinColumns[0][1] != 0) {
      return std::nullopt;
    }
    const auto& locatedTriplesState =
        tree.getRootOperation()->locatedTriplesState();
    auto isPlainPermutation = [&](const IndexScan& scan) {
      return scan.permutation()
                 .getLocatedTriplesForPermutation(locatedTriplesState)
                 .isEmpty() &&
             scan.permutation().permutationType() !=
                 Permutation::Type::MATERIALIZED_VIEW;
    };
    if (!isPlainPermutation(*leftScan) || !isPlainPermutation(*rightScan)) {
      return std::nullopt;
    }

    const auto& cancellationHandle =
        tree.getRootOperation()->getCancellationHandle();
    auto [leftBlocks, rightBlocks] =
        IndexScan::lazyScanForJoinOfTwoScans(*leftScan, *rightScan);
    FirstColumnRuns left{std::move(leftBlocks), cancellationHandle};
    FirstColumnRuns right{std::move(rightBlocks), cancellationHandle};

    markOptimizedOut(join, children[0]->getRootOperation(),
                     children[1]->getRootOperation());

    // Merge the two sorted run streams; each common key contributes the
    // product of its multiplicities.
    size_t total = 0;
    auto l = left.next();
    auto r = right.next();
    while (l.has_value() && r.has_value()) {
      if (l->first == r->first) {
        total += l->second * r->second;
        l = left.next();
        r = right.next();
      } else if (l->first < r->first) {
        l = left.next();
      } else {
        r = right.next();
      }
    }
    return total;
  }

  return std::nullopt;
}
