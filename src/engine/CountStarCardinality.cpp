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

// The first index `i` in `[from, col.size())` with `!pred(col[i])`, where
// `pred` holds on a prefix of `col`. Exponential search from `from`, then a
// binary search in the last step, so the cost is logarithmic in the distance
// `i - from`, not in the block size.
template <typename Pred>
size_t gallop(ql::span<const Id> col, size_t from, const Pred& pred) {
  if (from >= col.size() || !pred(col[from])) {
    return from;
  }
  size_t lo = from + 1;
  size_t step = 1;
  size_t probe = from + step;
  while (probe < col.size() && pred(col[probe])) {
    lo = probe + 1;
    step *= 2;
    probe = from + step;
  }
  const size_t hi = std::min(probe, col.size());
  return static_cast<size_t>(
      std::partition_point(col.begin() + lo, col.begin() + hi, pred) -
      col.begin());
}

// The IDs of a permutation without located triples are never
// `LocalVocabIndex`, so their order is the order of their bits (see
// `ValueId::compareWithoutLocalVocab`). Comparing the bits inlines to one
// integer comparison, unlike the general `ValueId::compareThreeWay`.
bool idLess(Id a, Id b) { return a.compareWithoutLocalVocab(b) < 0; }
bool idEqual(Id a, Id b) { return a.getBits() == b.getBits(); }

// A cursor over the first column of a lazy index scan whose result is sorted
// on that column. Holds one block at a time, so memory stays bounded by the
// block size. `skipTo` skips non-matching keys with an exponential search
// and drops a whole block with one comparison when all its keys are too
// small, so the merge below does work proportional to the matches, not to
// the rows of the larger side.
class FirstColumnCursor {
  using Blocks = CompressedRelationReader::IdTableGeneratorInputRange;
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  Blocks blocks_;
  CancellationHandle cancellationHandle_;
  std::optional<IdTable> block_;
  size_t pos_ = 0;

  ql::span<const Id> column() const { return block_->getColumn(0); }

 public:
  FirstColumnCursor(Blocks blocks, CancellationHandle cancellationHandle)
      : blocks_{std::move(blocks)},
        cancellationHandle_{std::move(cancellationHandle)} {}

  // Make the current block hold at least one unread row. Return false when
  // the scan is exhausted.
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

  // The key of the current row. Requires `fillBlock()` to have returned true.
  Id current() const { return column()[pos_]; }

  // Skip all rows whose key is less than `target`.
  void skipTo(Id target) {
    auto isLess = [target](Id id) { return idLess(id, target); };
    while (fillBlock()) {
      const auto col = column();
      if (isLess(col.back())) {
        pos_ = col.size();
        continue;
      }
      pos_ = gallop(col, pos_, isLess);
      return;
    }
  }

  // Consume all rows whose key equals the current key and return their
  // number. The run may span several blocks. Requires `fillBlock()` to have
  // returned true.
  size_t consumeRun() {
    const Id id = current();
    auto isEqual = [id](Id other) { return idEqual(other, id); };
    size_t count = 0;
    while (fillBlock() && isEqual(current())) {
      const auto col = column();
      const size_t end = gallop(col, pos_, isEqual);
      count += end - pos_;
      pos_ = end;
    }
    return count;
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
    FirstColumnCursor left{std::move(leftBlocks), cancellationHandle};
    FirstColumnCursor right{std::move(rightBlocks), cancellationHandle};

    markOptimizedOut(join, children[0]->getRootOperation(),
                     children[1]->getRootOperation());

    // Merge the two sorted key streams; each common key contributes the
    // product of its multiplicities. A side whose key is smaller skips ahead
    // to the other side's key.
    size_t total = 0;
    while (left.fillBlock() && right.fillBlock()) {
      const Id l = left.current();
      const Id r = right.current();
      if (idEqual(l, r)) {
        const size_t leftCount = left.consumeRun();
        total += leftCount * right.consumeRun();
      } else if (idLess(l, r)) {
        left.skipTo(r);
      } else {
        right.skipTo(l);
      }
    }
    return total;
  }

  return std::nullopt;
}
