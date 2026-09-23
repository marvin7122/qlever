// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>

#include "engine/CountStarCardinality.h"

#include <array>
#include <memory>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Operation.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Sort.h"
#include "engine/Union.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "index/ScanSpecification.h"
#include "index/TripleComponentConversions.h"
#include "util/CancellationHandle.h"
#include "util/HashMap.h"

namespace {
// An index scan whose exact result size can be taken from metadata: no graph
// filter is active, and no additional variables are present. Checked
// conservatively; failing either falls back to evaluating the subtree.
bool isMetadataEligibleScan(const IndexScan& scan) {
  return scan.graphsToFilter().areAllGraphsAllowed() &&
         scan.additionalVariables().empty();
}

std::optional<Permutation::Enum> permutationWithWantedCol1(
    const IndexScan& scan, const Variable& wantedCol1) {
  const bool predBound = !scan.predicate().isVariable();
  const bool subjBound = !scan.subject().isVariable();
  const bool objBound = !scan.object().isVariable();
  if (scan.subject().isVariable() &&
      scan.subject().getVariable() == wantedCol1) {
    if (predBound) {
      return Permutation::PSO;
    }
    if (objBound) {
      return Permutation::OSP;
    }
  } else if (scan.object().isVariable() &&
             scan.object().getVariable() == wantedCol1) {
    if (predBound) {
      return Permutation::POS;
    }
    if (subjBound) {
      return Permutation::SOP;
    }
  } else if (scan.predicate().isVariable() &&
             scan.predicate().getVariable() == wantedCol1) {
    if (subjBound) {
      return Permutation::SPO;
    }
    if (objBound) {
      return Permutation::OPS;
    }
  }
  return std::nullopt;
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

const IndexScan* unwrapIndexScan(const QueryExecutionTree& tree) {
  std::shared_ptr<Operation> op = tree.getRootOperation();
  if (auto* sort = dynamic_cast<Sort*>(op.get())) {
    const auto children = sort->getChildren();
    if (children.size() != 1) {
      return nullptr;
    }
    op = children[0]->getRootOperation();
  }
  return dynamic_cast<const IndexScan*>(op.get());
}

// Mark the optimized-out operations for runtime information display.
void markOptimizedOut(Operation* parent, const std::shared_ptr<Operation>& left,
                      const std::shared_ptr<Operation>& right) {
  left->updateRuntimeInformationWhenOptimizedOut({});
  right->updateRuntimeInformationWhenOptimizedOut({});
  parent->updateRuntimeInformationWhenOptimizedOut(
      {left->getRuntimeInfoPointer(), right->getRuntimeInfoPointer()});
}

// Inner join size of two `(join-key, multiplicity)` histograms that are both
// sorted by the join key: the sum over the shared keys of the multiplicity
// products.
size_t zipperInnerProduct(const IdTable& left, const IdTable& right) {
  size_t i = 0;
  size_t j = 0;
  size_t total = 0;
  while (i < left.numRows() && j < right.numRows()) {
    const Id leftId = left(i, 0);
    const Id rightId = right(j, 0);
    if (leftId == rightId) {
      total += static_cast<size_t>(left(i, 1).getInt()) *
               static_cast<size_t>(right(j, 1).getInt());
      ++i;
      ++j;
    } else if (leftId < rightId) {
      ++i;
    } else {
      ++j;
    }
  }
  return total;
}

// `COUNT(*)` of an inner join of two eligible bound-predicate scans on a
// single variable: the sum of the multiplicity products from the distinct
// col1 counts, like the last hop of the chain fold below.
std::optional<size_t> tryTwoScanJoin(
    const QueryExecutionTree& tree, Join* join,
    const std::vector<QueryExecutionTree*>& children) {
  const auto* leftScan = unwrapIndexScan(*children[0]);
  const auto* rightScan = unwrapIndexScan(*children[1]);
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
  if (joinColumns.size() != 1) {
    return std::nullopt;
  }
  auto joinVar =
      children[0]->getVariableAndInfoByColumnIndex(joinColumns[0][0]).first;
  const Index& index = tree.getRootOperation()->getIndex();
  const auto& locatedTriplesState =
      tree.getRootOperation()->locatedTriplesState();
  const auto& cancellationHandle =
      tree.getRootOperation()->getCancellationHandle();

  auto distinctCounts = [&](const IndexScan& scan) -> std::optional<IdTable> {
    const auto& locTriples =
        scan.permutation().getLocatedTriplesForPermutation(locatedTriplesState);
    if (!locTriples.isEmpty() || scan.permutation().permutationType() ==
                                     Permutation::Type::MATERIALIZED_VIEW) {
      return std::nullopt;
    }
    const auto& permutedTriple = scan.getPermutedTriple();
    std::optional<Id> col0Id = toValueId(*permutedTriple[0], index);
    if (!col0Id.has_value()) {
      return std::nullopt;
    }
    auto target = permutationWithWantedCol1(scan, joinVar);
    if (!target.has_value()) {
      return std::nullopt;
    }
    const auto& permutation = index.getImpl().getPermutation(target.value());
    return permutation.getDistinctCol1IdsAndCounts(
        col0Id.value(), cancellationHandle, locatedTriplesState,
        scan.getLimitOffset());
  };

  auto leftCounts = distinctCounts(*leftScan);
  auto rightCounts = distinctCounts(*rightScan);
  if (!leftCounts.has_value() || !rightCounts.has_value()) {
    return std::nullopt;
  }

  markOptimizedOut(join, children[0]->getRootOperation(),
                   children[1]->getRootOperation());
  return zipperInnerProduct(leftCounts.value(), rightCounts.value());
}

// A scan shape the join-chain fold understands: exactly two variables with a
// bound predicate (`?s <p> ?o`), no graph filter, no additional variables.
bool isChainEligibleScan(const IndexScan& scan) {
  return scan.numVariables() == 2 && !scan.predicate().isVariable() &&
         scan.subject().isVariable() && scan.object().isVariable() &&
         isMetadataEligibleScan(scan);
}

// Collect the chain-eligible scans under a tree of binary inner joins,
// unwrapping `Sort` nodes. Join and scan nodes are recorded in `nodes` for
// runtime-information marking. Return false unless every leaf is a
// chain-eligible scan.
bool collectChainScans(const QueryExecutionTree* tree,
                       std::vector<const IndexScan*>& scans,
                       std::vector<const QueryExecutionTree*>& nodes) {
  if (auto* join = dynamic_cast<Join*>(tree->getRootOperation().get())) {
    auto children = join->getChildren();
    if (children.size() != 2) {
      return false;
    }
    nodes.push_back(tree);
    return collectChainScans(children[0], scans, nodes) &&
           collectChainScans(children[1], scans, nodes);
  }
  const auto* scan = unwrapIndexScan(*tree);
  if (!scan || !isChainEligibleScan(*scan)) {
    return false;
  }
  nodes.push_back(tree);
  scans.push_back(scan);
  return true;
}

// The single variable the two scans have in common, or `std::nullopt` when
// they share none or more than one.
std::optional<Variable> sharedChainVar(const IndexScan& left,
                                       const IndexScan& right) {
  std::array<Variable, 2> leftVars{left.subject().getVariable(),
                                   left.object().getVariable()};
  std::array<Variable, 2> rightVars{right.subject().getVariable(),
                                    right.object().getVariable()};
  std::optional<Variable> found;
  for (const auto& leftVar : leftVars) {
    for (const auto& rightVar : rightVars) {
      if (leftVar == rightVar) {
        if (found.has_value() && found.value() != leftVar) {
          return std::nullopt;
        }
        found = leftVar;
      }
    }
  }
  return found;
}

// Read-only execution context for the metadata reads of the chain fold.
struct ChainContext {
  const Index& index;
  const LocatedTriplesState& locatedTriplesState;
  const SharedCancellationHandle& cancellationHandle;
  const ad_utility::AllocatorWithLimit<Id>& alloc;
};

// `(join-key, multiplicity)` histogram of `scan` for `joinVar`, read from the
// permutation that has the join variable in col1. An IRI that is missing from
// the index yields an empty histogram (no triple can match it), not a
// fallback.
std::optional<IdTable> chainDistinctCounts(const IndexScan& scan,
                                           const Variable& joinVar,
                                           const ChainContext& ctx) {
  const auto& locTriples = scan.permutation().getLocatedTriplesForPermutation(
      ctx.locatedTriplesState);
  if (!locTriples.isEmpty() || scan.permutation().permutationType() ==
                                   Permutation::Type::MATERIALIZED_VIEW) {
    return std::nullopt;
  }
  const auto& permutedTriple = scan.getPermutedTriple();
  std::optional<Id> col0Id = toValueId(*permutedTriple[0], ctx.index);
  if (!col0Id.has_value()) {
    return IdTable{2, ctx.alloc};
  }
  auto target = permutationWithWantedCol1(scan, joinVar);
  if (!target.has_value()) {
    return std::nullopt;
  }
  const auto& permutation = ctx.index.getImpl().getPermutation(target.value());
  return permutation.getDistinctCol1IdsAndCounts(
      col0Id.value(), ctx.cancellationHandle, ctx.locatedTriplesState,
      scan.getLimitOffset());
}

// `(in-key, out-key)` pairs of `scan`, read from the permutation that has
// `col1Var` in col1. A missing IRI yields no pairs.
std::optional<IdTable> chainScanPairs(const IndexScan& scan,
                                      const Variable& col1Var,
                                      const ChainContext& ctx) {
  const auto& locTriples = scan.permutation().getLocatedTriplesForPermutation(
      ctx.locatedTriplesState);
  if (!locTriples.isEmpty() || scan.permutation().permutationType() ==
                                   Permutation::Type::MATERIALIZED_VIEW) {
    return std::nullopt;
  }
  auto target = permutationWithWantedCol1(scan, col1Var);
  if (!target.has_value()) {
    return std::nullopt;
  }
  std::optional<Id> col0Id = toValueId(*scan.getPermutedTriple()[0], ctx.index);
  if (!col0Id.has_value()) {
    return IdTable{2, ctx.alloc};
  }
  const auto& permutation = ctx.index.getImpl().getPermutation(target.value());
  const Permutation::ColumnIndices extra{};
  return permutation.scan(
      permutation.getScanSpecAndBlocks(
          ScanSpecification{col0Id.value(), std::nullopt, std::nullopt},
          ctx.locatedTriplesState),
      extra, ctx.cancellationHandle, ctx.locatedTriplesState,
      scan.getLimitOffset());
}

// Push a `(join-key, multiplicity)` histogram through `(in-key, out-key)`
// pairs: each outgoing key accumulates the multiplicities of its incoming
// keys. Both inputs are sorted by their first column; so is the result.
IdTable pushHistogramThroughPairs(
    const IdTable& hist, const IdTable& pairs,
    const ad_utility::AllocatorWithLimit<Id>& alloc) {
  ad_utility::HashMap<Id, int64_t> outgoing;
  size_t histRow = 0;
  size_t pairRow = 0;
  while (histRow < hist.numRows() && pairRow < pairs.numRows()) {
    const Id histId = hist(histRow, 0);
    const Id pairId = pairs(pairRow, 0);
    if (histId == pairId) {
      outgoing[pairs(pairRow, 1)] += hist(histRow, 1).getInt();
      ++pairRow;
    } else if (histId < pairId) {
      ++histRow;
    } else {
      ++pairRow;
    }
  }
  std::vector<std::pair<Id, int64_t>> rows;
  rows.reserve(outgoing.size());
  for (const auto& entry : outgoing) {
    rows.emplace_back(entry.first, entry.second);
  }
  ql::ranges::sort(rows);
  IdTable table{2, alloc};
  table.reserve(rows.size());
  for (const auto& [id, count] : rows) {
    table.push_back({id, Id::makeFromInt(count)});
  }
  return table;
}

// Mark every node that the metadata answer replaces.
void markChainOptimizedOut(
    const std::vector<const QueryExecutionTree*>& nodes) {
  for (const auto* node : nodes) {
    node->getRootOperation()->updateRuntimeInformationWhenOptimizedOut({});
  }
}

// `COUNT(*)` of a join chain: a path of at least three bound-predicate
// two-variable scans where the variable-incidence graph has two degree-1
// endpoints and every other variable has degree 2. Fold `(join-key,
// multiplicity)` histograms along the path; the last hop uses the same
// distinct-count zipper as the two-scan case. A hop with exact size zero
// answers zero without scanning later hops.
std::optional<size_t> tryChainFold(const QueryExecutionTree& tree) {
  std::vector<const IndexScan*> scans;
  std::vector<const QueryExecutionTree*> nodes;
  if (!collectChainScans(&tree, scans, nodes) || scans.size() < 3) {
    return std::nullopt;
  }

  ad_utility::HashMap<Variable, std::vector<size_t>> scansByVar;
  for (size_t i = 0; i < scans.size(); ++i) {
    scansByVar[scans[i]->subject().getVariable()].push_back(i);
    scansByVar[scans[i]->object().getVariable()].push_back(i);
  }
  std::vector<Variable> ends;
  for (const auto& [var, idxs] : scansByVar) {
    if (idxs.size() == 1) {
      ends.push_back(var);
    } else if (idxs.size() != 2) {
      return std::nullopt;
    }
  }
  if (ends.size() != 2) {
    return std::nullopt;
  }

  std::vector<const IndexScan*> ordered;
  std::vector<char> used(scans.size(), 0);
  Variable current = ends.front();
  while (ordered.size() < scans.size()) {
    auto it = scansByVar.find(current);
    if (it == scansByVar.end()) {
      return std::nullopt;
    }
    std::optional<size_t> nextIdx;
    for (size_t idx : it->second) {
      if (used[idx] == 0) {
        nextIdx = idx;
        break;
      }
    }
    if (!nextIdx.has_value()) {
      return std::nullopt;
    }
    used[nextIdx.value()] = 1;
    const auto* scan = scans[nextIdx.value()];
    ordered.push_back(scan);
    current = scan->subject().getVariable() == current
                  ? scan->object().getVariable()
                  : scan->subject().getVariable();
  }

  for (const auto* scan : ordered) {
    if (scan->getLimitOffset().actualSize(scan->getExactSize()) == 0) {
      markChainOptimizedOut(nodes);
      return 0;
    }
  }

  const Index& index = tree.getRootOperation()->getIndex();
  ChainContext ctx{
      index, tree.getRootOperation()->locatedTriplesState(),
      tree.getRootOperation()->getCancellationHandle(),
      tree.getRootOperation()->getExecutionContext()->getAllocator()};

  auto firstJoin = sharedChainVar(*ordered[0], *ordered[1]);
  if (!firstJoin.has_value()) {
    return std::nullopt;
  }
  auto hist = chainDistinctCounts(*ordered[0], firstJoin.value(), ctx);
  if (!hist.has_value()) {
    return std::nullopt;
  }
  for (size_t hop = 1; hop + 1 < ordered.size(); ++hop) {
    auto inVar = sharedChainVar(*ordered[hop - 1], *ordered[hop]);
    if (!inVar.has_value()) {
      return std::nullopt;
    }
    auto pairs = chainScanPairs(*ordered[hop], inVar.value(), ctx);
    if (!pairs.has_value()) {
      return std::nullopt;
    }
    hist = pushHistogramThroughPairs(hist.value(), pairs.value(), ctx.alloc);
  }
  auto lastJoin = sharedChainVar(*ordered[ordered.size() - 2], *ordered.back());
  if (!lastJoin.has_value()) {
    return std::nullopt;
  }
  auto lastCounts = chainDistinctCounts(*ordered.back(), lastJoin.value(), ctx);
  if (!lastCounts.has_value()) {
    return std::nullopt;
  }
  markChainOptimizedOut(nodes);
  return zipperInnerProduct(hist.value(), lastCounts.value());
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
    if (auto twoScan = tryTwoScanJoin(tree, join, children)) {
      return twoScan;
    }
    return tryChainFold(tree);
  }

  return std::nullopt;
}
