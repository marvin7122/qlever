// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>

#include "engine/CountStarCardinality.h"

#include <memory>
#include <vector>

#include "engine/ExistsJoin.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Minus.h"
#include "engine/Operation.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Sort.h"
#include "engine/Union.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "index/TripleComponentConversions.h"
#include "util/AllocatorWithLimit.h"

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

// The root operation with all `Sort` layers stripped. The returned pointer is
// only valid as long as `tree` is alive.
Operation* skipSorts(const QueryExecutionTree& tree) {
  std::shared_ptr<Operation> op = tree.getRootOperation();
  while (auto* sort = dynamic_cast<Sort*>(op.get())) {
    const auto children = sort->getChildren();
    if (children.size() != 1) {
      return op.get();
    }
    op = children[0]->getRootOperation();
  }
  return op.get();
}

const IndexScan* unwrapIndexScan(const QueryExecutionTree& tree) {
  return dynamic_cast<const IndexScan*>(skipSorts(tree));
}

// A limit or offset on an operation (as opposed to on an index scan, where the
// histogram and size helpers respect it) changes the result size, so such an
// operation must not be answered from metadata.
bool hasNoLimitOffset(const Operation& op) {
  const auto& limitOffset = op.getLimitOffset();
  return !limitOffset._limit.has_value() && limitOffset._offset == 0;
}

// Mark a whole subtree as optimized out (post-order) for the runtime
// information display. Each operation keeps its children's runtime infos,
// leaves keep none.
void markSubtreeOptimizedOut(Operation* op) {
  std::vector<std::shared_ptr<RuntimeInformation>> childInfos;
  for (QueryExecutionTree* child : op->getChildren()) {
    markSubtreeOptimizedOut(child->getRootOperation().get());
    childInfos.push_back(child->getRootOperation()->getRuntimeInfoPointer());
  }
  op->updateRuntimeInformationWhenOptimizedOut(std::move(childInfos));
}

// How a pair of `(join-key, multiplicity)` histograms is combined.
enum class PairMode {
  // `OPTIONAL`: each left row survives at least once.
  Optional,
  // `MINUS`: only left rows with no join partner survive.
  Minus,
  // Correlated `EXISTS`: only left rows with a join partner survive.
  Semi
};

// Combine a left and a right histogram according to `mode`. Both histograms
// are sorted by key with distinct keys.
size_t zipPair(const IdTable& left, const IdTable& right, PairMode mode) {
  auto count = [](const IdTable& table, size_t row) {
    return static_cast<size_t>(table(row, 1).getInt());
  };
  size_t i = 0;
  size_t j = 0;
  size_t total = 0;
  const size_t numLeft = left.numRows();
  const size_t numRight = right.numRows();
  while (i < numLeft && j < numRight) {
    const Id leftId = left(i, 0);
    const Id rightId = right(j, 0);
    if (leftId == rightId) {
      const size_t leftCount = count(left, i);
      if (mode == PairMode::Optional) {
        // A present key always has a nonzero right multiplicity.
        total += leftCount * count(right, j);
      } else if (mode == PairMode::Semi) {
        total += leftCount;
      }
      // `Minus`: matching keys contribute nothing.
      ++i;
      ++j;
    } else if (leftId < rightId) {
      if (mode != PairMode::Semi) {
        total += count(left, i);
      }
      ++i;
    } else {
      // Right-only keys contribute nothing in all modes.
      ++j;
    }
  }
  // Leftover left keys survive in all modes but `Semi`.
  if (mode != PairMode::Semi) {
    while (i < numLeft) {
      total += count(left, i);
      ++i;
    }
  }
  return total;
}

// Inner-join cardinality of `k >= 2` histograms: the sum over the keys that
// are present in all histograms of the product of their multiplicities.
size_t zipInnerJoin(const std::vector<IdTable>& histograms) {
  std::vector<size_t> positions(histograms.size(), 0);
  size_t total = 0;
  while (true) {
    // The greatest key at any cursor; all cursors must reach it for a match.
    std::optional<Id> target;
    for (size_t i = 0; i < histograms.size(); ++i) {
      if (positions[i] >= histograms[i].numRows()) {
        return total;
      }
      const Id key = histograms[i](positions[i], 0);
      if (!target.has_value() || target.value() < key) {
        target = key;
      }
    }
    size_t product = 1;
    bool matched = true;
    for (size_t i = 0; i < histograms.size(); ++i) {
      while (positions[i] < histograms[i].numRows() &&
             histograms[i](positions[i], 0) < target.value()) {
        ++positions[i];
      }
      if (positions[i] >= histograms[i].numRows()) {
        return total;
      }
      if (!(histograms[i](positions[i], 0) == target.value())) {
        // This cursor overshoots the target; retry with a larger target.
        matched = false;
        break;
      }
      product *= static_cast<size_t>(histograms[i](positions[i], 1).getInt());
    }
    if (matched) {
      total += product;
      for (size_t i = 0; i < histograms.size(); ++i) {
        ++positions[i];
      }
    }
  }
}

// Collect the leaf scans of an inner-join tree (`Sort` layers are
// transparent). Every intermediate `Join` must join on exactly `joinVar` and
// carry no limit or offset, and every leaf must be an eligible two-variable
// scan. Anything else fails closed.
bool collectJoinScans(const QueryExecutionTree& tree, const Variable& joinVar,
                      std::vector<const IndexScan*>& scans) {
  if (auto* join = dynamic_cast<Join*>(skipSorts(tree))) {
    if (!hasNoLimitOffset(*join)) {
      return false;
    }
    auto children = join->getChildren();
    if (children.size() != 2) {
      return false;
    }
    auto joinColumns =
        QueryExecutionTree::getJoinColumns(*children[0], *children[1]);
    if (joinColumns.size() != 1) {
      return false;
    }
    if (children[0]->getVariableAndInfoByColumnIndex(joinColumns[0][0]).first !=
        joinVar) {
      return false;
    }
    return collectJoinScans(*children[0], joinVar, scans) &&
           collectJoinScans(*children[1], joinVar, scans);
  }
  const auto* scan = dynamic_cast<const IndexScan*>(skipSorts(tree));
  if (!scan || scan->numVariables() != 2 || !isMetadataEligibleScan(*scan)) {
    return false;
  }
  scans.push_back(scan);
  return true;
}
}  // namespace

// _____________________________________________________________________________
std::optional<size_t> computeCountStarCardinality(
    const QueryExecutionTree& tree) {
  const Index& index = tree.getRootOperation()->getIndex();
  const auto& locatedTriplesState =
      tree.getRootOperation()->locatedTriplesState();
  const auto& cancellationHandle =
      tree.getRootOperation()->getCancellationHandle();

  // The `(join-key, multiplicity)` histogram of an eligible two-variable scan
  // for `joinVar`: the sorted distinct `col1` Ids of the matching permutation
  // with their counts. A bound predicate that is missing from the vocabulary
  // matches nothing, so it yields an empty histogram (an `OPTIONAL` or `MINUS`
  // then keeps all left rows, an inner join or `EXISTS` yields zero).
  auto histogram = [&](const IndexScan& scan,
                       const Variable& joinVar) -> std::optional<IdTable> {
    const auto& locTriples =
        scan.permutation().getLocatedTriplesForPermutation(locatedTriplesState);
    if (!locTriples.isEmpty() || scan.permutation().permutationType() ==
                                     Permutation::Type::MATERIALIZED_VIEW) {
      return std::nullopt;
    }
    const auto& permutedTriple = scan.getPermutedTriple();
    std::optional<Id> col0Id = toValueId(*permutedTriple[0], index);
    if (!col0Id.has_value()) {
      return IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()};
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

  // Both sides of a two-scan `OPTIONAL`, `MINUS`, or correlated `EXISTS`:
  // eligible two-variable scans that share exactly one variable.
  auto twoScanHistograms = [&](QueryExecutionTree* leftTree,
                               QueryExecutionTree* rightTree,
                               const Variable& joinVar)
      -> std::optional<std::pair<IdTable, IdTable>> {
    const auto* leftScan = unwrapIndexScan(*leftTree);
    const auto* rightScan = unwrapIndexScan(*rightTree);
    if (!leftScan || !rightScan || leftScan->numVariables() != 2 ||
        rightScan->numVariables() != 2 || !isMetadataEligibleScan(*leftScan) ||
        !isMetadataEligibleScan(*rightScan)) {
      return std::nullopt;
    }
    auto joinColumns =
        QueryExecutionTree::getJoinColumns(*leftTree, *rightTree);
    if (joinColumns.size() != 1 ||
        leftTree->getVariableAndInfoByColumnIndex(joinColumns[0][0]).first !=
            joinVar) {
      return std::nullopt;
    }
    auto leftCounts = histogram(*leftScan, joinVar);
    auto rightCounts = histogram(*rightScan, joinVar);
    if (!leftCounts.has_value() || !rightCounts.has_value()) {
      return std::nullopt;
    }
    return std::pair{std::move(leftCounts.value()),
                     std::move(rightCounts.value())};
  };

  if (auto* unionOp = dynamic_cast<Union*>(tree.getRootOperation().get())) {
    if (!hasNoLimitOffset(*unionOp)) {
      return std::nullopt;
    }
    auto leftSize = exactSizeIfEligibleScan(*unionOp->leftChild());
    auto rightSize = exactSizeIfEligibleScan(*unionOp->rightChild());
    if (!leftSize.has_value() || !rightSize.has_value()) {
      return std::nullopt;
    }
    markSubtreeOptimizedOut(unionOp);
    return leftSize.value() + rightSize.value();
  }

  if (auto* optional =
          dynamic_cast<OptionalJoin*>(tree.getRootOperation().get())) {
    if (!hasNoLimitOffset(*optional)) {
      return std::nullopt;
    }
    auto children = optional->getChildren();
    if (children.size() != 2) {
      return std::nullopt;
    }
    if (children[1]->knownEmptyResult()) {
      auto leftSize = exactSizeIfEligibleScan(*children[0]);
      if (!leftSize.has_value()) {
        return std::nullopt;
      }
      markSubtreeOptimizedOut(optional);
      return leftSize.value();
    }
    auto joinColumns =
        QueryExecutionTree::getJoinColumns(*children[0], *children[1]);
    if (joinColumns.size() != 1) {
      return std::nullopt;
    }
    auto joinVar =
        children[0]->getVariableAndInfoByColumnIndex(joinColumns[0][0]).first;
    auto histograms = twoScanHistograms(children[0], children[1], joinVar);
    if (!histograms.has_value()) {
      return std::nullopt;
    }
    markSubtreeOptimizedOut(optional);
    return zipPair(histograms->first, histograms->second, PairMode::Optional);
  }

  if (auto* minus = dynamic_cast<Minus*>(tree.getRootOperation().get())) {
    if (!hasNoLimitOffset(*minus)) {
      return std::nullopt;
    }
    auto children = minus->getChildren();
    if (children.size() != 2) {
      return std::nullopt;
    }
    // Without shared variables `MINUS` removes nothing; with more than one
    // shared variable a single-key zipper is unsound. Both fail closed.
    auto joinColumns =
        QueryExecutionTree::getJoinColumns(*children[0], *children[1]);
    if (joinColumns.size() != 1) {
      return std::nullopt;
    }
    auto joinVar =
        children[0]->getVariableAndInfoByColumnIndex(joinColumns[0][0]).first;
    auto histograms = twoScanHistograms(children[0], children[1], joinVar);
    if (!histograms.has_value()) {
      return std::nullopt;
    }
    markSubtreeOptimizedOut(minus);
    return zipPair(histograms->first, histograms->second, PairMode::Minus);
  }

  if (auto* filter = dynamic_cast<Filter*>(tree.getRootOperation().get())) {
    if (!hasNoLimitOffset(*filter)) {
      return std::nullopt;
    }
    auto* existsJoin =
        dynamic_cast<ExistsJoin*>(skipSorts(*filter->getSubtree()));
    if (!existsJoin || !hasNoLimitOffset(*existsJoin)) {
      return std::nullopt;
    }
    // Only a filter that checks exactly the `ExistsJoin`'s result variable is
    // a semijoin. After planning this is either still an `ExistsExpression`
    // (it evaluates by reading the result column) or a single variable.
    const auto& expression = filter->getExpression();
    bool isSemijoinFilter = false;
    if (const auto* existsExpression =
            dynamic_cast<const sparqlExpression::ExistsExpression*>(
                expression.getPimpl())) {
      isSemijoinFilter =
          existsExpression->variable() == existsJoin->getExistsVariable();
    } else if (auto variable = expression.getVariableOrNullopt();
               variable.has_value()) {
      isSemijoinFilter = variable.value() == existsJoin->getExistsVariable();
    }
    if (!isSemijoinFilter) {
      return std::nullopt;
    }
    auto children = existsJoin->getChildren();
    if (children.size() != 2) {
      return std::nullopt;
    }
    // An uncorrelated `EXISTS` (no shared variable) is handled by the
    // dedicated `ExistsJoin` evaluation; do not steal it here.
    auto joinColumns =
        QueryExecutionTree::getJoinColumns(*children[0], *children[1]);
    if (joinColumns.size() != 1) {
      return std::nullopt;
    }
    auto joinVar =
        children[0]->getVariableAndInfoByColumnIndex(joinColumns[0][0]).first;
    auto histograms = twoScanHistograms(children[0], children[1], joinVar);
    if (!histograms.has_value()) {
      return std::nullopt;
    }
    markSubtreeOptimizedOut(filter);
    return zipPair(histograms->first, histograms->second, PairMode::Semi);
  }

  if (auto* join = dynamic_cast<Join*>(tree.getRootOperation().get())) {
    if (!hasNoLimitOffset(*join)) {
      return std::nullopt;
    }
    auto children = join->getChildren();
    if (children.size() != 2) {
      return std::nullopt;
    }
    auto joinColumns =
        QueryExecutionTree::getJoinColumns(*children[0], *children[1]);
    if (joinColumns.size() != 1) {
      return std::nullopt;
    }
    auto joinVar =
        children[0]->getVariableAndInfoByColumnIndex(joinColumns[0][0]).first;
    // A single common join variable across the whole tree: a two-scan join or
    // a k-star. A chain (different join variables per level) fails closed.
    std::vector<const IndexScan*> scans;
    if (!collectJoinScans(*children[0], joinVar, scans) ||
        !collectJoinScans(*children[1], joinVar, scans)) {
      return std::nullopt;
    }
    std::vector<IdTable> histograms;
    for (const auto* scan : scans) {
      auto counts = histogram(*scan, joinVar);
      if (!counts.has_value()) {
        return std::nullopt;
      }
      histograms.push_back(std::move(counts.value()));
    }
    markSubtreeOptimizedOut(join);
    return zipInnerJoin(histograms);
  }

  return std::nullopt;
}
