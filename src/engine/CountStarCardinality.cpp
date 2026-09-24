// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>

#include "engine/CountStarCardinality.h"

#include <memory>

#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Operation.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Sort.h"
#include "engine/Union.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "index/TripleComponentConversions.h"

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
          scan.permutation().getLocatedTriplesForPermutation(
              locatedTriplesState);
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

    const auto& left = leftCounts.value();
    const auto& right = rightCounts.value();
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

  return std::nullopt;
}
