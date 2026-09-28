// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "engine/QueryExecutionTree.h"
#include "util/http/MediaTypes.h"

namespace qlever::constructExport {

using StringTriple = QueryExecutionTree::StringTriple;

class ConstructDeduplicator;

// Instantiates a single preprocessed term for a specific row.
// For constants: returns the precomputed string.
// For variables: looks up the batch-evaluated value.
// For blank nodes: computes the value on the fly using precomputed
//   prefix/suffix and the blank node row id (rowOffset + actualRowIdx).
std::optional<EvaluatedTerm> instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowIdxInBatch, size_t rowIdxTotal);

// Bundles the state `instantiateBatch` needs to deduplicate triples as it
// instantiates them.
struct DeduplicationParams {
  std::reference_wrapper<ConstructDeduplicator> deduplicator_;
  std::reference_wrapper<const BatchEvaluationContext> ctx_;
};

// Instantiates all template triples for all rows in a batch. For each row,
// every triple in `tmpl.preprocessedTriples_` is instantiated; triples with
// any unbound term are silently dropped. `batchOffset` is the absolute
// row ID of the first row in the batch (used to generate unique blank node
// IDs). Triples are dropped if they are considered duplicates according to
// `deduplicationParams->deduplicator_`, see `ConstructDeduplicator.h` for
// details.
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t batchOffset,
    std::optional<DeduplicationParams> deduplicationParams = std::nullopt);

// Format a single term to its string form.
// `includeDataType=false`: integers, decimals
//   and booleans are emitted without quotes or datatype annotation.
// `includeDataType=true`: all typed literals carry an explicit
//   `"..."^^<type>` annotation.
// Terms with `type == nullptr` (IRIs, blank nodes, vocab-indexed literals)
// are returned as-is regardless of `includeDataType`.
std::string formatTerm(const EvaluatedTermData& term, bool includeDataType);

// Formats a triple (subject, predicate, object) according to the output
// format `format`.
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format);

// Per-stream cache of `formatTripleRle`. The `ConstructBatchEvaluator`'s
// `IdCache` hands out the same `EvaluatedTerm` (`shared_ptr`) for equal `Id`s,
// so consecutive rows with the same subject or predicate (a run in a result
// sorted by subject) carry the same pointer. The cache keeps the subject and
// predicate of the previous row, formatted and escaped for `format_`, together
// with owning handles: the cache lives across batches, while a batch only owns
// its terms until it is consumed, so raw pointers could dangle (and a freed
// address could be reused by a different term). Use one cache per output
// stream, single-threaded.
struct RleConstructTripleCache {
  std::optional<ad_utility::MediaType> format_;
  EvaluatedTerm lastSubject_ = nullptr;
  EvaluatedTerm lastPredicate_ = nullptr;
  std::string cachedSubject_;
  std::string cachedPredicate_;
};

// Same bytes as `formatTriple` (without `use-fast-export-stream-formatter`).
// A subject or predicate that is the same `EvaluatedTerm` as in the previous
// call is taken from `cache` instead of being formatted, escaped and copied
// again. Used by the CONSTRUCT export when `use-rle-prefix-construct-export`
// is set.
std::string formatTripleRle(const EvaluatedTriple& evaluatedTriple,
                            const ad_utility::MediaType& format,
                            RleConstructTripleCache& cache);

// Creates a `StringTriple` object. Needed for backwards compatibility with
// `ExportQueryExecutionTrees::constructQueryResultBindingsToQLeverJSON`
StringTriple createStringTriple(const EvaluatedTriple& evaluatedTriple,
                                bool includeDataType = false);

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
