// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H

#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "global/Id.h"
#include "global/ValueId.h"
#include "index/LocalVocab.h"
#include "util/Exception.h"

namespace qlever::constructExport {

// Canonical representation of a resolved RDF term stored in the LRU cache.
//
// Two fundamentally different representations are used, distinguished by
// whether `rdfTermDataType_` is null:
// 1) `rdfTermDataType_` != nullptr: `rdfTermString_` represents an encoded
// literal (directly encoded into `ValueId`). `rdfTermString_` is the raw
// unquoted value (e.g. `42` for an xsd:int, `3.14` for an xsd:decimal). `type`
// points to the compile-time XSD type string constant (e.g. XSD_INT_TYPE).
// Whether to emit the short form ("42") or the fully-qualified form
// ("\"42\"^^<xsd:integer>") is decided at formatting time by `formatTerm`.
// 2) `rdfTermDataType` == nullptr: an IRI, a blank node, or a
// vocabulary-indexed literal. `rdfTermString_` already holds the complete,
// ready-to-emit serialized form (e.g. "<http://example.org/>", "\"hello\"@en").
// No further formatting is needed; the value is returned as-is for every
// format. This is the legacy format returned by `ExportIds::idToStringAndType`.
struct EvaluatedTermData {
  // The owned representation used when formatting changes the term or it does
  // not originate in the shared vocabulary storage. Keeping this string in the
  // term preserves small-string optimization and avoids an extra allocation.
  std::string ownedRdfTermString_;
  // View onto the term's bytes. For an owned term it points into
  // `ownedRdfTermString_`; for a borrowed vocabulary term `owner_` keeps the
  // pointed-to batch lookup storage alive.
  std::string_view rdfTermString_;
  const char* rdfTermDataType_;  // non-null iff encoded literal (case 1 above)
  // Non-null only for a borrowed vocabulary term.
  std::shared_ptr<const void> owner_;

  // Owning constructor: materializes `rdfTermString` in this object. Used for
  // encoded values, local-vocabulary terms, and any path that rewrites bytes.
  EvaluatedTermData(std::string rdfTermString, const char* rdfTermDataType)
      : ownedRdfTermString_{std::move(rdfTermString)},
        rdfTermString_{ownedRdfTermString_},
        rdfTermDataType_{rdfTermDataType} {}

  EvaluatedTermData(const EvaluatedTermData& other)
      : ownedRdfTermString_{other.ownedRdfTermString_},
        rdfTermString_{other.owner_ ? other.rdfTermString_
                                    : std::string_view{ownedRdfTermString_}},
        rdfTermDataType_{other.rdfTermDataType_},
        owner_{other.owner_} {}

  EvaluatedTermData(EvaluatedTermData&& other) noexcept
      : ownedRdfTermString_{std::move(other.ownedRdfTermString_)},
        rdfTermString_{other.owner_ ? other.rdfTermString_
                                    : std::string_view{ownedRdfTermString_}},
        rdfTermDataType_{other.rdfTermDataType_},
        owner_{std::move(other.owner_)} {}

  EvaluatedTermData& operator=(const EvaluatedTermData& other) {
    ownedRdfTermString_ = other.ownedRdfTermString_;
    rdfTermDataType_ = other.rdfTermDataType_;
    owner_ = other.owner_;
    rdfTermString_ =
        owner_ ? other.rdfTermString_ : std::string_view{ownedRdfTermString_};
    return *this;
  }

  EvaluatedTermData& operator=(EvaluatedTermData&& other) noexcept {
    ownedRdfTermString_ = std::move(other.ownedRdfTermString_);
    rdfTermDataType_ = other.rdfTermDataType_;
    owner_ = std::move(other.owner_);
    rdfTermString_ =
        owner_ ? other.rdfTermString_ : std::string_view{ownedRdfTermString_};
    return *this;
  }

  // Borrowing constructor: `rdfTermString` must point into storage owned by
  // `owner`, which this term keeps alive.
  EvaluatedTermData(std::string_view rdfTermString, const char* rdfTermDataType,
                    std::shared_ptr<const void> owner)
      : rdfTermString_{rdfTermString},
        rdfTermDataType_{rdfTermDataType},
        owner_{std::move(owner)} {
    AD_CONTRACT_CHECK(owner_ != nullptr);
  }
};

// Shared ownership of `EvaluatedTermData`. The shared_ptr allows cheap copying
// when the same `Id` appears in multiple rows or is reused from the `IdCache`.
using EvaluatedTerm = std::shared_ptr<const EvaluatedTermData>;

// A constant (`Iri` or `Literal`) whose string value is fully known at
// preprocessing time. The `EvaluatedTerm` is built once at preprocessing and
// shared across all rows, avoiding per-row heap allocation.
struct PrecomputedConstant {
  EvaluatedTerm evaluatedTerm_;
  // The `ValueId` for this constant, used for the CONSTRUCT result
  // deduplication. It is set to the correct value by
  // `ConstructTemplatePreprocessor::resolveConstantDedupId`.
  std::optional<ValueId> dedupId_ = std::nullopt;
};

// A variable in a CONSTRUCT template. `columnIndex_` is the index of the
// column in the `IdTable` of the `Result` that holds this variable's values.
// It is used directly as the key into
// `BatchEvaluationResult::variablesByColumn_`. The set of distinct column
// indices used by the whole template is collected in
// `PreprocessedConstructTemplate::uniqueVariableColumns_`.
struct PrecomputedVariable {
  ColumnIndex columnIndex_;
};

// A blank node with precomputed prefix and suffix for fast evaluation. The
// blank node format is: prefix + rowNumber + suffix, where prefix is "_:g" or
// "_:u" (generated or user-defined) and suffix is "_" + label. This avoids
// recomputing these constant parts for every result table row.
struct PrecomputedBlankNode {
  std::string prefix_;  // "_:g" or "_:u"
  std::string suffix_;  // "_" + label
};

// A single preprocessed term position in a CONSTRUCT template triple. The
// variant type encodes what kind of term it is and holds all precomputed data
// needed for later evaluation.
using PreprocessedTerm = std::variant<PrecomputedConstant, PrecomputedVariable,
                                      PrecomputedBlankNode>;

// Number of positions in a triple: subject, predicate, object.
inline constexpr size_t NUM_TRIPLE_POSITIONS = 3;

// A single preprocessed CONSTRUCT template triple.
using PreprocessedTriple = std::array<PreprocessedTerm, NUM_TRIPLE_POSITIONS>;

// Result of instantiating a single template triple for a specific result table
// row.
struct EvaluatedTriple {
  EvaluatedTerm subject_;
  EvaluatedTerm predicate_;
  EvaluatedTerm object_;
};

// Result of preprocessing all CONSTRUCT template triples.
struct PreprocessedConstructTemplate {
  // The (non-ground) template triples, in template order.
  std::vector<PreprocessedTriple> preprocessedTriples_;
  // Deduplicated `IdTable` column indices of all variables that occur in the
  // template triples, in order of first encounter.
  std::vector<ColumnIndex> uniqueVariableColumns_;
  // `tripleContainsBlankNode_[i]` is true iff `preprocessedTriples_[i]`
  // contains a blank node constant.
  std::vector<bool> tripleContainsBlankNode_;
  // Owns and keeps alive the `LocalVocabEntry`s created while resolving literal
  // and IRI constants to their `PrecomputedConstant::dedupId_`.
  LocalVocab localVocabForConstants_;
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
