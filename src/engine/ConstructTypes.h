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
#include "index/LocalVocab.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/OverloadCallOperator.h"

namespace qlever::constructExport {

// Canonical representation of a resolved RDF term stored in the LRU cache.
//
// Two fundamentally different representations are used, distinguished by
// whether `rdfTermDataType()` is null:
// 1) `rdfTermDataType()` != nullptr: `rdfTermString()` represents an encoded
// literal (directly encoded into `ValueId`). `rdfTermString()` is the raw
// unquoted value (e.g. `42` for an xsd:int, `3.14` for an xsd:decimal). The
// type points to the compile-time XSD type string constant (e.g.
// XSD_INT_TYPE). Whether to emit the short form ("42") or the fully-qualified
// form ("\"42\"^^<xsd:integer>") is decided at formatting time by
// `formatTerm`.
// 2) `rdfTermDataType()` == nullptr: an IRI, a blank node, or a
// vocabulary-indexed literal. `rdfTermString()` already holds the complete,
// ready-to-emit serialized form (e.g. "<http://example.org/>", "\"hello\"@en").
// No further formatting is needed; the value is returned as-is for every
// format. This is the legacy format returned by `ExportIds::idToStringAndType`.
//
// The bytes of `rdfTermString()` are either owned by this object, or (for a
// word of the vocabulary, see `BorrowedVocabWord`) viewed in the batch-lookup
// result that the word was read from, which this object keeps alive. A
// borrowed word is always of the second kind.
class EvaluatedTermData {
 private:
  std::variant<std::string, BorrowedVocabWord> rdfTerm_;
  const char* rdfTermDataType_;  // non-null iff encoded literal (case 1 above)

 public:
  //____________________________________________________________________________
  EvaluatedTermData(std::string rdfTermString, const char* rdfTermDataType)
      : rdfTerm_{std::move(rdfTermString)}, rdfTermDataType_{rdfTermDataType} {}

  // A term that views the vocabulary word `word` instead of copying it.
  explicit EvaluatedTermData(BorrowedVocabWord word)
      : rdfTerm_{std::move(word)}, rdfTermDataType_{nullptr} {}

  // The string of the term, see above. The returned view is valid as long as
  // this object lives; it must not be kept beyond that.
  std::string_view rdfTermString() const {
    return std::visit(
        ad_utility::OverloadCallOperator{
            [](const std::string& owned) -> std::string_view { return owned; },
            [](const BorrowedVocabWord& borrowed) { return borrowed.word(); }},
        rdfTerm_);
  }

  // The XSD type of an encoded literal, `nullptr` otherwise, see above.
  const char* rdfTermDataType() const { return rdfTermDataType_; }

  // Return true iff the bytes of `rdfTermString()` are viewed in a batch-lookup
  // result instead of owned. Only used by tests.
  bool isBorrowed() const {
    return std::holds_alternative<BorrowedVocabWord>(rdfTerm_);
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
