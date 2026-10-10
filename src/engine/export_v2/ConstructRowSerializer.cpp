// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/export_v2/ConstructRowSerializer.h"

#include <absl/strings/str_cat.h>

#include <optional>
#include <string_view>
#include <variant>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/export_v2/ResolvedColumn.h"
#include "global/Id.h"
#include "index/ExportIds.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace ql::engine::export_v2 {
namespace {

using ad_utility::triple_component::LiteralOrIriView;
using qlever::constructExport::EvaluatedTermData;
using qlever::constructExport::formatTerm;

// True when Legacy's `RdfEscaping::validRDFLiteralFromNormalized` would change
// the literal `text` (or reject it). Mirrors its early return: a literal with
// exactly two quotes and no backslash or line break is already valid.
bool literalNeedsEscaping(std::string_view text) {
  const size_t posSecondQuote = text.find('"', 1);
  return posSecondQuote == std::string_view::npos ||
         posSecondQuote != text.rfind('"') ||
         text.find_first_of("\\\n\r") != std::string_view::npos;
}

// Append `text` in object position: like Legacy `formatTriple`, a literal
// (text starting with `"`) is escaped, everything else is written as is.
void appendObject(std::string& out, std::string_view text) {
  if (ql::starts_with(text, '"') && literalNeedsEscaping(text)) {
    out.append(RdfEscaping::validRDFLiteralFromNormalized(text));
  } else {
    out.append(text);
  }
}

// Resolve the terms of one variable column for a window of rows, with the
// text of Legacy `ConstructBatchEvaluator` + `formatTerm`. `VocabIndex` ids
// go through one batched vocabulary lookup and become views into the decoded
// batch. All other ids take Legacy's `idToStringAndType`; `Undefined` marks
// the cell unbound.
void resolveTermColumn(ResolvedColumn& column, const Index& index,
                       ql::span<const Id> ids, const LocalVocab& localVocab,
                       bool includeDataType) {
  std::vector<size_t> vocabRows;
  std::vector<size_t> vocabIndices;
  for (size_t row = 0; row < ids.size(); ++row) {
    const Id id = ids[row];
    if (id.getDatatype() == Datatype::VocabIndex) {
      vocabRows.push_back(row);
      vocabIndices.push_back(id.getVocabIndex().get());
      continue;
    }
    auto stringAndType =
        ql::exportIds::idToStringAndType(index, id, localVocab);
    if (!stringAndType.has_value()) {
      column.setUnbound(row);
      continue;
    }
    auto& [text, type] = stringAndType.value();
    if (type == nullptr) {
      column.setCopy(row, text);
    } else {
      column.setCopy(row, formatTerm(EvaluatedTermData{std::move(text), type},
                                     includeDataType));
    }
  }
  if (vocabRows.empty()) {
    return;
  }
  ad_utility::vocabulary::ArenaVocabBatchBuilder builder(
      vocabIndices.size(), index.getImpl().allocator());
  index.getImpl().getVocab().lookupBatch(vocabIndices, builder);
  auto words = std::move(builder).finalize();
  AD_CORRECTNESS_CHECK(words.size() == vocabRows.size());
  auto word = words.begin();
  for (size_t row : vocabRows) {
    auto literalOrIri = LiteralOrIriView::fromStringRepresentation(*word++);
    std::optional<std::string_view> blankNode;
    if (literalOrIri.isIri()) {
      blankNode = ql::exportIds::blankNodeIriToString(literalOrIri.getIri());
    }
    column.setView(row, blankNode.has_value()
                            ? blankNode.value()
                            : literalOrIri.toStringRepresentation());
  }
  column.keepAlive(std::move(words));
}

// Upper bound of the decimal digits of a blank-node row id plus the label
// separators, for the size estimate only.
constexpr size_t kBlankNodeRowIdBytes = 20;

}  // namespace

// _____________________________________________________________________________
bool ConstructRowSerializer::supportsMediaType(
    ad_utility::MediaType mediaType) noexcept {
  return mediaType == ad_utility::MediaType::turtle ||
         mediaType == ad_utility::MediaType::ntriples;
}

// _____________________________________________________________________________
ConstructRowSerializer::ConstructRowSerializer(
    const ad_utility::sparql_types::Triples& templateTriples,
    const VariableToColumnMap& variableColumns, const Index& index,
    ad_utility::MediaType mediaType, uint64_t rowOffset)
    : includeDataType_{mediaType == ad_utility::MediaType::ntriples},
      rowOffset_{rowOffset} {
  AD_CONTRACT_CHECK(supportsMediaType(mediaType));
  using namespace qlever::constructExport;
  // Legacy's preprocessing decides which template triples survive and the
  // text of every constant, so V2 reuses it unchanged.
  const auto preprocessed = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns, index);
  for (const auto& triple : preprocessed.preprocessedTriples_) {
    Triple& out = triples_.emplace_back();
    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      Term& term = out[pos];
      std::visit(
          [&](const auto& t) {
            using T = std::decay_t<decltype(t)>;
            if constexpr (std::is_same_v<T, PrecomputedConstant>) {
              term.kind_ = Term::Kind::Constant;
              std::string text =
                  formatTerm(*t.evaluatedTerm_, includeDataType_);
              if (pos == 2) {
                std::string object;
                appendObject(object, text);
                text = std::move(object);
              }
              term.text_ = std::move(text);
            } else if constexpr (std::is_same_v<T, PrecomputedVariable>) {
              term.kind_ = Term::Kind::Variable;
              auto it = ql::ranges::find(columns_, t.columnIndex_);
              term.slot_ = static_cast<size_t>(it - columns_.begin());
              if (it == columns_.end()) {
                columns_.push_back(t.columnIndex_);
                slotUses_.push_back(0);
              }
              ++slotUses_[term.slot_];
            } else {
              static_assert(std::is_same_v<T, PrecomputedBlankNode>);
              term.kind_ = Term::Kind::BlankNode;
              term.text_ = t.prefix_;
              term.suffix_ = t.suffix_;
            }
          },
          triple[pos]);
    }
  }
}

// _____________________________________________________________________________
void ConstructRowSerializer::appendRows(
    const IdTableView<0>& idTable, const LocalVocab& localVocab,
    const Index& index, uint64_t rowBegin, uint64_t rowEnd,
    uint64_t rowsExportedBeforeBlock,
    ScatterGatherChunkBuilder& builder) const {
  rowEnd = std::min(rowEnd, static_cast<uint64_t>(idTable.numRows()));
  if (rowBegin >= rowEnd || triples_.empty()) {
    return;
  }
  const size_t n = static_cast<size_t>(rowEnd - rowBegin);

  // `ResolvedColumn` is pinned, so the vector is sized once and each column
  // is constructed in place.
  std::vector<std::optional<ResolvedColumn>> resolved(columns_.size());
  for (size_t slot = 0; slot < columns_.size(); ++slot) {
    auto& column = resolved[slot].emplace(n);
    resolveTermColumn(column, index,
                      idTable.getColumn(columns_[slot]).subspan(rowBegin, n),
                      localVocab, includeDataType_);
    column.finish();
  }

  // Size estimate (exact unless literals need escaping or rows are skipped):
  // the variable texts as often as the template uses them, the constants and
  // blank nodes once per row, and ` `, ` `, ` .\n` per triple.
  size_t expectedBytes = 5 * n * triples_.size();
  for (size_t slot = 0; slot < columns_.size(); ++slot) {
    expectedBytes += resolved[slot]->totalBytes() * slotUses_[slot];
  }
  for (const auto& triple : triples_) {
    for (const auto& term : triple) {
      if (term.kind_ == Term::Kind::Constant) {
        expectedBytes += term.text_.size() * n;
      } else if (term.kind_ == Term::Kind::BlankNode) {
        expectedBytes +=
            (term.text_.size() + term.suffix_.size() + kBlankNodeRowIdBytes) *
            n;
      }
    }
  }

  const uint64_t firstRowId = rowOffset_ + rowsExportedBeforeBlock + rowBegin;
  builder.appendCopiedWith(expectedBytes, [&](std::string& out) {
    auto isBound = [&](const Term& term, size_t row) {
      return term.kind_ != Term::Kind::Variable ||
             resolved[term.slot_]->isBound(row);
    };
    auto append = [&](const Term& term, size_t row, bool isObject) {
      switch (term.kind_) {
        case Term::Kind::Constant:
          out.append(term.text_);
          return;
        case Term::Kind::Variable: {
          const std::string_view text = (*resolved[term.slot_])[row];
          if (isObject) {
            appendObject(out, text);
          } else {
            out.append(text);
          }
          return;
        }
        case Term::Kind::BlankNode:
          absl::StrAppend(&out, term.text_, firstRowId + row, term.suffix_);
          return;
      }
      AD_FAIL();
    };
    for (size_t row = 0; row < n; ++row) {
      for (const auto& triple : triples_) {
        if (!isBound(triple[0], row) || !isBound(triple[1], row) ||
            !isBound(triple[2], row)) {
          continue;
        }
        append(triple[0], row, false);
        out.push_back(' ');
        append(triple[1], row, false);
        out.push_back(' ');
        append(triple[2], row, true);
        out.append(" .\n");
      }
    }
  });
}

}  // namespace ql::engine::export_v2
