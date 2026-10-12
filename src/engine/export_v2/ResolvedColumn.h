// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"

namespace ql::engine::export_v2 {

// The text of one output column for a window of rows. A cell views either a
// decoded vocabulary word (kept alive by `batch_`, no copy) or `scratch_`
// (escaped words, values encoded in the `Id`, local-vocab words). Cells that
// are never set are empty views. SELECT CSV/TSV serializes unbound cells as an
// empty field, exactly like an empty string, so it never marks them. CONSTRUCT
// drops a triple with an unbound term, so it marks those cells with
// `setUnbound` to tell them apart from empty strings.
class ResolvedColumn {
 private:
  std::vector<std::string_view> cells_;
  std::string scratch_;
  // Cells whose text lives in `scratch_`: (row, offset, size). `scratch_` may
  // reallocate while the column is resolved, so these become views only in
  // `finish`.
  struct ScratchCell {
    size_t row_;
    size_t offset_;
    size_t size_;
  };
  std::vector<ScratchCell> scratchCells_;
  // Empty unless some cell was marked unbound (the common case).
  std::vector<bool> unbound_;
  ad_utility::vocabulary::VocabBatchLookupResult batch_;
  size_t totalBytes_ = 0;

 public:
  explicit ResolvedColumn(size_t numRows) : cells_(numRows) {}
  // Pinned: after `finish`, cells view `scratch_`, whose bytes may live inside
  // the object (small-string buffer), so a moved or copied column would
  // dangle.
  ResolvedColumn(const ResolvedColumn&) = delete;
  ResolvedColumn& operator=(const ResolvedColumn&) = delete;
  ResolvedColumn(ResolvedColumn&&) = delete;
  ResolvedColumn& operator=(ResolvedColumn&&) = delete;

  // Point `row` at `text`, which must outlive this column (vocabulary batch).
  void setView(size_t row, std::string_view text) {
    cells_[row] = text;
    totalBytes_ += text.size();
  }

  // Copy `text` into the column's scratch buffer.
  void setCopy(size_t row, std::string_view text) {
    if (text.empty()) {
      return;
    }
    scratchCells_.push_back({row, scratch_.size(), text.size()});
    scratch_.append(text);
    totalBytes_ += text.size();
  }

  // Mark `row` as unbound (UNDEF). Its text stays empty.
  void setUnbound(size_t row) {
    if (unbound_.empty()) {
      unbound_.resize(cells_.size(), false);
    }
    unbound_[row] = true;
  }

  void keepAlive(ad_utility::vocabulary::VocabBatchLookupResult batch) {
    batch_ = std::move(batch);
  }

  // Turn the scratch cells into views. Call once, after the last `set*`.
  void finish() {
    for (const auto& cell : scratchCells_) {
      cells_[cell.row_] =
          std::string_view{scratch_.data() + cell.offset_, cell.size_};
    }
  }

  [[nodiscard]] std::string_view operator[](size_t row) const {
    return cells_[row];
  }
  [[nodiscard]] bool isBound(size_t row) const {
    return unbound_.empty() || !unbound_[row];
  }
  [[nodiscard]] size_t size() const { return cells_.size(); }
  [[nodiscard]] size_t totalBytes() const { return totalBytes_; }
};

}  // namespace ql::engine::export_v2
