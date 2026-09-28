// Copyright 2016 - 2026, The QLever Authors, in particular:
//
// 2016 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARYONDISK_H
#define QLEVER_SRC_INDEX_VOCABULARYONDISK_H

#include <gtest/gtest_prod.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "index/vocabulary/VocabularyBinarySearchMixin.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Algorithm.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/IoUringManager.h"
#include "util/Iterators.h"
#include "util/Serializer/Serializer.h"
#include "util/ThreadSafeQueue.h"

// On-disk vocabulary of strings. Each entry is a pair of <ID, String>. The IDs
// are ascending, but not (necessarily) contiguous. If the strings are sorted,
// then binary search for a string can be performed.
class VocabularyOnDisk : public VocabularyBinarySearchMixin<VocabularyOnDisk> {
 private:
  // The offset of a word in the underlying file.
  using Offset = uint64_t;
  // The file in which the words are stored.
  ad_utility::File file_;

  // The file in which the offsets of the words are stored. It contains one
  // `Offset` per word, plus a final offset that marks the end of the last
  // word, followed by an `MmapVectorMetaData` trailer at the end of the file
  // that records the number of offsets. The number of words is therefore the
  // number of stored offsets minus one.
  ad_utility::File offsetsFile_;

  // The number of words stored in the vocabulary.
  size_t size_ = 0;

  // Pool of persistent `BatchIoManager`s for `lookupBatch`.
  mutable std::unique_ptr<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchManagerBase>>>
      ioManagers_;

  // This suffix is appended to the filename of the main file, in order to get
  // the name for the file in which IDs and offsets are stored.
  static constexpr std::string_view offsetSuffix_ = ".offsets";

 public:
  // A helper class that is used to build a vocabulary word by word.
  // Each call to `operator()` adds the next word to the vocabulary.
  // At the end, the `finish()` method can be called. Note that `finish`
  // is also implicitly called by the destructor, but doing so implicitly
  // releases resources earlier and is cleaner in case of exceptions.
  class WordWriter : public WordWriterBase {
   private:
    ad_utility::File file_;
    ad_utility::File offsetsFile_;
    uint64_t currentOffset_ = 0;
    uint64_t numWords_ = 0;

   public:
    // Constructor, used by `VocabularyOnDisk::wordWriter`.
    explicit WordWriter(const std::string& filename);
    // Add the next word to the vocabulary and return its index.
    uint64_t operator()(std::string_view word, bool isExternalDummy) override;

    ~WordWriter() override;

   private:
    // Finish the writing. After this no more calls to `operator()` are allowed.
    void finishImpl() override;
  };

  // Open the vocabulary from file. It must have been previously written to
  // this file via a `WordWriter`.
  void open(const std::string& filename);

  // Return the word that is stored at the index. Throw an exception if `idx >=
  // size`.
  std::string operator[](uint64_t idx) const;

  // Efficient iteration over all words in the vocabulary, in order, yielded as
  // `IndexAndWord`s (the word as a `string_view` together with its index).
  // Internally the words are read in batches, each produced by two large
  // sequential reads (offsets and word data). This is much faster than looking
  // up the words one at a time via `operator[]`, which performs two small
  // `pread`s and allocates a string per word. A batch is bounded both in the
  // number of words and in the number of bytes of word data it holds (but
  // always contains at least one word, even if that word alone exceeds the byte
  // limit).
  VocabularyScanRange scanAll() const;

  // Look up the words at `indices` (in this order) with two batched reads
  // through a pooled `io_uring` manager: first the offsets of the words, then
  // the words. If the runtime parameter `vocabulary-iouring-pipeline-depth` is
  // at least `2`, the two phases overlap (see `lookupBatchPipelined`).
  VocabBatchLookupResult lookupBatch(ql::span<const size_t> indices) const;

  //____________________________________________________________________________
  VocabLookupOutput lookupBatchesStreamed(
      VocabLookupInput rangeOfIndexBatches) const;

  // Pipelined variant of `lookupBatchesStreamed`: up to `pipelineDepth`
  // batches may have offset reads in flight at once, so batch N+1's reads
  // issue while batch N is consumed. A `pipelineDepth` below `2` is equivalent
  // to the sequential `lookupBatchesStreamed` above. The parameterless overload
  // reads the `vocabulary-iouring-pipeline-depth` runtime parameter (default
  // `1`, hence sequential unless explicitly raised).
  VocabLookupOutput lookupBatchesStreamed(VocabLookupInput rangeOfIndexBatches,
                                          size_t pipelineDepth) const;

  // Get the number of words in the vocabulary.
  size_t size() const { return size_; }

  // Default constructor for an empty vocabulary.
  VocabularyOnDisk() = default;

  // `VocabularyOnDisk` is movable, but not copyable.
  VocabularyOnDisk(VocabularyOnDisk&&) noexcept = default;
  VocabularyOnDisk& operator=(VocabularyOnDisk&&) noexcept = default;

  // The offset of a word in `file_` and its size in number of bytes.
  struct OffsetAndSize {
    uint64_t offset_;
    uint64_t size_;
  };

  // The `Accessor` for the `IteratorForAccessOperator` class below.
  struct Accessor {
    template <typename Voc>
    constexpr auto operator()(const Voc& vocabulary, uint64_t index) const {
      return vocabulary[index];
    }
  };
  // Const random access iterators, implemented via the
  // `IteratorForAccessOperator` template.
  using const_iterator =
      ad_utility::IteratorForAccessOperator<VocabularyOnDisk, Accessor>;
  const_iterator begin() const { return {this, 0}; }
  const_iterator end() const { return {this, size()}; }

  // Convert an iterator to the corresponding `WordAndIndex`. Needed for the
  // Mixin base class
  WordAndIndex iteratorToWordAndIndex(const_iterator it) const {
    if (it == end()) {
      return WordAndIndex::end();
    } else {
      return {*it, static_cast<uint64_t>(it - begin())};
    }
  }

  // Generic serialization support.
  AD_SERIALIZE_FRIEND_FUNCTION(VocabularyOnDisk) {
    (void)serializer;
    (void)arg;
    throw std::runtime_error(
        "Generic serialization is not implemented for VocabularyOnDisk.");
  }

 private:
  // Get the `OffsetAndSize` for the element with the `idx`. Return
  // `std::nullopt` if `idx` is not contained in the vocabulary.
  OffsetAndSize getOffsetAndSize(uint64_t idx) const;

  // Helper for `scanAll`: return a lazy input range that reads the word offsets
  // from the `.offsets` file in batches of at most
  // `VOCABULARY_SCAN_MAX_WORDS_PER_BATCH` words. Each element is a span over
  // the offsets of one batch, with one trailing entry marking the end of the
  // last word.
  auto readOffsetsInBatches() const;

  // Helper for `scanAll`: given the `offsets` of a single chunk of words (a
  // span over the chunk's offsets, with one trailing entry marking the end of
  // the last word), return a lazy input range that yields each word of the
  // chunk as a `string_view`, reading the word data from disk in sub-batches of
  // at most `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` bytes. The return type is
  // deduced, so this function can only be used within `VocabularyOnDisk.cpp`.
  auto chunkToWords(ql::span<const uint64_t> offsets) const;

  // A word's start offset and the start offset of the following word (which
  // marks the end of the word), stored contiguously in the `.offsets` file.
  struct OffsetPair {
    uint64_t offset_;
    uint64_t nextOffset_;
  };

  // Phase 1 of `lookupBatch`: for each requested index, read its `OffsetPair`
  // (16 bytes) from the `.offsets` file in a single batched read via `manager`.
  // With `pageCacheFastPath`, each run of consecutive indices is first read as
  // one range of the `.offsets` file with `readPageCacheHits`, and only the
  // pairs of the runs that were not in the page cache go through `manager`.
  std::vector<OffsetPair> readOffsetPairs(ad_utility::BatchManagerBase& manager,
                                          ql::span<const size_t> indices,
                                          bool pageCacheFastPath) const;

  // Submitted-but-not-yet-completed phase-1 reads for one batch: owns the
  // target buffers of the submitted reads, so the caller's span of indices may
  // go out of scope while the reads are in flight. `handle_` is empty if no
  // read had to go through the manager (all pairs came from the page cache).
  struct PendingOffsetRead {
    std::vector<OffsetPair> offsetPairs_;
    std::optional<ad_utility::BatchManagerBase::BatchHandle> handle_;
  };

  // Submit the phase-1 reads for `indices` without waiting for them. With
  // `pageCacheFastPath`, the pairs that are in the page cache are read
  // synchronously as in `readOffsetPairs`, and only the others are submitted.
  PendingOffsetRead submitOffsetPairs(ad_utility::BatchManagerBase& manager,
                                      ql::span<const size_t> indices,
                                      bool pageCacheFastPath) const;

  // Block until the reads in `pending` complete and move their `OffsetPair`s
  // out of `pending`.
  static std::vector<OffsetPair> waitOffsetPairs(
      ad_utility::BatchManagerBase& manager, PendingOffsetRead& pending);

  // Phase 2 of `lookupBatch`: given the `offsetPairs` from phase 1, read the
  // string data from `file_` into one contiguous buffer in a single batched
  // read via `manager`, and return it as a `VocabBatchLookupResult`. With
  // `pageCacheFastPath`, the words that are in the page cache are read with
  // `readPageCacheHits` (adjacent words in one call), and only the others go
  // through `manager`. `offsetPairs` must be non-empty (guaranteed by
  // `lookupBatch`, which rejects empty input; the `ContiguousVocabBatchBuilder`
  // requires it).
  VocabBatchLookupResult readStrings(ad_utility::BatchManagerBase& manager,
                                     ql::span<const OffsetPair> offsetPairs,
                                     bool pageCacheFastPath) const;

  // Submit reads of `numBytes[i]` bytes at `offsets[i]` of `fd` into
  // `buffers[i]` for every `i` in `positions` through `manager`, without
  // waiting for them. Return the handle of the submitted batch, or
  // `std::nullopt` if `positions` is empty.
  static std::optional<ad_utility::BatchManagerBase::BatchHandle>
  submitThroughManager(ad_utility::BatchManagerBase& manager, int fd,
                       ql::span<const size_t> numBytes,
                       ql::span<const uint64_t> offsets,
                       ql::span<char*> buffers,
                       ql::span<const size_t> positions);

  // Whether the lookups use the page-cache fast path: the runtime parameter
  // `vocabulary-iouring-page-cache-fast-path` is set and the kernel supports
  // `preadv2` with `RWF_NOWAIT`.
  static bool pageCacheFastPathIsEnabled();

  // Submitted-but-not-yet-completed phase-2 reads for one batch: `builder_`
  // owns the buffer that the reads target and the view of every word.
  // `handle_` is empty if no read had to go through the manager (all words
  // came from the page cache). Call `finalize` on `builder_` only after the
  // reads of `handle_` have completed.
  struct PendingStringRead {
    ContiguousVocabBatchBuilder builder_;
    std::optional<ad_utility::BatchManagerBase::BatchHandle> handle_;
  };

  // Submit the phase-2 reads for the non-empty `offsetPairs` without waiting
  // for them: the words are packed contiguously into the buffer of the
  // returned builder. With `pageCacheFastPath`, the words that are in the page
  // cache are read synchronously as in `readStrings`, and only the others are
  // submitted.
  PendingStringRead submitStrings(ad_utility::BatchManagerBase& manager,
                                  ql::span<const OffsetPair> offsetPairs,
                                  bool pageCacheFastPath) const;

  // The number of indices per sub-batch in `lookupBatchPipelined`. Half of the
  // default ring size of `ad_utility::BatchManager` (256), so that at the
  // pipeline depth `2` the offset reads in flight fill one ring.
  static constexpr size_t PIPELINE_SUB_BATCH_SIZE = 128;

  // `lookupBatch` for a pipeline depth of at least `2`: split `indices` into
  // sub-batches of `subBatchSize` indices and keep the offset reads of up to
  // `pipelineDepth` sub-batches in flight on `manager`. The word reads of a
  // sub-batch are submitted as soon as its offsets are known and are only
  // waited for at the end, so they overlap with the offset reads of the
  // following sub-batches instead of starting after all offsets are read.
  VocabBatchLookupResult lookupBatchPipelined(
      ad_utility::BatchManagerBase& manager, ql::span<const size_t> indices,
      size_t pipelineDepth, size_t subBatchSize, bool pageCacheFastPath) const;

  FRIEND_TEST(VocabularyOnDisk, LookupBatchPipelinedBoundsOffsetReadsInFlight);
  FRIEND_TEST(VocabularyOnDisk, LookupBatchPipelinedDrainsReadsOnException);
};

#endif  // QLEVER_SRC_INDEX_VOCABULARYONDISK_H
