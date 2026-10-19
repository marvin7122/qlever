// Copyright 2016 - 2026, The QLever Authors, in particular:
//
// 2016 - 2026 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARYONDISK_H
#define QLEVER_SRC_INDEX_VOCABULARYONDISK_H

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "index/vocabulary/VocabularyBinarySearchMixin.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Algorithm.h"
#include "util/BufferPool.h"
#include "util/BufferPoolBudget.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/IoUringManager.h"
#include "util/Iterators.h"
#include "util/ResidentFileMapping.h"
#include "util/Serializer/Serializer.h"
#include "util/ThreadSafeQueue.h"

namespace ad_utility::vocabulary {

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

  // Read-only mappings of `file_` and `offsetsFile_` with the pages that this
  // process has read before (see `vocabulary-mmap-resident-reads`).
  ad_utility::ResidentFileMapping wordsMapping_;
  ad_utility::ResidentFileMapping offsetsMapping_;

  // Pool of persistent `BatchIoManager`s for `lookupBatch`.
  mutable std::unique_ptr<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchManagerBase>>>
      ioManagers_;

  // Descriptors of `file_` and `offsetsFile_` that are opened with `O_DIRECT`
  // when the runtime parameter `vocabulary-iouring-direct-io` is first used
  // (see `batchReadOptions`). A descriptor stays closed if the file system
  // does not support `O_DIRECT`; the reads from that file then use the page
  // cache.
  struct DirectIoFiles {
    std::string filename_;
    std::once_flag opened_;
    ad_utility::export_prototypes::DirectIoFile words_;
    ad_utility::export_prototypes::DirectIoFile offsets_;
  };
  mutable std::unique_ptr<DirectIoFiles> directIoFiles_;

  // Buffer pools for `file_` and `offsetsFile_` (see
  // `vocabulary-buffer-pool-size`), created on first use and shared by all
  // lookups. The total budget is split between the two files in proportion to
  // their sizes.
  struct BufferPools {
    std::string filename_;
    std::mutex mutex_;
    // The total budget the pools were last set to (0 = not created yet).
    std::atomic<size_t> budget_{0};
    // Set if creating the pools failed (e.g. no address space): the lookups
    // then take the other paths.
    std::atomic<bool> failed_{false};
    std::unique_ptr<ad_utility::BufferPool> words_;
    std::unique_ptr<ad_utility::BufferPool> offsets_;
    // With `vocabulary-buffer-pool-auto`: the registration with the elastic
    // budget controller. Declared after the pools, so it is destroyed (and
    // the controller stops using the pools) before them.
    std::atomic<bool> elastic_{false};
    std::unique_ptr<ad_utility::BufferPoolBudgetController::Registration>
        elasticRegistration_;
  };
  mutable std::unique_ptr<BufferPools> bufferPools_;

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

  // The words are stored under the base filename itself, the IDs and offsets
  // in an additional file (see `offsetSuffix_`).
  static FileSuffixes fileSuffixes() {
    return {"", std::string{offsetSuffix_}};
  }

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

  //____________________________________________________________________________
  VocabBatchLookupResult lookupBatch(ql::span<const size_t> indices) const;

  // Split-phase variant of `lookupBatch`: `beginLookup` submits the offset
  // reads and returns a handle immediately (without blocking), `finishLookup`
  // blocks until the lookup is complete and returns the resolved strings. This
  // lets a caller overlap the I/O of one batch with the CPU work of another
  // (see the depth-2 pipeline in the CONSTRUCT export path).
  std::unique_ptr<VocabLookupHandleBase> beginLookup(
      ql::span<const size_t> indices) const;

  // Complete a lookup started by `beginLookup`. `handle` must be the handle
  // returned by `beginLookup` on this vocabulary.
  VocabBatchLookupResult finishLookup(
      std::unique_ptr<VocabLookupHandleBase> handle) const;

  //____________________________________________________________________________
  VocabLookupOutput lookupBatchesStreamed(
      VocabLookupInput rangeOfIndexBatches) const;

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

    [[nodiscard]] uint64_t offset() const noexcept { return offset_; }
    // The word's size in bytes (`nextOffset_ - offset_`); the offsets must
    // be well-formed, which is checked.
    [[nodiscard]] size_t wordSize() const {
      AD_CORRECTNESS_CHECK(nextOffset_ >= offset_);
      return nextOffset_ - offset_;
    }
  };

  // The state of a split-phase lookup: owns the pooled I/O manager (removed
  // from the pool by `beginLookup`) and the submitted offset-read batch until
  // `finish` completes the lookup and returns the manager to the pool.
  class LookupHandle : public VocabLookupHandleBase {
   public:
    // Requires `vocab_`, `manager_`, `indices_`, `offsetBatch_` and
    // `offsetPairs_` to be set by `VocabularyOnDisk::beginLookup`.
    VocabBatchLookupResult finish() override;

    // For a pipelined lookup through the buffer pools (see
    // `LookupPipelineScope`): wait for the offset pages and start loading the
    // word pages. A no-op otherwise, and when called again.
    void advance() override;

    // Drain in-flight offset reads, then return the `manager_` if `finish`
    // was never called. The reads target `offsetPairs_`, which dies here.
    ~LookupHandle() override;

   private:
    // Only `VocabularyOnDisk::beginLookup` sets up the state below. The handle
    // is only reachable through `VocabLookupHandleBase`, so no other code can
    // mutate the targets of the in-flight reads.
    friend class VocabularyOnDisk;

    // The vocabulary that created this handle. It must outlive the handle.
    const VocabularyOnDisk* vocab_ = nullptr;
    std::unique_ptr<ad_utility::BatchManagerBase> manager_;
    // The manager that the reads of this lookup go through: `manager_`, or
    // for a pipelined lookup the thread's manager of the
    // `LookupPipelineScope` (then `manager_` stays empty).
    ad_utility::BatchManagerBase* io_ = nullptr;
    // The requested indices, owned so the offset reads can be completed after
    // the caller's span has gone out of scope.
    std::vector<size_t> indices_;
    // The batched offset read submitted by `beginLookup`. Empty until the
    // batch was actually submitted, so a handle whose `beginLookup` threw
    // before the submission never waits on a batch it does not own.
    std::optional<ad_utility::BatchManagerBase::BatchHandle> offsetBatch_;
    // The target buffers of the submitted offset read.
    std::vector<OffsetPair> offsetPairs_;
    // Whether this lookup uses the page-cache fast path (see
    // `vocabulary-iouring-page-cache-fast-path`). Fixed by `beginLookup`, so
    // both phases of one lookup take the same path.
    bool pageCacheFastPath_ = false;
    // Whether this lookup copies known-resident reads from the memory
    // mappings (see `vocabulary-mmap-resident-reads`). Fixed by `beginLookup`
    // together with `pageCacheFastPath_`, so both phases agree.
    bool residentReads_ = false;
    // The positions of the phase-1 reads that missed the resident/page-cache
    // fast path and were submitted to the manager; `finish` marks them
    // resident once the batch has completed.
    std::vector<size_t> missedPositions_;
    // The buffer pools of this lookup (see `vocabulary-buffer-pool-size`), or
    // null. With the pools, both phases run in `finish` and read through the
    // pools; the mappings and the page-cache fast path are not used.
    VocabularyOnDisk::BufferPools* bufferPools_ = nullptr;

    // Hand the `manager_` back to the pool. Used by `finish` and the
    // destructor; the handle owns the manager until one of them runs.
    void returnManagerToPool();

    // The state of a pipelined lookup through the buffer pools: phase 1 (the
    // offset pairs) is started by `beginLookup`, phase 2 (the words) by
    // `advance`, and `finish` completes it. The spans of each phase's reads
    // must stay valid while it is pending, so they are members. The pending
    // reads are declared last, so they are completed (in their destructors)
    // before the buffers they write into are destroyed.
    bool pipelinedPools_ = false;
    bool advanced_ = false;
    std::vector<size_t> offsetSizes_;
    std::vector<uint64_t> offsetFileOffsets_;
    std::vector<char*> offsetTargets_;
    std::vector<size_t> wordSizes_;
    std::vector<uint64_t> wordFileOffsets_;
    std::optional<ContiguousVocabBatchBuilder> words_;
    std::vector<char*> wordTargets_;
    ad_utility::BufferPool::PendingRead offsetsRead_;
    ad_utility::BufferPool::PendingRead wordsRead_;
  };

  // Phase 2 of `lookupBatch`: given the `offsetPairs` from phase 1, read the
  // string data from `file_` into one contiguous buffer in a single batched
  // read via `manager`, and return it as a `VocabBatchLookupResult`.
  // `offsetPairs` must be non-empty (guaranteed by `lookupBatch`, which
  // rejects empty input; the `ContiguousVocabBatchBuilder` requires it). With
  // `pageCacheFastPath`, the words that are in the page cache are read with
  // `readPageCacheHits` (adjacent words in one call), and only the others go
  // through `manager`.
  // With `residentReads`, words whose pages are known resident are copied
  // from `wordsMapping_` first.
  // With `wordsPool` (not null), all words are read through that buffer pool
  // (misses through `manager`) instead.
  VocabBatchLookupResult readStrings(
      ad_utility::BatchManagerBase& manager,
      ql::span<const OffsetPair> offsetPairs, bool pageCacheFastPath,
      bool residentReads, ad_utility::BufferPool* wordsPool = nullptr) const;

  // The buffer pools (created on first use), or null if they are off or
  // cannot be created. With `elastic` false, the pools have the fixed total
  // budget `totalBudget` (off if zero; budgets updated when it changes). With
  // `elastic` true, the pools are registered with the process-wide
  // `BufferPoolBudgetController`, which sets their budgets; `totalBudget`, if
  // positive, is its upper bound, and `reserve` the free memory it leaves.
  BufferPools* bufferPools(size_t totalBudget, bool elastic = false,
                           size_t reserve = 0) const;

  // A `BufferPool::Loader` that reads the missing pages through `manager`.
  static ad_utility::BufferPool::Loader loaderFor(
      ad_utility::BatchManagerBase& manager);

  // A `BufferPool::AsyncLoader` that submits the missing pages to `manager`
  // as one batch and waits for that batch.
  static ad_utility::BufferPool::AsyncLoader asyncLoaderFor(
      ad_utility::BatchManagerBase& manager);

  // Serve the reads whose pages are known resident in `mapping` (if not null)
  // from the mapping, then the others with `readPageCacheHits`; mark the pages
  // of the reads served by `readPageCacheHits` resident. Return the indices
  // (ascending) of the reads that were served by neither.
  static std::vector<size_t> readResidentOrPageCacheHits(
      const ad_utility::ResidentFileMapping* mapping, int fd,
      ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
      ql::span<char*> buffers);

  // Submit the reads of `numBytes[i]` bytes at `offsets[i]` of `fd` into
  // `buffers[i]` for every `i` in `positions` to `manager` as one batch,
  // without waiting. Return `std::nullopt` (and submit nothing) if
  // `positions` is empty.
  static std::optional<ad_utility::BatchManagerBase::BatchHandle>
  submitThroughManager(ad_utility::BatchManagerBase& manager, int fd,
                       ql::span<const size_t> numBytes,
                       ql::span<const uint64_t> offsets,
                       ql::span<char*> buffers,
                       ql::span<const size_t> positions,
                       const ad_utility::BatchReadOptions& options);

  // The `BatchReadOptions` for the batched reads from `offsetsFile_` (if
  // `forOffsetsFile` is set) or `file_`, as selected by the runtime parameters
  // `vocabulary-iouring-registered-buffers` and `vocabulary-iouring-direct-io`.
  ad_utility::BatchReadOptions batchReadOptions(bool forOffsetsFile) const;

  // Read `numBytes[i]` bytes at `offsets[i]` of `fd` into `buffers[i]` for
  // every `i` in `positions` through `manager` and wait for them.
  static void readThroughManager(ad_utility::BatchManagerBase& manager, int fd,
                                 ql::span<const size_t> numBytes,
                                 ql::span<const uint64_t> offsets,
                                 ql::span<char*> buffers,
                                 ql::span<const size_t> positions,
                                 const ad_utility::BatchReadOptions& options);
};

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARYONDISK_H
