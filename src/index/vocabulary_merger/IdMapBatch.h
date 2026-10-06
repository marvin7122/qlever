// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "global/Id.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/LruCache.h"
#include "util/Serializer/BufferedSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

// The third stage of the merging pipeline of the vocabulary merger (see the
// comment above `mergeVocabulary` in `index/VocabularyMerger.h`), which writes
// the entries of the partial ID maps. This is not part of the public interface
// of that header.
namespace ad_utility::vocabulary_merger::detail {

// The index mappings for a complete merged batch of words. `globalIds_` stores
// the global IDs for the words in this batch,
// `LocalIdxToBatchMapping::indexOfWordInBatch_` is an index into `globalIds_`.
struct IdMapBatch {
  LocalIdxToBatchMappings localIdxMappings_;
  std::vector<Id> globalIds_;
};
}  // namespace ad_utility::vocabulary_merger::detail

namespace ad_utility::serialization {

// A seekable writer for an ID map that shares a bounded cache of open files
// with the other maps in a batch writer. Its logical position survives cache
// eviction; reopening a file never truncates previously written entries.
// It lives in the serialization namespace for argument-dependent lookup.
class IdMapBatchFileWriteSerializer {
 public:
  using SerializerType = ad_utility::serialization::WriteSerializerTag;
  using FileHandleCache =
      ad_utility::util::LRUCache<std::string,
                                 std::unique_ptr<ad_utility::File>>;

 private:
  std::string filename_;
  uint64_t position_ = 0;
  std::shared_ptr<FileHandleCache> fileHandles_;

 public:
  IdMapBatchFileWriteSerializer(std::string filename,
                                std::shared_ptr<FileHandleCache> fileHandles)
      : filename_{std::move(filename)}, fileHandles_{std::move(fileHandles)} {
    // Create/truncate each file exactly once, without retaining its descriptor.
    ad_utility::File file{filename_, "w"};
  }

  void serializeBytes(const char* bytes, size_t numBytes) {
    const auto& file =
        fileHandles_->getOrCompute(filename_, [](const std::string& filename) {
          return std::make_unique<ad_utility::File>(filename, "r+");
        });
    AD_CONTRACT_CHECK(file->seek(static_cast<off_t>(position_), SEEK_SET),
                      "Seeking in ID map file ", filename_, " failed");
    AD_CONTRACT_CHECK(file->write(bytes, numBytes) == numBytes,
                      "Short write to ID map file ", filename_);
    position_ += numBytes;
  }

  [[nodiscard]] uint64_t getSerializationPosition() const { return position_; }
  void setSerializationPosition(uint64_t position) { position_ = position; }
};
}  // namespace ad_utility::serialization

namespace ad_utility::vocabulary_merger::detail {
using ad_utility::serialization::IdMapBatchFileWriteSerializer;

// The third stage of the merging pipeline: write the entries of a complete
// `IdMapBatch` to the partial ID maps, one of which is created per partial
// vocabulary.
//
// NOTE: This class is used exclusively by the single thread of the
// `idMapWriterQueue_` of the `VocabularyMergePipeline`. Therefore it does not
// need to be threadsafe.
class IdMapBatchWriter {
 private:
  using WriteSerializer = ad_utility::serialization::BufferedWriteSerializer<
      IdMapBatchFileWriteSerializer>;
  using Writer =
      ad_utility::serialization::VectorIncrementalSerializer<IdMapEntry,
                                                             WriteSerializer>;
  // The buffered ID map writers, one per partial vocabulary. They share at
  // most 64 open file handles, independent of the number of vocabularies.
  std::vector<Writer> idMapWriters_;

 public:
  // Create the ID map for each of the partial vocabularies, in the files
  // `idMapFilenames`, one per partial vocabulary and in the order of the
  // partial vocabularies. The filenames are taken as a type-erased range, such
  // that this class is oblivious of how they are derived (see
  // `index/PartialVocabularyFilenames.h`).
  // NOTE: That the number of partial vocabularies fits into the `uint32_t` of
  // a `LocalIdxToBatchMapping` is checked by `mergeVocabulary` (see
  // `index/VocabularyMergerImpl.h`).
  explicit IdMapBatchWriter(
      ad_utility::InputRangeTypeErased<std::string> idMapFilenames) {
    // The cache is retained only by the per-file serializers, so destroying
    // all of them closes and flushes the cached files.
    auto fileHandles =
        std::make_shared<IdMapBatchFileWriteSerializer::FileHandleCache>(64);
    for (const std::string& filename : idMapFilenames) {
      idMapWriters_.emplace_back(
          WriteSerializer{IdMapBatchFileWriteSerializer{filename, fileHandles},
                          idMapWriterBufferSize});
    }
  }

  // Write all the mappings of the `batch` to their respective ID maps.
  //
  // TODO<optimization> The `mappings_` are in merge order, so the loop below
  // round-robins over the `idMapWriters_` instead of grouping the entries by
  // the file they belong to. Stably partitioning (or sorting) the mappings by
  // their `partialVocabularyIndex_` before the loop would give each
  // writer one contiguous run per batch and thus a more useful external
  // access pattern. This is local to this stage and doesn't affect any of the
  // other stages of the pipeline.
  void writeBatch(const IdMapBatch& batch) {
    AD_LOG_TRACE << "Start writing a batch of ID map entries\n";
    const auto& globalIds = batch.globalIds_;
    const auto& localIdxMappings = batch.localIdxMappings_;
    // NOTE: We deliberately use a manual loop and not a range-based one,
    // because only the first `numMappings_` elements of the `mappings_` are
    // initialized (see `LocalIdxToBatchMappings` in
    // `index/vocabulary_merger/WordBatch.h`).
    for (size_t i = 0; i < localIdxMappings.numMappings_; ++i) {
      const auto& mapping = localIdxMappings.mappings_[i];
      idMapWriters_[mapping.partialVocabularyIndex_].push(
          IdMapEntry{mapping.indexOfWordInPartialVocabulary_,
                     globalIds[mapping.indexOfWordInBatch_]});
    }
  }

  // Flush and close all the ID maps. After this, no more batches may be
  // written. NOTE: This is also done implicitly by the destructor, because the
  // destructor of each incremental writer calls its `finish()`.
  void finish() {
    for (auto& idMapWriter : idMapWriters_) {
      idMapWriter.finish();
    }
    // finish() patches the headers but retains the underlying serializers.
    // Release them (and their shared cache) to close/flush all files now.
    idMapWriters_.clear();
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H
