//  Copyright 2026, University of Freiburg,
//  Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_DECOMPRESSEDBLOCKCACHE_H
#define QLEVER_SRC_INDEX_VOCABULARY_DECOMPRESSEDBLOCKCACHE_H

#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace ad_utility::vocabulary {

// A small bounded cache of recently decompressed words for the batch lookup
// path of `CompressedVocabulary` (see `CompressedVocabulary.h`).
//
// A "block" is one `NumWordsPerBlock` chunk of words that share a decoder.
// Scattered batch lookups often ask for the same words (and hence the same
// blocks) again across batches, so remembering recently decompressed words
// avoids repeating the (FSST) decompression for them. Only the words that are
// actually requested are stored, never whole blocks.
//
// Entries are grouped by block index and evicted one whole block at a time,
// least-recently-used block first, keeping at most `maxBlocks` blocks with at
// most `maxBytes` payload bytes in total. The size accounting is exact in the
// sense that it sums the sizes of all stored words; the (small, bounded by
// `maxBlocks`) overhead of keys and LRU bookkeeping is not counted. A single
// word larger than `maxBytes` is never stored, so that it cannot flush the
// whole cache.
//
// All member functions are thread-safe and `const`, so that the cache can be
// used from the `const` lookup path. Returned words are copies, so callers
// never observe eviction.
class DecompressedBlockCache {
 public:
  // The capacity bounds, see the class documentation.
  struct Config {
    size_t maxBlocks_ = 4;
    size_t maxBytes_ = 1ul << 20;
  };

  explicit DecompressedBlockCache(Config config = Config{})
      : config_{config} {}

  // Non-copyable and non-movable (it holds a mutex). `CompressedVocabulary`
  // shares the cache between copies via a `shared_ptr` instead.
  DecompressedBlockCache(const DecompressedBlockCache&) = delete;
  DecompressedBlockCache& operator=(const DecompressedBlockCache&) = delete;

  // Return a copy of the cached word for `blockIndex` and `offsetInBlock`, or
  // `std::nullopt` if it is not cached. A hit marks the block as most
  // recently used and increments `hits()`, a miss increments `misses()`.
  std::optional<std::string> lookup(size_t blockIndex,
                                    size_t offsetInBlock) const {
    std::lock_guard guard{mutex_};
    auto it = index_.find(blockIndex);
    if (it == index_.end()) {
      ++misses_;
      return std::nullopt;
    }
    const auto& words = it->second->second.words_;
    auto wordIt = words.find(offsetInBlock);
    if (wordIt == words.end()) {
      ++misses_;
      return std::nullopt;
    }
    // The block was used, move it to the front (most recently used).
    lru_.splice(lru_.begin(), lru_, it->second);
    ++hits_;
    return wordIt->second;
  }

  // Store `word` for `blockIndex` and `offsetInBlock`, evicting the least
  // recently used blocks while the bounds are exceeded. Storing for an
  // already cached offset overwrites the previous word. A word larger than
  // `maxBytes_` is silently not stored. A stored block becomes most recently
  // used.
  void store(size_t blockIndex, size_t offsetInBlock, std::string word) const {
    if (word.size() > config_.maxBytes_) {
      return;
    }
    std::lock_guard guard{mutex_};
    auto it = index_.find(blockIndex);
    if (it == index_.end()) {
      lru_.emplace_front(blockIndex, BlockEntry{});
      it = index_.emplace(blockIndex, lru_.begin()).first;
    } else {
      lru_.splice(lru_.begin(), lru_, it->second);
    }
    BlockEntry& entry = it->second->second;
    bytes_ += word.size();
    if (auto wordIt = entry.words_.find(offsetInBlock);
        wordIt != entry.words_.end()) {
      bytes_ -= wordIt->second.size();
      wordIt->second = std::move(word);
    } else {
      entry.words_.emplace(offsetInBlock, std::move(word));
    }
    evictWhileOverBudget();
  }

  // The number of blocks currently cached.
  size_t numBlocks() const {
    std::lock_guard guard{mutex_};
    return lru_.size();
  }

  // The exact number of cached payload bytes (the summed sizes of all stored
  // words, see the class documentation).
  size_t numBytes() const {
    std::lock_guard guard{mutex_};
    return bytes_;
  }

  // Drop all cached entries (and reset the byte accounting and the hit/miss
  // statistics). Called when the decoders change, so that no word
  // decompressed with a previous decoder can be returned afterwards.
  void clear() const {
    std::lock_guard guard{mutex_};
    lru_.clear();
    index_.clear();
    bytes_ = 0;
    hits_ = 0;
    misses_ = 0;
  }

  // The total number of `lookup` hits and misses since construction (or the
  // last `clear()`). Useful for tests and profiling; the cache behaves
  // identically with or without them.
  size_t hits() const {
    std::lock_guard guard{mutex_};
    return hits_;
  }
  size_t misses() const {
    std::lock_guard guard{mutex_};
    return misses_;
  }

 private:
  // The cached words of a single block, keyed by offset within the block,
  // together with their summed sizes.
  struct BlockEntry {
    std::unordered_map<size_t, std::string> words_;
  };

  // Evict the least recently used blocks (from the back) while either bound
  // is exceeded. The most recently used block is evicted last, so a block
  // that was just stored is only evicted again when it alone exceeds a bound
  // (which `store` already prevents for `maxBytes_`).
  void evictWhileOverBudget() {
    while (!lru_.empty() &&
           (lru_.size() > config_.maxBlocks_ || bytes_ > config_.maxBytes_)) {
      const auto& entry = lru_.back().second;
      for (const auto& [offset, word] : entry.words_) {
        (void)offset;
        bytes_ -= word.size();
      }
      index_.erase(lru_.back().first);
      lru_.pop_back();
    }
  }

  Config config_;
  mutable std::mutex mutex_;
  // The blocks from most recently used (front) to least recently used (back).
  mutable std::list<std::pair<size_t, BlockEntry>> lru_;
  // Index from block index into `lru_` for O(1) access.
  mutable std::unordered_map<
      size_t, std::list<std::pair<size_t, BlockEntry>>::iterator>
      index_;
  mutable size_t bytes_ = 0;
  mutable size_t hits_ = 0;
  mutable size_t misses_ = 0;
};

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_DECOMPRESSEDBLOCKCACHE_H
