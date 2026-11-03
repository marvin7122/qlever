// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_FSSTCOMPRESSOR_H
#define QLEVER_FSSTCOMPRESSOR_H

#include <absl/cleanup/cleanup.h>
#include <fsst.h>

#include <array>
#include <memory>
#include <string>
#include <vector>

#include "backports/span.h"
#include "util/Concepts.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/TypeTraits.h"

namespace detail {
// A helper function to cast `char*` to `unsigned char*` and `const char*` to
// `const unsigned char*` which is used below because FSST always works on
// unsigned character types. Note that this is one of the few cases where a
// `reinterpret_cast` is safe.
struct CastToUnsignedPtr {
  CPP_template(typename T)(
      requires ad_utility::SameAsAny<T, char*, const char*>)
  auto operator()(T ptr) const {
    using Res = std::conditional_t<ql::concepts::same_as<T, const char*>,
                                   const unsigned char*, unsigned char*>;
    return reinterpret_cast<Res>(ptr);
  }
};
constexpr CastToUnsignedPtr castToUnsignedPtr{};
}  // namespace detail

// Two buffers that a decoder reuses for the words of a batch (see
// `decompressWithScratch` below), so that it does not allocate and zero-fill a
// new string per word. They only grow.
using FsstDecompressionScratch = std::array<std::string, 2>;

// A simple C++ wrapper around the C-API of the `FSST` library. It consists of
// two types, a thredsafe `FsstDecoder` that can be used to perform
// decompression, and a single-threaded `FsstEncoder` for compression.
class FsstDecoder {
 private:
  fsst_decoder_t decoder_;

 public:
  // The default constructor does lead to an invalid decoder, but is required
  // for the serialization module. Don't use it.
  FsstDecoder() = default;

  // Construct from the internal `fsst_decoder_t`. Note that the typical way to
  // obtain an `FsstDecoder` is by first creating a `FsstEncoder` and calling
  // `getDecoder()` on that encoder.
  explicit FsstDecoder(const fsst_decoder_t& decoder) : decoder_{decoder} {
    // NOTE: `fsst_decoder` leaves the (unused) `version` field of the struct
    // uninitialized. It is set here, so that the serialized decoders are the
    // same for the same input, no matter on which thread they were created.
    // See https://github.com/cwida/fsst/issues/50, once that is fixed, this
    // workaround is no longer required.
    decoder_.version = 0;
  }

  // Decompress a  single string.
  std::string decompress(std::string_view str) const {
    std::string output;
    // FSST compresses at most by a factor of 8.
    output.resize(8 * str.size());
    output.resize(decompressInto(str, output));
    return output;
  }

  // Decompress `str` into `out`, which must have room for the decompressed
  // string (at most `8 * str.size()` bytes, see `decompress`), and return the
  // decompressed size.
  [[nodiscard]] size_t decompressInto(std::string_view str,
                                      ql::span<char> out) const {
    auto cast = detail::castToUnsignedPtr;
    size_t size = fsst_decompress(&decoder_, str.size(), cast(str.data()),
                                  out.size(), cast(out.data()));
    AD_CORRECTNESS_CHECK(size <= out.size());
    return size;
  }

  // Decompress `str` into `buffer`, which is grown (never shrunk) to the
  // maximal decompressed size, and return a view of the result in `buffer`.
  std::string_view decompressIntoBuffer(std::string_view str,
                                        std::string& buffer) const {
    if (buffer.size() < 8 * str.size()) {
      buffer.resize(8 * str.size());
    }
    return {buffer.data(), decompressInto(str, buffer)};
  }

  // Like `decompress`, but the result is a view into `scratch`, valid until
  // `scratch` is used again.
  std::string_view decompressWithScratch(
      std::string_view str, FsstDecompressionScratch& scratch) const {
    return decompressIntoBuffer(str, scratch[0]);
  }
  // Allow this type to be trivially serializable,
  CPP_template(typename T, typename U)(
      requires ql::concepts::same_as<T, FsstDecoder>)
  friend std::true_type allowTrivialSerialization(T, U&&) {
    return {};
  }
};

// A sequence of `N` `FsstDecoder` s that are chained in inverted order (the
// last one first) when decompressing a string. The inverted order is chosen,
// because it is the correct way to decompress a string that was compressed by
// the N corresponding encoders in the "normal" order (first encoder first).
template <size_t N = 2>
class FsstRepeatedDecoder {
 public:
  using Decoders = std::array<FsstDecoder, N>;

 private:
  Decoders decoders_;

 public:
  // The default constructor does lead to an invalid decoder, but is required
  // for the serialization module. Don't use it.
  FsstRepeatedDecoder() = default;

  // Construct from the internal `fsst_decoder_t`. Note that the typical way to
  // obtain an `FsstDecoder` is by first creating a `FsstEncoder` and calling
  // `getDecoder()` on that encoder.
  explicit FsstRepeatedDecoder(Decoders decoders) : decoders_{decoders} {}

  // Decompress a  single string.
  std::string decompress(std::string_view str) const {
    std::string result;
    std::string_view nextInput = str;
    auto decompressSingle = [&result, &nextInput](const FsstDecoder& decoder) {
      result = decoder.decompress(nextInput);
      nextInput = result;
    };

    ql::ranges::for_each(ql::views::reverse(decoders_), decompressSingle);
    return result;
  }

  // Like `decompress`, but each step writes into one of the two buffers of
  // `scratch` (alternating, so that the input of a step is never the buffer it
  // writes to), instead of a new string. The result is a view into `scratch`,
  // valid until `scratch` is used again.
  std::string_view decompressWithScratch(
      std::string_view str, FsstDecompressionScratch& scratch) const {
    std::string_view nextInput = str;
    size_t nextBuffer = 0;
    for (const FsstDecoder& decoder : ql::views::reverse(decoders_)) {
      nextInput = decoder.decompressIntoBuffer(nextInput, scratch[nextBuffer]);
      nextBuffer ^= 1;
    }
    return nextInput;
  }
  // Allow this type to be trivially serializable,
  CPP_template_2(typename T, typename U)(
      requires ql::concepts::same_as<T, FsstRepeatedDecoder>)
  [[maybe_unused]] friend std::true_type allowTrivialSerialization(T, U) {
    return {};
  }
};

// The encoder class.
class FsstEncoder {
 private:
  // The encoder state of FSST is rather complex and managed via a pointer
  // indirection. We manage this using a `unique_ptr` with a custom deleter.
  struct Deleter {
    void operator()(fsst_encoder_t* ptr) const { fsst_destroy(ptr); }
  };
  using Encoder = std::unique_ptr<fsst_encoder_t, Deleter>;
  Encoder encoder_;
  static constexpr auto cast = detail::castToUnsignedPtr;

 public:
  // Create an `FsstEncoder`. The given `strings` are used to create the
  // codebook.
  explicit FsstEncoder(const std::vector<std::string>& strings)
      : encoder_{makeEncoder(strings)} {}

  // Compress a single string.
  std::string compress(std::string_view word) {
    size_t len = word.size();
    std::string output;
    output.resize(7 + 2 * len);
    unsigned char* dummyOutput;
    auto data = cast(word.data());
    size_t outputLen = 0;
    size_t numCompressed =
        fsst_compress(encoder_.get(), 1, &len, &data, output.size(),
                      cast(output.data()), &outputLen, &dummyOutput);
    AD_CORRECTNESS_CHECK(numCompressed == 1);
    output.resize(outputLen);
    return output;
  }

  // Return a decoder, that can be used to decompress strings that have been
  // compressed by this encoder.
  FsstDecoder makeDecoder() const {
    return FsstDecoder{fsst_decoder(encoder_.get())};
  }

  // Interface for the case that all the strings that shall ever be compressed
  // using the same codebook shall also contribute to that codebook. Build a
  // codebook from the `strings`, and then use that codebook to compress each of
  // the `strings`. The result consists of a large `std::string` that contains
  // all the compressed strings concatenated, a `vector<string_view` that points
  // to the compressed strings, and a decoder that can be used to decompress the
  // strings again.
  using BulkResult = std::tuple<std::shared_ptr<std::string>,
                                std::vector<std::string_view>, FsstDecoder>;
  template <typename T>
  static BulkResult compressAll(const T& strings) {
    return makeEncoder<true>(strings);
  }

 private:
  // The implementation of the constructor and of `compressAll`.
  template <bool alsoCompressAll = false, typename Strings>
  static std::conditional_t<alsoCompressAll, BulkResult, Encoder> makeEncoder(
      const Strings& strings) {
    std::vector<size_t> lengths;
    std::vector<const unsigned char*> pointers;
    [[maybe_unused]] size_t totalSize = 0;
    for (const auto& string : strings) {
      lengths.push_back(string.size());
      totalSize += string.size();
      pointers.push_back(cast(string.data()));
    }
    auto encoder =
        fsst_create(strings.size(), lengths.data(), pointers.data(), 0);
    if constexpr (!alsoCompressAll) {
      return Encoder{encoder, Deleter{}};
    } else {
      absl::Cleanup cleanup{[&encoder]() { fsst_destroy(encoder); }};
      auto outputPtr = std::make_unique<std::string>();
      std::string& output = *outputPtr;
      output.resize(totalSize);
      std::vector<char*> outputPtrs;
      outputPtrs.resize(strings.size());
      std::vector<size_t> outputLengths;
      outputLengths.resize(strings.size());
      while (true) {
        size_t numCompressed = fsst_compress(
            encoder, strings.size(), lengths.data(), pointers.data(),
            output.size(), cast(output.data()), outputLengths.data(),
            reinterpret_cast<unsigned char**>(outputPtrs.data()));
        // Typically one iteration should suffice, we repeat in a loop with
        // exponential growth of the output buffer.
        if (numCompressed == strings.size()) {
          break;
        }
        AD_LOG_DEBUG << "FSST compression of a block of strings made the input "
                        "larger instead of smaller"
                     << std::endl;
        output.resize(2 * output.size());
      }
      // Convert the result pointers to `string_views` for easier handling.
      std::vector<std::string_view> stringViews;
      stringViews.reserve(strings.size());
      for (size_t i = 0; i < strings.size(); ++i) {
        stringViews.emplace_back(outputPtrs.at(i), outputLengths.at(i));
      }
      return BulkResult{std::move(outputPtr), std::move(stringViews),
                        FsstDecoder(fsst_decoder(encoder))};
    }
  }
};

#endif  // QLEVER_FSSTCOMPRESSOR_H
