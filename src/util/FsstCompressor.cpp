#include "util/FsstCompressor.h"

#include <string>
#include <string_view>

// _____________________________________________________________________________
size_t FsstDecoder::decompressInto(std::string_view str,
                                   ql::span<char> out) const {
  const size_t bound = maxDecompressedSize(str);
  AD_CONTRACT_CHECK(out.size() >= bound);
  if (bound == 0) {
    return 0;
  }
  auto cast = detail::castToUnsignedPtr;
  size_t size = fsst_decompress(&decoder_, str.size(), cast(str.data()), bound,
                                cast(out.data()));
  AD_CORRECTNESS_CHECK(size <= bound);
  return size;
}
