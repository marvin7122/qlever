// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cerrno>
#include <exception>
#include <utility>
#include <vector>

#include "engine/export_v2/ScatterGatherArenaStreamer.h"
#include "util/Generator.h"
#include "util/Log.h"
#include "util/http/beast.h"

namespace ql::engine::export_v2 {

// Beast body whose generator yields ScatterGatherChunk. The writer keeps the
// current chunk alive and exposes its segments as an Asio buffer sequence so
// Beast can write several owned slices without concatenating them first.
// Opt-in via `export-send=iovec`; the default V2 path still uses
// `streamable_body` / `generator<std::string>`.
struct scatter_gather_body {
  class writer;
  using value_type = cppcoro::generator<qlever::export_v2::ScatterGatherChunk>;
};

class scatter_gather_body::writer {
  using Chunk = qlever::export_v2::ScatterGatherChunk;

  value_type& generator_;
  value_type::iterator iterator_;
  Chunk chunk_;
  std::vector<boost::asio::const_buffer> buffers_;
  bool first_ = true;

  bool loadNextChunk(boost::system::error_code& ec) {
    try {
      if (first_) {
        iterator_ = generator_.begin();
        first_ = false;
      } else {
        ++iterator_;
      }
      while (iterator_ != generator_.end()) {
        chunk_ = std::move(*iterator_);
        buffers_.clear();
        buffers_.reserve(chunk_.numSegments());
        chunk_.visitSegments([this](std::string_view bytes) {
          buffers_.emplace_back(bytes.data(), bytes.size());
        });
        if (!buffers_.empty()) {
          return true;
        }
        ++iterator_;
      }
      return false;
    } catch (const std::exception& e) {
      ec = {EPIPE, boost::system::generic_category()};
      AD_LOG_ERROR << "Failed to generate scatter-gather response:\n"
                   << e.what() << std::endl;
      return false;
    }
  }

 public:
  using const_buffers_type = std::vector<boost::asio::const_buffer>;

  template <bool isRequest, class Fields>
  writer([[maybe_unused]] boost::beast::http::header<isRequest, Fields>& header,
         value_type& generator)
      : generator_{generator} {}

  void init(boost::system::error_code& ec) noexcept { ec = {}; }

  boost::optional<std::pair<const_buffers_type, bool>> get(
      boost::system::error_code& ec) {
    ec = {};
    if (!loadNextChunk(ec)) {
      return boost::none;
    }
    return {{buffers_, true}};
  }
};

static_assert(boost::beast::http::is_body<scatter_gather_body>::value,
              "Body type requirements not met");

}  // namespace ql::engine::export_v2
