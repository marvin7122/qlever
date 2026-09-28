// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_HTTP_ZEROCOPYHTTPSENDER_H
#define QLEVER_SRC_UTIL_HTTP_ZEROCOPYHTTPSENDER_H

#include <string>
#include <type_traits>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/http/ZeroCopyChunkSender.h"
#include "util/http/beast.h"
#include "util/http/streamable_body.h"

namespace ad_utility::httpUtils {

// The sender used for the export path of one HTTP session.
using ZeroCopyHttpSender = ZeroCopyChunkSender<SocketSendBackend>;

// Detect `http::response<streamable_body>` (any fields type) for the
// send-path dispatch in `HttpServer`.
template <typename T>
struct IsStreamableBodyResponse : std::false_type {};
template <typename Fields>
struct IsStreamableBodyResponse<
    boost::beast::http::response<httpStreams::streamable_body, Fields>>
    : std::true_type {};

// Send a chunked `streamable_body` response over `stream`. The response head
// is written with Boost.Beast; the body chunks are then moved out of the
// generator into `sender`, which sends each of them with its chunk framing in
// one `IORING_OP_SENDMSG_ZC` straight from the generator's memory (or with a
// blocking `sendmsg()` when io_uring is unavailable; the bytes are identical
// either way). Only valid when `response.chunked()` is true. Generator
// exceptions propagate like on the Beast path (the session is then closed);
// before that, the sender waits until the kernel no longer references any
// chunk.
template <typename Stream>
boost::asio::awaitable<void> asyncWriteStreamableBodyZeroCopy(
    Stream& stream,
    boost::beast::http::response<httpStreams::streamable_body>& response,
    ZeroCopyHttpSender& sender) {
  namespace http = boost::beast::http;
  AD_CONTRACT_CHECK(response.chunked());
  // Serialize the head (status line + headers, no body access yet).
  http::response_serializer<httpStreams::streamable_body> serializer{response};
  co_await http::async_write_header(stream, serializer,
                                    boost::asio::use_awaitable);
  auto generator = std::move(response.body());
  const size_t notificationsBefore = sender.numNotifications();
  const size_t copiedBefore = sender.numKernelCopiedNotifications();
  try {
    for (auto& chunk : generator) {
      // An empty chunk would serialize as the terminating `0` chunk, so it
      // must never be emitted mid-body.
      if (!chunk.empty()) {
        sender.sendChunk(std::move(chunk));
      }
    }
    sender.sendRaw("0\r\n\r\n");
    sender.finish();
  } catch (...) {
    sender.abort();
    throw;
  }
  AD_LOG_INFO << "Zero-copy export response: "
              << sender.numNotifications() - notificationsBefore
              << " send notification(s), "
              << sender.numKernelCopiedNotifications() - copiedBefore
              << " of them copied by the kernel" << std::endl;
  co_return;
}

}  // namespace ad_utility::httpUtils

#endif  // QLEVER_SRC_UTIL_HTTP_ZEROCOPYHTTPSENDER_H
