// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de

#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "util/http/HttpUtils.h"
#include "util/http/beast.h"

using ad_utility::httpUtils::Url;

// ___________________________________________________________________________
TEST(HttpUtils, Url) {
  const auto& HTTP = Url::Protocol::HTTP;
  const auto& HTTPS = Url::Protocol::HTTPS;
  auto check = [](const std::string& urlString, const Url::Protocol& protocol,
                  const std::string& host, const std::string& port,
                  const std::string& target) {
    Url url(urlString);
    ASSERT_EQ(url.protocol(), protocol);
    ASSERT_EQ(url.host(), host);
    ASSERT_EQ(url.port(), port);
    ASSERT_EQ(url.target(), target);
  };
  check("http://host.name/tar/get", HTTP, "host.name", "80", "/tar/get");
  check("https://host.name/tar/get", HTTPS, "host.name", "443", "/tar/get");
  check("http://host.name:81/tar/get", HTTP, "host.name", "81", "/tar/get");
  check("https://host.name:442/tar/get", HTTPS, "host.name", "442", "/tar/get");
  check("http://host.name", HTTP, "host.name", "80", "/");
  check("http://host.name:81", HTTP, "host.name", "81", "/");
  check("https://host.name", HTTPS, "host.name", "443", "/");
  check("https://host.name:442", HTTPS, "host.name", "442", "/");

  ASSERT_EQ(Url("http://bla").protocolAsString(), "http");
  ASSERT_EQ(Url("https://bla").protocolAsString(), "https");

  ASSERT_EQ(Url("http://bla/bli").asString(), "http://bla:80/bli");
  ASSERT_EQ(Url("https://bla:81/bli").asString(), "https://bla:81/bli");

  ASSERT_ANY_THROW(Url("htt://host.name/tar/get"));
  ASSERT_ANY_THROW(Url("http://host.name:8x/tar/get"));
  ASSERT_ANY_THROW(Url("http://host.name:8x"));
}

// ___________________________________________________________________________
TEST(HttpUtils, GetHeaderOnlyRequest) {
  namespace http = boost::beast::http;
  http::request<http::string_body> original;
  original.method(http::verb::post);
  original.target("/api/test");
  original.version(11);
  original.keep_alive(true);
  original.set(http::field::content_type, "text/plain");
  original.set(http::field::authorization, "Bearer token123");
  original.body() = "some body that must not appear in the header-only copy";

  auto headerOnly = ad_utility::httpUtils::getHeaderOnlyRequest(original);

  EXPECT_EQ(headerOnly.method(), http::verb::post);
  EXPECT_EQ(headerOnly.target(), "/api/test");
  EXPECT_EQ(headerOnly.version(), 11);
  EXPECT_TRUE(headerOnly.keep_alive());
  EXPECT_EQ(headerOnly.at(http::field::content_type), "text/plain");
  EXPECT_EQ(headerOnly.at(http::field::authorization), "Bearer token123");
}

// ___________________________________________________________________________
TEST(HttpUtils, GetStringBodyRequest) {
  namespace http = boost::beast::http;
  http::request<http::string_body> original;
  original.method(http::verb::post);
  original.target("/api/test");
  original.version(11);
  original.keep_alive(true);
  original.set(http::field::content_type, "text/plain");
  original.set(http::field::authorization, "Bearer token123");
  original.body() = "original body (must not appear in result)";

  auto result =
      ad_utility::httpUtils::getStringBodyRequest(original, "new body");

  EXPECT_EQ(result.method(), http::verb::post);
  EXPECT_EQ(result.target(), "/api/test");
  EXPECT_EQ(result.version(), 11);
  EXPECT_TRUE(result.keep_alive());
  EXPECT_EQ(result.at(http::field::content_type), "text/plain");
  EXPECT_EQ(result.at(http::field::authorization), "Bearer token123");
  EXPECT_EQ(result.body(), "new body");
}

namespace {
cppcoro::generator<std::string> yieldStrings(std::vector<std::string> parts) {
  for (auto& part : parts) {
    co_yield part;
  }
}

std::string drainBody(ad_utility::httpUtils::ResponseT& response) {
  std::string out;
  for (const auto& part : response.body()) {
    out += part;
  }
  return out;
}
}  // namespace

// ___________________________________________________________________________
TEST(HttpUtils, CreateOkResponsePreEncoded) {
  namespace http = boost::beast::http;
  using ad_utility::content_encoding::CompressionMethod;
  // The client accepts both; the producer chose the encoding.
  http::request<http::string_body> request{http::verb::get, "/", 11};
  request.set(http::field::accept_encoding, "gzip, deflate");
  for (auto [method, name] :
       {std::pair{CompressionMethod::DEFLATE, std::string_view{"deflate"}},
        std::pair{CompressionMethod::GZIP, std::string_view{"gzip"}}}) {
    auto response = ad_utility::httpUtils::createOkResponsePreEncoded(
        yieldStrings({"\x78\x01", "pre-", "encoded"}), request,
        ad_utility::MediaType::csv, method);
    EXPECT_EQ(response.result(), http::status::ok);
    EXPECT_EQ(response[http::field::content_encoding], name);
    EXPECT_EQ(response[http::field::content_type], "text/csv");
    // The bytes pass through unchanged: no second compression.
    EXPECT_EQ(drainBody(response), "\x78\x01pre-encoded");
  }

  // `NONE` sets no `Content-Encoding`.
  auto plain = ad_utility::httpUtils::createOkResponsePreEncoded(
      yieldStrings({"a,b\n"}), request, ad_utility::MediaType::csv,
      CompressionMethod::NONE);
  EXPECT_EQ(plain.count(http::field::content_encoding), 0u);
  EXPECT_EQ(drainBody(plain), "a,b\n");
}
