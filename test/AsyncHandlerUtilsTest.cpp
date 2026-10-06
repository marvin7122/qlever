// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "util/AsyncHandlerUtils.h"

namespace {
namespace net = boost::asio;
using ad_utility::makeHandlerExecutorAware;
using namespace std::chrono_literals;

// _____________________________________________________________________________
TEST(AsyncHandlerUtils, DefaultExecutorPostsAndForwardsArguments) {
  net::io_context context;
  auto exception = std::make_exception_ptr(std::runtime_error{"test error"});
  bool called = false;
  auto wrapper = makeHandlerExecutorAware<int>(
      [&](std::exception_ptr actualException, int payload) {
        called = true;
        EXPECT_EQ(actualException, exception);
        EXPECT_EQ(payload, 42);
        EXPECT_TRUE(context.get_executor().running_in_this_thread());
      },
      context.get_executor());

  wrapper(exception, 42);
  EXPECT_FALSE(called);
  EXPECT_EQ(context.run(), 1);
  EXPECT_TRUE(called);
  // The wrapper is still alive, but it must no longer retain work.
  EXPECT_TRUE(context.stopped());
}

// _____________________________________________________________________________
TEST(AsyncHandlerUtils, SameStrandPostsWithoutHopping) {
  net::io_context context;
  auto strand = net::make_strand(context);
  std::vector<int> events;
  auto wrapper = makeHandlerExecutorAware<int>(
      net::bind_executor(strand,
                         [&](std::exception_ptr exception, int payload) {
                           EXPECT_FALSE(exception);
                           EXPECT_EQ(payload, 42);
                           EXPECT_TRUE(strand.running_in_this_thread());
                           events.push_back(3);
                         }),
      context.get_executor());

  net::post(strand, [&] {
    EXPECT_TRUE(strand.running_in_this_thread());
    events.push_back(1);
    wrapper(nullptr, 42);
    events.push_back(2);
  });
  context.run();
  EXPECT_EQ(events, (std::vector<int>{1, 2, 3}));
  EXPECT_TRUE(context.stopped());
}

// _____________________________________________________________________________
TEST(AsyncHandlerUtils, PendingAssociatedContextRetainsWork) {
  net::io_context sourceContext;
  net::io_context completionContext;
  std::promise<int> completed;
  auto completionFuture = completed.get_future();
  auto wrapper = makeHandlerExecutorAware<std::unique_ptr<int>>(
      net::bind_executor(
          completionContext.get_executor(),
          [state = std::make_unique<int>(19), &completed, &sourceContext,
           &completionContext](std::exception_ptr exception,
                               std::unique_ptr<int> payload) {
            EXPECT_FALSE(exception);
            EXPECT_TRUE(
                completionContext.get_executor().running_in_this_thread());
            EXPECT_FALSE(sourceContext.get_executor().running_in_this_thread());
            completed.set_value(*state + *payload);
          }),
      sourceContext.get_executor());

  EXPECT_EQ(completionContext.poll(), 0);
  ASSERT_FALSE(completionContext.stopped());

  std::promise<void> finished;
  auto finishedFuture = finished.get_future();
  std::thread runner{[&] {
    try {
      completionContext.run();
      finished.set_value();
    } catch (...) {
      finished.set_exception(std::current_exception());
    }
  }};
  absl::Cleanup cleanup{[&] {
    completionContext.stop();
    runner.join();
  }};

  net::post(sourceContext,
            [&] { wrapper(nullptr, std::make_unique<int>(23)); });
  EXPECT_EQ(sourceContext.run(), 1);
  ASSERT_EQ(completionFuture.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(completionFuture.get(), 42);
  // The wrapper is still alive here, so retaining work in it would prevent
  // exit.
  ASSERT_EQ(finishedFuture.wait_for(2s), std::future_status::ready);
  EXPECT_NO_THROW(finishedFuture.get());
}

// _____________________________________________________________________________
TEST(AsyncHandlerUtils, AbandonedWrapperReleasesWork) {
  net::io_context context;
  {
    auto wrapper = makeHandlerExecutorAware<int>(
        [](std::exception_ptr, int) {
          FAIL() << "Abandoned handler was called";
        },
        context.get_executor());
    EXPECT_EQ(context.poll(), 0);
    EXPECT_FALSE(context.stopped());
  }
  EXPECT_EQ(context.poll(), 0);
  EXPECT_TRUE(context.stopped());
}

// _____________________________________________________________________________
TEST(AsyncHandlerUtils, ThrowingCompletionReleasesWork) {
  net::io_context context;
  auto wrapper = makeHandlerExecutorAware<int>(
      [](std::exception_ptr, int) {
        throw std::runtime_error{"completion error"};
      },
      context.get_executor());

  wrapper(nullptr, 42);
  EXPECT_THROW(context.run(), std::runtime_error);
  // The wrapper is still alive, even after the completion threw.
  EXPECT_EQ(context.poll(), 0);
  EXPECT_TRUE(context.stopped());
}
}  // namespace
