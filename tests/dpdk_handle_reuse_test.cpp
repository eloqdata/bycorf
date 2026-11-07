// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>
#include <netinet/in.h>

#include <array>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <string_view>

#include "bycorf/io/dpdk_backend.h"
#include "bycorf/io/io.h"
#include "bycorf/net/connection.h"
#include "freebsd/abi.h"
#include "runtime_test.h"

namespace {
std::string_view scenario;
unsigned send_calls = 0;
}  // namespace

// Real EAL/ring PMD, BSD socket creation/close and runtime completion delivery;
// only readiness is controlled so old operations deterministically stay queued.
extern "C" int __wrap_bycorf_bsd_send(struct socket*, const void*, size_t size,
                                      size_t* sent) {
  ++send_calls;
  *sent = scenario == "completion" ? size : 0;
  return *sent ? 0 : 35;  // FreeBSD EAGAIN.
}
extern "C" int __wrap_bycorf_bsd_connect(struct socket*, uint32_t, uint16_t) {
  return scenario == "connect-error" ? 61 : 36;  // ECONNREFUSED / EINPROGRESS.
}

namespace {
struct Completion final : bycorf::IoCompletion {
  unsigned calls = 0;
  int result = 0;
  unsigned flags = 0;
  int close_on_complete = -1;
  int opened_in_callback = -1;
  void Complete(bycorf::Worker&, int value,
                unsigned completion_flags) override {
    ++calls;
    result = value;
    flags = completion_flags;
    if (close_on_complete >= 0) {
      EXPECT_EQ(bycorf::DpdkBackend::Close(close_on_complete), 0);
      opened_in_callback = bycorf::DpdkBackend::OpenClient(AF_INET);
      EXPECT_GE(opened_in_callback, 0);
      EXPECT_NE(opened_in_callback, close_on_complete);
    }
  }
};

Completion shutdown_completion;
std::array<std::byte, 8> shutdown_bytes{};

bycorf::Task<absl::Status> CheckReuse(bycorf::Worker& worker) {
  using bycorf::DpdkBackend;
  if (scenario == "churn") {
    const int first = DpdkBackend::OpenClient(AF_INET);
    if (first < 0) co_return absl::InternalError("initial socket failed");
    EXPECT_EQ(DpdkBackend::Close(first), 0);
    for (unsigned i = 0; i < 256; ++i) {
      const int next = DpdkBackend::OpenClient(AF_INET);
      if (next < 0) co_return absl::InternalError("socket churn failed");
      EXPECT_EQ(next, first);
      EXPECT_EQ(DpdkBackend::Close(next), 0);
    }
    co_return absl::OkStatus();
  }

  const int old = DpdkBackend::OpenClient(AF_INET);
  if (old < 0) co_return absl::InternalError("initial socket failed");
  if (scenario == "shutdown") {
    // This unregistered socket and completion outlive the request coroutine.
    // Backend shutdown must still close and drain them exactly once.
    co_return worker.SubmitSend({.fd_ = old}, shutdown_bytes,
                                &shutdown_completion);
  }
  Completion completion;
  std::array<std::byte, 8> bytes{};
  iovec vector{bytes.data(), bytes.size()};
  msghdr message{};
  message.msg_iov = &vector;
  message.msg_iovlen = 1;
  bycorf::Connection connection;
  connection.worker_ = &worker;
  connection.file_.fd_ = old;
  connection.closed_ = false;
  absl::Status submitted;
  if (scenario == "sendv") {
    submitted = worker.SubmitSendMsg({.fd_ = old}, &message, &completion);
  } else if (scenario.starts_with("connect")) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(0xc6120002);
    address.sin_port = htons(12345);
    submitted =
        worker.SubmitConnect(old, reinterpret_cast<const sockaddr*>(&address),
                             sizeof(address), &completion);
  } else if (scenario == "accept") {
    // Closing before the first poll cancels the request without entering BSD.
    submitted = worker.SubmitAcceptMultishot(old, &completion);
  } else if (scenario == "recv" || scenario == "recv-cancel") {
    submitted = worker.EnsureRecvArmed(&connection);
    if (submitted.ok() && scenario == "recv-cancel")
      submitted = worker.SubmitCancelRecv(&connection, &completion);
  } else if (scenario == "disconnect") {
    submitted = worker.EnsurePeerDisconnectPollArmed(&connection);
  } else {
    submitted = worker.SubmitSend({.fd_ = old}, bytes, &completion);
  }
  if (!submitted.ok()) {
    DpdkBackend::Close(old);
    co_return submitted;
  }

  if (scenario == "send" || scenario == "sendv" || scenario == "cancel") {
    co_await bycorf::Yield(worker);
    EXPECT_GT(send_calls, 0U);
    EXPECT_EQ(completion.calls, 0U) << "EAGAIN must leave the old send pending";
  }
  const unsigned attempts_before_close = send_calls;

  int successor = -1;
  if (scenario == "completion") {
    completion.close_on_complete = old;
  } else {
    if (scenario == "cancel")
      EXPECT_TRUE(worker.SubmitCancel(&completion).ok());
    EXPECT_EQ(DpdkBackend::Close(old), 0);
    successor = DpdkBackend::OpenClient(AF_INET);
    EXPECT_GE(successor, 0);
    EXPECT_NE(successor, old) << "pending operation still names the old socket";
  }

  // Yield returns to backend polling before resuming this coroutine.
  co_await bycorf::Yield(worker);
  if (scenario == "recv" || scenario == "disconnect") {
    EXPECT_EQ(connection.inflight_ops_, 0U);
    EXPECT_FALSE(connection.recv_armed_);
    EXPECT_FALSE(connection.peer_disconnect_poll_armed_);
  } else {
    EXPECT_EQ(completion.calls, 1U);
    EXPECT_EQ(completion.flags, 0U);  // Closed multishot accept is terminal.
    const int expected =
        scenario == "recv-cancel"     ? 0
        : scenario == "completion"    ? static_cast<int>(bytes.size())
        : scenario == "connect-error" ? -ECONNREFUSED
        : scenario == "connect" || scenario == "cancel" ? -ECANCELED
                                                        : -EBADF;
    EXPECT_EQ(completion.result, expected);
  }
  if (scenario == "completion") successor = completion.opened_in_callback;
  EXPECT_EQ(send_calls, scenario == "completion" ? 1U : attempts_before_close)
      << "a closed request must never send through the successor socket";
  const int reused = DpdkBackend::OpenClient(AF_INET);
  EXPECT_EQ(reused, old) << "completion delivery must return the handle slot";
  if (reused >= 0) EXPECT_EQ(DpdkBackend::Close(reused), 0);
  if (successor >= 0) EXPECT_EQ(DpdkBackend::Close(successor), 0);
  co_return absl::OkStatus();
}

int RunScenario(std::string_view name) {
  scenario = name;
  setenv("BYCORF_EAL_ARGS", "--no-pci --no-huge --vdev=net_ring0", 1);
  setenv("BYCORF_DPDK_MODE", "poll", 1);
  setenv("BYCORF_DPDK_QUEUES", "1", 1);
  setenv("BYCORF_DPDK_RX_STEERING", "hash", 1);
  setenv("BYCORF_DPDK_IP", "198.18.0.2", 1);
  setenv("BYCORF_DPDK_NETMASK", "255.255.255.0", 1);
  setenv("BYCORF_DPDK_MEMORY_MB", "512", 1);
  unsetenv("BYCORF_DPDK_GATEWAY");
  if (!bycorf::ConfigureIoBackends({.dpdk_network = true}).ok()) return 2;
  bycorf::test::RunOnWorker(CheckReuse);
  if (scenario == "shutdown") {
    EXPECT_EQ(shutdown_completion.calls, 1U);
    EXPECT_EQ(shutdown_completion.result, -EBADF);
  }
  return testing::Test::HasFailure() ? 1 : 0;
}

class DpdkHandleReuseDeathTest : public testing::TestWithParam<const char*> {};
TEST_P(DpdkHandleReuseDeathTest, ReusesOnlyAfterOldOperationsRetire) {
  // Backend selection/EAL have process lifetime; each case needs a fresh child.
  const auto selected = GetParam();
  ASSERT_EXIT(std::exit(RunScenario(selected)), testing::ExitedWithCode(0), "");
}
INSTANTIATE_TEST_SUITE_P(Scenarios, DpdkHandleReuseDeathTest,
                         testing::Values("churn", "send", "sendv", "connect",
                                         "connect-error", "accept", "cancel",
                                         "completion", "recv", "recv-cancel",
                                         "disconnect", "shutdown"));
}  // namespace
