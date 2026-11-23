// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <charconv>
#include <chrono>
#include <iostream>
#include <string_view>

#include "bycorf/io/storage.h"
#include "bycorf/runtime/runtime.h"
#include "rdma_demo_session.h"

namespace {
using namespace bycorf;
using namespace bycorf::rdma;
struct Options {
  bool server;
  std::string_view ip;
  unsigned port = 7471;
  unsigned bytes = 65536;
  unsigned timeout = 30;
};
bool Number(std::string_view text, unsigned& value) {
  auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size();
}
Task<absl::Status> Exchange(Worker& worker, Options options) {
  RcConnection connection;
  if (options.server) {
    auto listener = RcListener::Listen(worker, options.ip, options.port);
    if (!listener.ok()) co_return listener.status();
    // Port zero lets the integration harness discover an OS-assigned endpoint
    // without reserving a TCP port or probing the single-use RDMA listener.
    std::cout << "RDMA_READY " << listener->port() << std::endl;
    auto accepted = co_await listener->Accept();
    const auto closed = co_await listener->Close();
    if (!accepted.ok()) co_return accepted.status();
    if (!closed.ok()) co_return closed;
    connection = std::move(*accepted);
  } else {
    auto connected =
        co_await RcConnection::Connect(worker, options.ip, options.port);
    if (!connected.ok()) co_return connected.status();
    connection = std::move(*connected);
  }
  co_return co_await example::RunRdmaDemo(worker, connection, options.server,
                                          options.bytes);
}
Task<absl::Status> Complete(Worker& worker, Options options, bool& finished,
                            absl::Status& result) {
  result = co_await Exchange(worker, options);
  finished = true;
  worker.RequestStop();
  co_return result;
}
Task<absl::Status> Deadline(Worker& worker, unsigned seconds) {
  const auto status = co_await SleepFor(worker, std::chrono::seconds(seconds));
  worker.RequestStop();
  co_return status;
}
}  // namespace

int main(int argc, char** argv) {
  constexpr std::string_view usage =
      "Usage: bycorf_rdma_demo server|client IP [PORT [BYTES "
      "[TIMEOUT_SECONDS]]]\n"
      "Defaults: port 7471, payload 65536 bytes, deadline 30 seconds.\n"
      "One connection exercises SEND/RECV, READ/WRITE and both IMM variants.\n";
  if (argc == 2 && std::string_view(argv[1]) == "--help") {
    std::cout << usage;
    return 0;
  }
  Options options{};
  if (argc < 3 || argc > 6 ||
      (std::string_view(argv[1]) != "server" &&
       std::string_view(argv[1]) != "client")) {
    std::cerr << usage;
    return 2;
  }
  options.server = std::string_view(argv[1]) == "server";
  options.ip = argv[2];
  if ((argc > 3 && !Number(argv[3], options.port)) ||
      (argc > 4 && !Number(argv[4], options.bytes)) ||
      (argc > 5 && !Number(argv[5], options.timeout)) || options.port > 65535 ||
      (!options.server && !options.port) || !options.bytes ||
      options.bytes > 16 * 1024 * 1024 || !options.timeout ||
      options.timeout > 3600) {
    std::cerr << "Invalid port, payload (1..16777216), or deadline (1..3600)\n";
    return 2;
  }
  Runtime runtime;
  runtime.Start(1, [&](unsigned, Worker& worker) {
    const auto initialized = worker.Init({.recv_buffer_count_ = 0});
    if (!initialized.ok()) {
      std::cerr << initialized << '\n';
      return 1;
    }
    bool finished = false;
    absl::Status result =
        absl::DeadlineExceededError("RDMA example did not finish");
    worker.Spawn(Complete(worker, options, finished, result));
    worker.Spawn(Deadline(worker, options.timeout));
    worker.Run();
    // Quiesce DMA and kernel callbacks BEFORE reclaiming frame-owned payloads,
    // timers and RC handles, always on their owner worker.
    worker.Shutdown();
    worker.DestroyDetachedTasks();
    if (!finished || !result.ok()) {
      std::cerr << result << '\n';
      return 1;
    }
    std::cout << "RDMA_OK " << (options.server ? "server" : "client")
              << " bytes=" << options.bytes << std::endl;
    return 0;
  });
  runtime.WaitUntilStopped();
  return runtime.exit_code();
}
