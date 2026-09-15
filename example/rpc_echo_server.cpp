/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Minimal RPC echo server for benchmarking celer::rpc.
//   rpc_echo_server [bind_ip] [port] [threads]
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <string>
#include <string_view>

#include "celer/net/server.h"
#include "celer/rpc/rpc.h"
#include "spdlog/spdlog.h"

namespace {

int g_signal_event_fd = -1;

void OnSignal(int) {
  if (g_signal_event_fd >= 0) {
    const std::uint64_t v = 1;
    (void)write(g_signal_event_fd, &v, sizeof(v));
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::string bind_ip = (argc >= 2) ? argv[1] : "127.0.0.1";
  std::uint16_t port =
      (argc >= 3) ? static_cast<std::uint16_t>(std::stoi(argv[2])) : 9000;
  unsigned threads =
      (argc >= 4) ? static_cast<unsigned>(std::stoul(argv[3])) : 1;

  g_signal_event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  struct sigaction sa{};
  sigemptyset(&sa.sa_mask);
  sa.sa_handler = OnSignal;
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);

  celer::rpc::RpcServer rpc(port);
  rpc.OnVerb(1, [](celer::rpc::BytesView req) {
    return celer::rpc::Bytes(req.begin(), req.end());  // echo
  });

  celer::ServerOptions options;
  options.bind_ip_ = bind_ip;
  options.thread_count_ = threads;

  celer::Server server;
  server.AddService(&rpc);
  auto status = server.Start(options);
  if (!status.ok()) {
    spdlog::error("rpc echo server start failed: {}", status.message());
    return 1;
  }
  spdlog::info("rpc echo server on {}:{} threads={}", bind_ip, port, threads);

  pollfd fds[2] = {
      {.fd = g_signal_event_fd, .events = POLLIN, .revents = 0},
      {.fd = server.completion_fd(), .events = POLLIN, .revents = 0},
  };
  while (true) {
    const int rc = poll(fds, 2, -1);
    if (rc < 0 && errno == EINTR) continue;
    if ((fds[0].revents & POLLIN) != 0 || (fds[1].revents & POLLIN) != 0) break;
  }

  spdlog::info("shutting down");
  server.RequestStop();
  server.WaitUntilStopped();
  if (g_signal_event_fd >= 0) close(g_signal_event_fd);
  return server.exit_code();
}
