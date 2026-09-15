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

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "celer/net/server.h"
#include "celer/net/tcp_service.h"
#include "spdlog/spdlog.h"

namespace celer {

namespace {

std::atomic<bool> g_shutdown_requested = false;
volatile sig_atomic_t g_last_shutdown_signal = 0;
int g_signal_event_fd = -1;

void ShutdownSignalHandler(int signal) {
  g_last_shutdown_signal = signal;
  if (g_signal_event_fd < 0) {
    return;
  }
  const std::uint64_t wake = 1;
  (void)write(g_signal_event_fd, &wake, sizeof(wake));
}

absl::Status InstallShutdownSignalHandler() {
  g_signal_event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (g_signal_event_fd < 0) {
    return absl::InternalError("eventfd setup failed");
  }

  struct sigaction action{};
  sigemptyset(&action.sa_mask);
  action.sa_handler = ShutdownSignalHandler;
  if (sigaction(SIGINT, &action, nullptr) != 0 ||
      sigaction(SIGTERM, &action, nullptr) != 0) {
    close(g_signal_event_fd);
    g_signal_event_fd = -1;
    return absl::InternalError("sigaction setup failed");
  }
  return absl::OkStatus();
}

void CleanupShutdownSignalHandler() noexcept {
  struct sigaction action{};
  sigemptyset(&action.sa_mask);
  action.sa_handler = SIG_DFL;
  (void)sigaction(SIGINT, &action, nullptr);
  (void)sigaction(SIGTERM, &action, nullptr);
  if (g_signal_event_fd >= 0) {
    close(g_signal_event_fd);
    g_signal_event_fd = -1;
  }
}

class EchoService final : public TcpService {
 public:
  explicit EchoService(std::uint16_t port) : TcpService(port) {}

 protected:
  Task<absl::Status> Serve(TcpStream stream) override {
    std::array<std::byte, 4096> buffer{};
    while (true) {
      auto read_result = co_await stream.ReadSome(buffer);
      if (!read_result.ok()) [[unlikely]] {
        co_return read_result.status();
      }
      if (*read_result == 0) [[unlikely]] {
        co_return absl::OkStatus();
      }
      auto write_status = co_await stream.WriteAll(
          std::span<const std::byte>(buffer.data(), *read_result));
      if (!write_status.ok()) [[unlikely]] {
        co_return write_status;
      }
    }
  }
};

enum class WaitResult {
  kSignal,
  kStopped,
};

WaitResult WaitForSignalOrServerStop(const Server& server) {
  pollfd fds[2] = {
      {.fd = g_signal_event_fd, .events = POLLIN, .revents = 0},
      {.fd = server.completion_fd(), .events = POLLIN, .revents = 0},
  };

  while (true) {
    const int rc = poll(fds, 2, -1);
    if (rc < 0) [[unlikely]] {
      if (errno == EINTR) {
        continue;
      }
      spdlog::warn("poll failed errno={}", errno);
      return WaitResult::kStopped;
    }
    if ((fds[0].revents & POLLIN) != 0) {
      std::uint64_t wake = 0;
      (void)read(g_signal_event_fd, &wake, sizeof(wake));
      g_shutdown_requested.store(true, std::memory_order_release);
      return WaitResult::kSignal;
    }
    if ((fds[1].revents & POLLIN) != 0) {
      std::uint64_t wake = 0;
      (void)read(server.completion_fd(), &wake, sizeof(wake));
      return WaitResult::kStopped;
    }
  }
}

}  // namespace

}  // namespace celer

int main(int argc, char** argv) {
  std::string_view bind_ip = "127.0.0.1";
  std::uint16_t port = 8080;
  unsigned thread_count = 1;
  int idle_timeout_ms = -1;

  if (argc >= 2) {
    bind_ip = argv[1];
  }
  if (argc >= 3) {
    port = static_cast<std::uint16_t>(std::stoi(argv[2]));
  }
  if (argc >= 4) {
    thread_count = static_cast<unsigned>(std::stoul(argv[3]));
    if (thread_count == 0) [[unlikely]] {
      spdlog::error("thread_count must be >= 1");
      return 1;
    }
  }
  if (argc >= 5) {
    idle_timeout_ms = std::stoi(argv[4]);
  }

  spdlog::info(
      "celer echo server listening on {}:{} threads={} idle_timeout_ms={}",
      bind_ip, port, thread_count, idle_timeout_ms);

  const auto signal_status = celer::InstallShutdownSignalHandler();
  if (!signal_status.ok()) [[unlikely]] {
    spdlog::error("signal setup failed: {}", signal_status.message());
    return 1;
  }

  celer::ServerOptions options;
  options.bind_ip_ = std::string(bind_ip);
  options.thread_count_ = thread_count;
  options.idle_timeout_ms_ = idle_timeout_ms;

  celer::EchoService echo(port);
  celer::Server server;
  server.AddService(&echo);
  auto start_status = server.Start(options);
  if (!start_status.ok()) [[unlikely]] {
    spdlog::error("server start failed: {}", start_status.message());
    celer::CleanupShutdownSignalHandler();
    return 1;
  }

  const celer::WaitResult wait_result =
      celer::WaitForSignalOrServerStop(server);
  if (wait_result == celer::WaitResult::kSignal) {
    const int signal = static_cast<int>(celer::g_last_shutdown_signal);
    spdlog::info(
        "shutdown requested by signal {}",
        (signal == 0 ? std::string("unknown") : std::to_string(signal)));
    server.RequestStop();
  }

  server.WaitUntilStopped();
  celer::CleanupShutdownSignalHandler();
  return server.exit_code();
}
