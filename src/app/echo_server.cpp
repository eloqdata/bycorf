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

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <memory>
#include <pthread.h>
#include <string>
#include <string_view>
#include <span>
#include <thread>
#include <vector>

#include "celer/base/log.h"
#include "celer/base/status.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/worker.h"

namespace celer {

namespace {

std::atomic<bool> g_shutdown_requested = false;
std::atomic<int> g_last_shutdown_signal = 0;

sigset_t ShutdownSignalSet() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  return set;
}

void BlockShutdownSignals() {
  g_shutdown_requested.store(false, std::memory_order_release);
  g_last_shutdown_signal.store(0, std::memory_order_relaxed);
  const sigset_t set = ShutdownSignalSet();
  pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

void WaitForShutdownSignal(std::atomic<bool>* running) {
  const sigset_t set = ShutdownSignalSet();
  while (running->load(std::memory_order_acquire)) {
    timespec timeout{
        .tv_sec = 0,
        .tv_nsec = 100 * 1000 * 1000,
    };
    const int signal = sigtimedwait(&set, nullptr, &timeout);
    if (signal == SIGINT || signal == SIGTERM) {
      g_last_shutdown_signal.store(signal, std::memory_order_relaxed);
      g_shutdown_requested.store(true, std::memory_order_release);
      return;
    }
    if (signal < 0 && errno != EAGAIN && errno != EINTR) {
      CELER_LOG_WARN << "sigtimedwait failed errno=" << errno;
    }
  }
}

struct EchoWorkerRuntime {
  Worker worker;
  TcpListener listener;
  std::atomic<bool> stop_requested = false;
  std::atomic<bool> accept_loop_done = false;
  std::atomic<bool> finished = false;
  std::atomic<int> exit_code = 0;

  void BeginShutdown() {
    stop_requested.store(true, std::memory_order_release);
    listener.Close();
  }

  void RequestStop() {
    BeginShutdown();
    worker.RequestStop();
  }
};

Task<Status> EchoSession(Worker& worker, Connection* connection) {
  TcpStream stream(connection);
  std::array<std::byte, 4096> buffer{};
  while (true) {
    auto read_result = co_await stream.ReadSome(buffer);
    if (!read_result.ok()) {
      worker.BeginClose(connection, read_result.status(), CloseMode::kLocalError);
      co_return read_result.status();
    }
    if (*read_result == 0) {
      worker.BeginClose(connection, Status::Ok(), CloseMode::kPeerClosed);
      co_return Status::Ok();
    }

    auto write_status =
        co_await stream.WriteAll(std::span<const std::byte>(buffer.data(), *read_result));
    if (!write_status.ok()) {
      worker.BeginClose(connection, write_status, CloseMode::kLocalError);
      co_return write_status;
    }
  }
}

Task<Status> AcceptLoop(EchoWorkerRuntime& runtime) {
  while (!runtime.stop_requested.load(std::memory_order_acquire)) {
    auto accepted = co_await runtime.listener.Accept();
    if (!accepted.ok()) {
      const auto code = accepted.status().code();
      if (runtime.stop_requested.load(std::memory_order_acquire) ||
          code == StatusCode::kCancelled ||
          code == StatusCode::kFailedPrecondition) {
        runtime.accept_loop_done.store(true, std::memory_order_release);
        co_return Status::Ok();
      }
      if (code != StatusCode::kUnavailable) {
        CELER_LOG_WARN << "accept failed: " << accepted.status().message();
      }
      continue;
    }

    runtime.worker.Spawn(EchoSession(runtime.worker, *accepted));
  }
  runtime.accept_loop_done.store(true, std::memory_order_release);
  co_return Status::Ok();
}

void RunEchoWorker(std::string bind_ip, std::uint16_t port, RecvMode recv_mode,
                   int idle_timeout_ms, bool reuse_port, unsigned worker_index,
                   EchoWorkerRuntime* runtime) {
  WorkerOptions worker_options;
  worker_options.recv_mode = recv_mode;
  worker_options.idle_timeout_ms = idle_timeout_ms;

  auto init_status = runtime->worker.Init(worker_options);
  if (!init_status.ok()) {
    CELER_LOG_ERROR << "worker[" << worker_index
                      << "] init failed: " << init_status.message();
    runtime->exit_code.store(1, std::memory_order_release);
    runtime->finished.store(true, std::memory_order_release);
    return;
  }

  auto bind_status = runtime->listener.Bind(&runtime->worker, bind_ip, port, 128, reuse_port);
  if (!bind_status.ok()) {
    CELER_LOG_ERROR << "worker[" << worker_index
                      << "] bind failed: " << bind_status.message();
    runtime->exit_code.store(1, std::memory_order_release);
    runtime->finished.store(true, std::memory_order_release);
    runtime->worker.RequestStop();
    return;
  }

  runtime->worker.Spawn(AcceptLoop(*runtime));
  runtime->worker.Run();
  if (!runtime->stop_requested.load(std::memory_order_acquire) &&
      !g_shutdown_requested.load(std::memory_order_acquire)) {
    runtime->exit_code.store(1, std::memory_order_release);
  }
  runtime->finished.store(true, std::memory_order_release);
}

}  // namespace

}  // namespace celer

int main(int argc, char** argv) {
  std::string_view bind_ip = "127.0.0.1";
  std::uint16_t port = 8080;
  unsigned thread_count = 1;
  int idle_timeout_ms = -1;
  constexpr celer::RecvMode recv_mode = celer::kDefaultRecvMode;

  if (argc >= 2) {
    bind_ip = argv[1];
  }
  if (argc >= 3) {
    port = static_cast<std::uint16_t>(std::stoi(argv[2]));
  }
  if (argc >= 4) {
    thread_count = static_cast<unsigned>(std::stoul(argv[3]));
    if (thread_count == 0) {
      CELER_LOG_ERROR << "thread_count must be >= 1";
      return 1;
    }
  }
  if (argc >= 5) {
    idle_timeout_ms = std::stoi(argv[4]);
  }

  CELER_LOG_INFO << "celer echo server listening on " << bind_ip << ':' << port
                   << " threads=" << thread_count
                   << " idle_timeout_ms=" << idle_timeout_ms
                   << " recv_mode="
                   << (recv_mode == celer::RecvMode::kMultishot ? "multishot"
                                                                   : "registered_buf");

  const bool reuse_port = thread_count > 1;
  celer::BlockShutdownSignals();
  std::atomic<bool> signal_wait_running = true;
  std::thread signal_waiter(celer::WaitForShutdownSignal, &signal_wait_running);

  std::vector<std::unique_ptr<celer::EchoWorkerRuntime>> runtimes;
  runtimes.reserve(thread_count);
  std::vector<std::thread> threads;
  threads.reserve(thread_count);
  for (unsigned i = 0; i < thread_count; ++i) {
    runtimes.push_back(std::make_unique<celer::EchoWorkerRuntime>());
    threads.emplace_back(celer::RunEchoWorker, std::string(bind_ip), port, recv_mode,
                         idle_timeout_ms, reuse_port, i, runtimes.back().get());
  }

  bool failed = false;
  while (true) {
    bool all_finished = true;
    for (const auto& runtime : runtimes) {
      if (!runtime->finished.load(std::memory_order_acquire)) {
        all_finished = false;
      }
      if (runtime->exit_code.load(std::memory_order_acquire) != 0) {
        failed = true;
      }
    }

    if (celer::g_shutdown_requested.load(std::memory_order_acquire) || failed || all_finished) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  signal_wait_running.store(false, std::memory_order_release);
  signal_waiter.join();

  if (celer::g_shutdown_requested.load(std::memory_order_acquire)) {
    const int signal = celer::g_last_shutdown_signal.load(std::memory_order_relaxed);
    CELER_LOG_INFO << "shutdown requested by signal "
                   << (signal == 0 ? "unknown" : std::to_string(signal));
  }

  if (celer::g_shutdown_requested.load(std::memory_order_acquire) || failed) {
    for (const auto& runtime : runtimes) {
      runtime->BeginShutdown();
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    while (std::chrono::steady_clock::now() < deadline) {
      bool all_accept_loops_done = true;
      for (const auto& runtime : runtimes) {
        if (!runtime->accept_loop_done.load(std::memory_order_acquire) &&
            !runtime->finished.load(std::memory_order_acquire)) {
          all_accept_loops_done = false;
          break;
        }
      }
      if (all_accept_loops_done) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    for (const auto& runtime : runtimes) {
      runtime->worker.RequestStop();
    }
  }
  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& runtime : runtimes) {
    if (runtime->exit_code.load(std::memory_order_acquire) != 0) {
      failed = true;
    }
  }
  return failed ? 1 : 0;
}
