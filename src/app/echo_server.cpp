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
#include <cstdlib>
#include <cstdint>
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

Task<Status> AcceptLoop(Worker& worker, TcpListener& listener) {
  while (true) {
    auto accepted = co_await listener.Accept();
    if (!accepted.ok()) {
      if (accepted.status().code() != StatusCode::kUnavailable) {
        CELER_LOG_WARN << "accept failed: " << accepted.status().message();
      }
      continue;
    }

    worker.Spawn(EchoSession(worker, *accepted));
  }
}

void RunEchoWorker(std::string bind_ip, std::uint16_t port, RecvMode recv_mode,
                   int idle_timeout_ms, bool reuse_port, unsigned worker_index) {
  Worker worker;
  WorkerOptions worker_options;
  worker_options.recv_mode = recv_mode;
  worker_options.idle_timeout_ms = idle_timeout_ms;

  auto init_status = worker.Init(worker_options);
  if (!init_status.ok()) {
    CELER_LOG_ERROR << "worker[" << worker_index
                      << "] init failed: " << init_status.message();
    std::exit(1);
  }

  TcpListener listener;
  auto bind_status = listener.Bind(&worker, bind_ip, port, 128, reuse_port);
  if (!bind_status.ok()) {
    CELER_LOG_ERROR << "worker[" << worker_index
                      << "] bind failed: " << bind_status.message();
    std::exit(1);
  }

  worker.Spawn(AcceptLoop(worker, listener));
  worker.Run();
}

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
  std::vector<std::thread> threads;
  threads.reserve(thread_count);
  for (unsigned i = 0; i < thread_count; ++i) {
    threads.emplace_back(celer::RunEchoWorker, std::string(bind_ip), port, recv_mode,
                         idle_timeout_ms, reuse_port, i);
  }
  for (auto& thread : threads) {
    thread.join();
  }
  return 0;
}
