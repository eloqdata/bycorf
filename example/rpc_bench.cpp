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

// Closed-loop RPC echo benchmark for celer::rpc.
//   rpc_bench [server_ip] [port] [threads] [conns_per_thread]
//   [concurrency_per_conn]
//             [payload_bytes] [duration_sec]
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "celer/rpc/rpc.h"
#include "celer/runtime/runtime.h"
#include "celer/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace {

std::string g_ip = "127.0.0.1";
std::uint16_t g_port = 9000;
unsigned g_conns = 1;
unsigned g_concurrency = 1;
std::size_t g_payload = 64;
std::atomic<bool> g_stop{false};

struct alignas(64) Stat {
  std::uint64_t calls = 0;
  std::uint64_t latency_ns = 0;
};
std::vector<Stat> g_stats;

using namespace celer;

Task<absl::Status> Caller(rpc::RpcClient* client, unsigned wid) {
  rpc::Bytes payload(g_payload, std::byte{'x'});
  while (!g_stop.load(std::memory_order_acquire)) {
    const auto t0 = std::chrono::steady_clock::now();
    auto reply = co_await client->Call(
        1, rpc::BytesView(payload.data(), payload.size()));
    if (!reply.ok()) [[unlikely]] {
      break;
    }
    const auto dt = std::chrono::steady_clock::now() - t0;
    g_stats[wid].calls += 1;
    g_stats[wid].latency_ns += static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count());
  }
  co_return absl::OkStatus();
}

Task<absl::Status> Setup(Worker& worker) {
  const unsigned wid = worker.id();
  for (unsigned c = 0; c < g_conns; ++c) {
    auto* client =
        new rpc::RpcClient();  // leaked for the lifetime of the bench
    auto status = co_await client->Connect(g_ip, g_port);
    if (!status.ok()) [[unlikely]] {
      spdlog::error("worker[{}] connect failed: {}", wid, status.message());
      continue;
    }
    for (unsigned k = 0; k < g_concurrency; ++k) {
      worker.Spawn(Caller(client, wid));
    }
  }
  co_return absl::OkStatus();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc >= 2) g_ip = argv[1];
  if (argc >= 3) g_port = static_cast<std::uint16_t>(std::stoi(argv[2]));
  unsigned threads =
      (argc >= 4) ? static_cast<unsigned>(std::stoul(argv[3])) : 1;
  if (argc >= 5) g_conns = static_cast<unsigned>(std::stoul(argv[4]));
  if (argc >= 6) g_concurrency = static_cast<unsigned>(std::stoul(argv[5]));
  if (argc >= 7) g_payload = static_cast<std::size_t>(std::stoul(argv[6]));
  unsigned duration_sec =
      (argc >= 8) ? static_cast<unsigned>(std::stoul(argv[7])) : 5;

  g_stats.assign(threads, Stat{});

  spdlog::info(
      "rpc_bench -> {}:{} threads={} conns/thread={} concurrency/conn={} "
      "payload={} dur={}s",
      g_ip, g_port, threads, g_conns, g_concurrency, g_payload, duration_sec);

  Runtime runtime;
  runtime.Start(threads, [](unsigned, Worker& worker) -> int {
    WorkerOptions options;
    if (!worker.Init(options).ok()) return 1;
    worker.Spawn(Setup(worker));
    worker.Run();
    return 0;
  });

  const auto t_start = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
  g_stop.store(true, std::memory_order_release);
  const auto elapsed = std::chrono::steady_clock::now() - t_start;

  runtime.RequestStop();
  runtime.WaitUntilStopped();

  std::uint64_t total_calls = 0;
  std::uint64_t total_latency = 0;
  for (const auto& s : g_stats) {
    total_calls += s.calls;
    total_latency += s.latency_ns;
  }
  const double secs = std::chrono::duration<double>(elapsed).count();
  const double qps = (secs > 0) ? total_calls / secs : 0;
  const double avg_us =
      (total_calls > 0)
          ? (static_cast<double>(total_latency) / total_calls) / 1000.0
          : 0;

  spdlog::info("calls={} elapsed={:.2f}s  QPS={:.0f}  avg_latency={:.2f}us",
               total_calls, secs, qps, avg_us);
  return 0;
}
