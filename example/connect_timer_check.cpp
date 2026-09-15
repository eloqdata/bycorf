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

// Verification harness for the NuRaft-adapter prerequisite primitives:
//   - celer::ConnectTcp        (IORING_OP_CONNECT + deadline + loser cancel)
//   - celer::CancellableSleepFor (one-shot timer with a thread-safe cancel)
//   - celer::Runtime           (stop requests racing worker startup)
// Runs every check on one celer worker and exits non-zero on any failure.

#include <arpa/inet.h>
#include <sys/socket.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <string>
#include <string_view>
#include <thread>

#include "absl/status/status.h"
#include "celer/io/storage.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/runtime.h"
#include "celer/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace {

using namespace std::chrono_literals;

int g_failures = 0;

void Check(bool condition, std::string_view name) {
  if (condition) {
    spdlog::info("[pass] {}", name);
  } else {
    spdlog::error("[FAIL] {}", name);
    ++g_failures;
  }
}

void CheckStopDuringStartup(bool before_init) {
  celer::Runtime runtime;
  std::promise<absl::Status> parked;
  auto ready = parked.get_future();
  std::promise<void> release;
  auto resume = release.get_future();
  runtime.Start(
      1,
      [&](unsigned, celer::Worker& worker) {
        // Hold startup at a known boundary so RequestStop wins the race on
        // every run, including hosts that normally schedule the worker first.
        if (before_init) {
          parked.set_value(absl::OkStatus());
          resume.wait();
        }
        celer::WorkerOptions options;
        options.recv_buffer_count_ = 64;
        const auto status = worker.Init(options);
        if (!before_init) {
          parked.set_value(status);
          resume.wait();
        }
        if (!status.ok()) return 1;
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      /*pin_workers=*/false);
  const auto initialized = ready.get();
  runtime.RequestStop();
  release.set_value();
  runtime.WaitUntilStopped();
  Check(initialized.ok() && runtime.exit_code() == 0,
        before_init ? "startup stop: before Worker::Init"
                    : "startup stop: after Init, before Run");
}

std::int64_t ElapsedMs(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

// Cancels `handle` after `delay`; runs on the worker, so Cancel takes the
// prompt io_uring_prep_cancel path.
celer::Task<absl::Status> CancelAfterOnWorker(celer::Worker& worker,
                                              celer::TimerCancelHandle handle,
                                              std::chrono::nanoseconds delay) {
  (void)co_await celer::SleepFor(worker, delay);
  handle.Cancel();
  co_return absl::OkStatus();
}

celer::Task<absl::Status> CheckTimerFire(celer::Worker& worker) {
  auto timer = celer::CancellableSleepFor(worker, 100ms);
  const auto started = std::chrono::steady_clock::now();
  const absl::Status fired = co_await timer;
  Check(fired.ok(), "timer fires with OkStatus");
  Check(ElapsedMs(started) >= 90, "timer fire respects the deadline");
  co_return absl::OkStatus();
}

celer::Task<absl::Status> CheckTimerCancelOnWorker(celer::Worker& worker) {
  auto timer = celer::CancellableSleepFor(worker, 5s);
  celer::TimerCancelHandle handle = timer.CancelHandle();
  worker.Spawn(CancelAfterOnWorker(worker, handle, 50ms));
  const auto started = std::chrono::steady_clock::now();
  const absl::Status fired = co_await timer;
  Check(fired.code() == absl::StatusCode::kCancelled,
        "on-worker cancel resolves kCancelled");
  // The pending timeout is pulled off the ring, so the resume is prompt
  // (~50ms), not at the 5s deadline.
  Check(ElapsedMs(started) < 2000, "on-worker cancel resumes promptly");
  co_return absl::OkStatus();
}

celer::Task<absl::Status> CheckTimerCancelForeignThread(celer::Worker& worker) {
  auto timer = celer::CancellableSleepFor(worker, 800ms);
  celer::TimerCancelHandle handle = timer.CancelHandle();
  // NuRaft cancels from its own internal threads: the ring is single-issuer,
  // so only the flag is set and the late fire must resolve as a no-op
  // kCancelled at the original deadline.
  std::thread canceller([handle]() mutable {
    std::this_thread::sleep_for(50ms);
    handle.Cancel();
  });
  const auto started = std::chrono::steady_clock::now();
  const absl::Status fired = co_await timer;
  canceller.join();
  Check(fired.code() == absl::StatusCode::kCancelled,
        "foreign-thread cancel resolves kCancelled");
  Check(ElapsedMs(started) >= 700,
        "foreign-thread cancel: late fire is a no-op at the deadline");
  co_return absl::OkStatus();
}

celer::Task<absl::Status> CheckTimerCancelAfterFire(celer::Worker& worker) {
  auto timer = celer::CancellableSleepFor(worker, 30ms);
  celer::TimerCancelHandle handle = timer.CancelHandle();
  const absl::Status fired = co_await timer;
  Check(fired.ok(), "timer fires before the cancel");
  // Cancel-after-fire and double cancel are safe no-ops on live shared state.
  handle.Cancel();
  handle.Cancel();
  celer::TimerCancelHandle empty;
  empty.Cancel();
  Check(true, "cancel-after-fire and double cancel are safe");
  co_return absl::OkStatus();
}

celer::Task<absl::Status> CheckTimerCancelBeforeAwait(celer::Worker& worker) {
  auto timer = celer::CancellableSleepFor(worker, 5s);
  timer.CancelHandle().Cancel();
  const auto started = std::chrono::steady_clock::now();
  const absl::Status fired = co_await timer;
  Check(fired.code() == absl::StatusCode::kCancelled,
        "cancel-before-await resolves kCancelled");
  Check(ElapsedMs(started) < 100, "cancel-before-await never hits the ring");
  co_return absl::OkStatus();
}

absl::StatusOr<std::uint16_t> BoundPort(const celer::TcpListener& listener) {
  sockaddr_in address{};
  socklen_t length = sizeof(address);
  if (::getsockname(listener.NativeFd(), reinterpret_cast<sockaddr*>(&address),
                    &length) != 0) {
    return absl::Status(absl::StatusCode::kInternal, "getsockname failed");
  }
  return ntohs(address.sin_port);
}

// Connect success against a local listener, including an echo round trip to
// prove the registered Connection works, and cancellation of the pending
// deadline (a stray timeout firing later would close or corrupt the stream).
celer::Task<absl::Status> CheckConnectSuccess(celer::Worker& worker) {
  celer::TcpListener listener;
  absl::Status bound = listener.Bind(&worker, "127.0.0.1", 0);
  if (!bound.ok()) {
    Check(false, "connect-success: listener bind");
    co_return bound;
  }
  auto port = BoundPort(listener);
  if (!port.ok()) {
    Check(false, "connect-success: bound port");
    co_return port.status();
  }

  auto client = co_await celer::ConnectTcp(worker, "127.0.0.1", *port, 2s);
  Check(client.ok(), "connect-success: ConnectTcp to local listener");
  if (!client.ok()) {
    co_return client.status();
  }
  auto accepted = co_await listener.Accept();
  Check(accepted.ok(), "connect-success: listener accepts");
  if (!accepted.ok()) {
    co_return accepted.status();
  }
  celer::TcpStream server(*accepted);

  constexpr std::string_view kPing = "ping";
  absl::Status written = co_await client->WriteAll(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(kPing.data()), kPing.size()));
  Check(written.ok(), "connect-success: client write");

  std::array<std::byte, 16> buffer{};
  auto server_read = co_await server.ReadSome(buffer);
  Check(server_read.ok() && *server_read == kPing.size() &&
            std::memcmp(buffer.data(), kPing.data(), kPing.size()) == 0,
        "connect-success: server read");
  if (server_read.ok()) {
    absl::Status echoed = co_await server.WriteAll(
        std::span<const std::byte>(buffer.data(), *server_read));
    Check(echoed.ok(), "connect-success: server echo");
  }
  auto client_read = co_await client->ReadSome(buffer);
  Check(client_read.ok() && *client_read == kPing.size() &&
            std::memcmp(buffer.data(), kPing.data(), kPing.size()) == 0,
        "connect-success: client read echo");

  auto peer = client->PeerAddress();
  Check(peer.ok() && *peer == "127.0.0.1:" + std::to_string(*port),
        "connect-success: peer address");

  server.Close().IgnoreError();
  client->Close().IgnoreError();
  listener.Close().IgnoreError();
  co_return absl::OkStatus();
}

// ECONNREFUSED: deterministic immediate failure — bind an ephemeral port,
// close the listener, then connect to it. Verifies failure cleanup (fd
// closed, both CQEs consumed, error mapped).
celer::Task<absl::Status> CheckConnectRefused(celer::Worker& worker) {
  celer::TcpListener listener;
  absl::Status bound = listener.Bind(&worker, "127.0.0.1", 0);
  if (!bound.ok()) {
    Check(false, "connect-refused: listener bind");
    co_return bound;
  }
  auto port = BoundPort(listener);
  listener.Close().IgnoreError();
  if (!port.ok()) {
    Check(false, "connect-refused: bound port");
    co_return port.status();
  }
  const auto started = std::chrono::steady_clock::now();
  auto client = co_await celer::ConnectTcp(worker, "127.0.0.1", *port, 5s);
  Check(
      !client.ok() && client.status().code() == absl::StatusCode::kUnavailable,
      "connect-refused: ECONNREFUSED maps to kUnavailable");
  Check(ElapsedMs(started) < 2000, "connect-refused: fails fast");
  co_return absl::OkStatus();
}

// Deadline path: 10.255.255.1 is unreachable-but-routable here (default route
// exists, nobody answers), so the connect hangs until the 300ms deadline wins
// and cancels it.
celer::Task<absl::Status> CheckConnectTimeout(celer::Worker& worker) {
  const auto started = std::chrono::steady_clock::now();
  auto client =
      co_await celer::ConnectTcp(worker, "10.255.255.1", 17699, 300ms);
  const std::int64_t elapsed = ElapsedMs(started);
  if (!client.ok() &&
      client.status().code() == absl::StatusCode::kUnavailable) {
    // An environment without a route to 10/8 fails immediately with
    // ENETUNREACH instead of hanging; the timeout path is then not exercised.
    spdlog::warn(
        "[warn] connect-timeout: immediate unreachable after {}ms (no route "
        "to 10/8 in this environment)",
        elapsed);
    Check(true, "connect-timeout: immediate unreachable (see warning)");
    co_return absl::OkStatus();
  }
  Check(!client.ok() &&
            client.status().code() == absl::StatusCode::kDeadlineExceeded,
        "connect-timeout: resolves kDeadlineExceeded");
  Check(elapsed >= 280 && elapsed < 10000,
        "connect-timeout: deadline bounds the wait");
  co_return absl::OkStatus();
}

celer::Task<absl::Status> CheckConnectRejectsHostname(celer::Worker& worker) {
  auto client = co_await celer::ConnectTcp(worker, "localhost", 17699, 100ms);
  Check(!client.ok() &&
            client.status().code() == absl::StatusCode::kInvalidArgument,
        "connect: non-numeric host is rejected without DNS");
  co_return absl::OkStatus();
}

// Regression: a connect that resolves long before its deadline must retire the
// pending timeout before resuming. Otherwise the deadline CQE later dispatches
// into the destroyed ConnectTcp frame (use-after-free). Every loop iteration
// builds and destroys a same-size ConnectTcp frame, so the coroutine frame
// pool recycles the previous one and scribbles over any stale timeout tag; the
// trailing sleep then keeps the worker alive past every armed deadline. A
// stale dispatch crashes here (garbage vtable); the fixed code leaves nothing
// pending and the sleep is silent.
celer::Task<absl::Status> CheckConnectFastFailDeadlineRetired(
    celer::Worker& worker) {
  celer::TcpListener listener;
  absl::Status bound = listener.Bind(&worker, "127.0.0.1", 0);
  if (!bound.ok()) {
    Check(false, "deadline-retired(fail): listener bind");
    co_return bound;
  }
  auto port = BoundPort(listener);
  listener.Close().IgnoreError();
  if (!port.ok()) {
    Check(false, "deadline-retired(fail): bound port");
    co_return port.status();
  }

  constexpr int kAttempts = 64;
  const auto started = std::chrono::steady_clock::now();
  for (int attempt = 0; attempt < kAttempts; ++attempt) {
    auto client = co_await celer::ConnectTcp(worker, "127.0.0.1", *port, 1s);
    if (client.ok() ||
        client.status().code() != absl::StatusCode::kUnavailable) {
      Check(false,
            "deadline-retired(fail): refused connect maps to "
            "kUnavailable");
      co_return absl::OkStatus();
    }
  }
  Check(ElapsedMs(started) < 2000,
        "deadline-retired(fail): refused loop stays fast");
  (void)co_await celer::SleepFor(worker, 1500ms);
  Check(true,
        "deadline-retired(fail): no late timeout dispatch after teardown");
  co_return absl::OkStatus();
}

// Success twin of the check above: the connect wins instantly against a live
// listener, so the loser-cancel must pull the 1s deadline off the ring before
// the frame is destroyed.
celer::Task<absl::Status> CheckConnectFastSuccessDeadlineRetired(
    celer::Worker& worker) {
  celer::TcpListener listener;
  absl::Status bound = listener.Bind(&worker, "127.0.0.1", 0);
  if (!bound.ok()) {
    Check(false, "deadline-retired(ok): listener bind");
    co_return bound;
  }
  auto port = BoundPort(listener);
  if (!port.ok()) {
    Check(false, "deadline-retired(ok): bound port");
    co_return port.status();
  }

  constexpr int kAttempts = 8;
  const auto started = std::chrono::steady_clock::now();
  for (int attempt = 0; attempt < kAttempts; ++attempt) {
    // The kernel completes the handshake against the listen backlog; no
    // userspace accept is needed for the connect to win instantly.
    auto client = co_await celer::ConnectTcp(worker, "127.0.0.1", *port, 1s);
    if (!client.ok()) {
      Check(false, "deadline-retired(ok): connect succeeds");
      co_return client.status();
    }
    client->Close().IgnoreError();
  }
  Check(ElapsedMs(started) < 2000,
        "deadline-retired(ok): connect loop stays fast");
  (void)co_await celer::SleepFor(worker, 1500ms);
  Check(true, "deadline-retired(ok): no late timeout dispatch after teardown");

  for (int attempt = 0; attempt < kAttempts; ++attempt) {
    auto accepted = co_await listener.Accept();
    if (!accepted.ok()) {
      Check(false, "deadline-retired(ok): drain accepted sockets");
      break;
    }
    celer::TcpStream server(*accepted);
    server.Close().IgnoreError();
  }
  listener.Close().IgnoreError();
  co_return absl::OkStatus();
}

celer::Task<absl::Status> RunAllChecks(celer::Worker& worker) {
  co_await CheckTimerFire(worker);
  co_await CheckTimerCancelOnWorker(worker);
  co_await CheckTimerCancelForeignThread(worker);
  co_await CheckTimerCancelAfterFire(worker);
  co_await CheckTimerCancelBeforeAwait(worker);
  co_await CheckConnectSuccess(worker);
  co_await CheckConnectRefused(worker);
  co_await CheckConnectTimeout(worker);
  co_await CheckConnectRejectsHostname(worker);
  co_await CheckConnectFastFailDeadlineRetired(worker);
  co_await CheckConnectFastSuccessDeadlineRetired(worker);
  spdlog::info("checks complete: {} failure(s)", g_failures);
  worker.RequestStop();
  co_return absl::OkStatus();
}

}  // namespace

int main() {
  CheckStopDuringStartup(/*before_init=*/true);
  CheckStopDuringStartup(/*before_init=*/false);
  celer::Runtime runtime;
  runtime.Start(
      1,
      [](unsigned, celer::Worker& worker) -> int {
        celer::WorkerOptions options;
        options.recv_buffer_count_ = 64;
        if (!worker.Init(options).ok()) {
          return 1;
        }
        worker.Spawn(RunAllChecks(worker));
        worker.Run();
        return g_failures == 0 ? 0 : 1;
      },
      /*pin_workers=*/false);
  runtime.WaitUntilStopped();
  return runtime.exit_code();
}
