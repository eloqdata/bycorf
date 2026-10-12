/*
 * Copyright (C) 2026 EloqData Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

#include "bycorf/io/io_uring_backend.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/runtime/runtime.h"

namespace {
using namespace std::chrono_literals;
using bycorf::Task;
using bycorf::TcpStream;
int failures = 0;

void Check(bool passed, const char* message) {
  std::cout << (passed ? "PASS " : "FAIL ") << message << '\n';
  if (!passed) ++failures;
}

std::span<const std::byte> Bytes(const std::string& value) {
  return std::as_bytes(std::span(value));
}

// Model AcquireSqe's recursive reap directly, without depending on kernel
// timing to fill both rings. The current CQE must already be retired when a
// completion submits work and reenters polling.
void CheckCompletionReentry(bool wait) {
  bycorf::Worker worker;
  bycorf::IoUringBackend backend;
  bycorf::IoBackendOptions options;
  options.ring_entries_ = 8;
  options.recv_buffer_count_ = 0;
  const auto initialized = backend.Init(options, &worker, -1);
  Check(initialized.ok(), "initialize completion reentry test");
  if (!initialized.ok()) return;
  struct Completion final : bycorf::IoCompletion {
    bycorf::IoUringBackend* backend = nullptr;
    unsigned calls = 0;
    void Complete(bycorf::Worker&, int, unsigned) override {
      if (++calls == 1) backend->Poll();
    }
  } completion;
  completion.backend = &backend;
  __kernel_timespec timeout{0, 1};
  Check(backend.SubmitTimeout(timeout, &completion).ok(),
        "submit reentry timer");
  Check(backend.Submit().ok(), "flush reentry timer");
  if (wait) Check(backend.Wait(1000), "wait dispatches the reentry timer");
  const auto deadline = std::chrono::steady_clock::now() + 1s;
  while (completion.calls == 0 && std::chrono::steady_clock::now() < deadline) {
    // DEFER_TASKRUN needs an enter to publish timer task-work into the CQ.
    (void)backend.Submit();
    backend.Poll();
  }
  Check(completion.calls == 1,
        "recursive polling dispatches each CQE exactly once");
  backend.Shutdown();
}

// A tiny send buffer and a delayed reader force the full-write operations to
// survive multiple completions while retaining their payload and iovec state.
Task<absl::Status> CheckWrite(bycorf::Worker& worker, bool vectored,
                              bool disconnect) {
  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0) {
    Check(false, "socketpair");
    co_return absl::InternalError("socketpair failed");
  }
  const int capacity = 4096;
  Check(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &capacity,
                   sizeof(capacity)) == 0,
        "small send buffer");
  Check(fcntl(sockets[0], F_SETFL, O_NONBLOCK) == 0, "nonblocking sender");
  bycorf::Connection initial;
  initial.worker_ = &worker;
  initial.closed_ = false;
  initial.file_.fd_ = sockets[0];
  auto* connection = worker.AddConnection(std::move(initial));
  TcpStream stream(connection);
  auto borrow = stream.BorrowStorage();

  std::string payload(1024 * 1024, '\0');
  for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = char(i % 251);
  std::string received;
  std::thread reader([&] {
    std::this_thread::sleep_for(20ms);
    std::array<char, 4096> buffer;
    while (received.size() < payload.size()) {
      const auto n = read(sockets[1], buffer.data(), buffer.size());
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) break;
      received.append(buffer.data(), static_cast<std::size_t>(n));
      if (disconnect) break;
    }
    close(sockets[1]);
  });

  absl::Status status;
  if (vectored) {
    // Include empty entries and cuts inside and across vector boundaries.
    std::array<iovec, 5> vectors{
        {{nullptr, 0},
         {payload.data(), 13},
         {nullptr, 0},
         {payload.data() + 13, 65539},
         {payload.data() + 65552, payload.size() - 65552}}};
    const auto original = vectors;
    status = co_await stream.WriteAllV(vectors);
    bool unchanged = true;
    for (std::size_t i = 0; i < vectors.size(); ++i) {
      unchanged &= vectors[i].iov_base == original[i].iov_base &&
                   vectors[i].iov_len == original[i].iov_len;
    }
    Check(unchanged, "WriteAllV preserves caller descriptors");
  } else {
    status = co_await stream.WriteAll(Bytes(payload));
  }
  // Wake the peer even if a regression returns before the payload is complete.
  shutdown(sockets[0], SHUT_WR);
  reader.join();
  Check(disconnect ? !status.ok() : status.ok(),
        disconnect ? "disconnect during full write returns an error"
                   : "full write succeeds under backpressure");
  if (!disconnect)
    Check(received == payload, "short writes preserve every byte");
  Check(!connection->write_inflight_ && connection->inflight_ops_ == 0,
        "full write retires all sends");
  Check(stream.Close().ok(), "close after full write");
  co_return absl::OkStatus();
}

Task<absl::Status> CheckStream(bycorf::Worker& worker) {
  TcpStream unbound;
  std::array<std::byte, 8> buffer{};
  Check((co_await unbound.ReadSome({})).value() == 0, "empty read");
  Check((co_await unbound.WriteSome({})).value() == 0, "empty write");
  Check((co_await unbound.WriteSomeV({})).value() == 0, "empty vector write");
  Check((co_await unbound.WriteAll({})).ok(), "empty full write");
  std::array<iovec, 2> empty_vectors{};
  Check((co_await unbound.WriteAllV(empty_vectors)).ok(),
        "all-empty full vector write");
  Check(!(co_await unbound.ReadSome(buffer)).ok(),
        "unbound read fails immediately");
  Check(!(co_await unbound.WriteAll(buffer)).ok(),
        "unbound full write fails immediately");
  iovec vector{buffer.data(), buffer.size()};
  Check(!(co_await unbound.WriteAllV(std::span(&vector, 1))).ok(),
        "unbound full vector write fails immediately");

  bool immediate_errors = true;
  for (unsigned i = 0; i < 100000; ++i) {
    immediate_errors &= !(co_await unbound.ReadSome(buffer)).ok();
  }
  Check(immediate_errors,
        "repeated immediate completion keeps native stack bounded");

  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                 sockets) == 0) {
    bycorf::Connection initial;
    initial.worker_ = &worker;
    initial.closed_ = false;
    initial.file_.fd_ = sockets[0];
    auto* connection = worker.AddConnection(std::move(initial));
    TcpStream stream(connection);
    auto borrow = stream.BorrowStorage();
    Check(write(sockets[1], "abcdefgh", 8) == 8, "queue received bytes");
    shutdown(sockets[1], SHUT_WR);
    std::size_t read_bytes = 0;
    while (read_bytes < buffer.size()) {
      auto result =
          co_await stream.ReadSome(std::span(buffer).subspan(read_bytes));
      if (!result.ok() || *result == 0) break;
      read_bytes += *result;
    }
    Check(read_bytes == 8 && std::string(reinterpret_cast<char*>(buffer.data()),
                                         8) == "abcdefgh",
          "read resumes with the received bytes");
    auto eof = co_await stream.ReadSome(buffer);
    Check(eof.ok() && *eof == 0, "read reports EOF");
    auto sent = co_await stream.WriteSome(buffer);
    Check(sent.ok() && *sent == buffer.size(),
          "WriteSome reports transferred bytes");
    std::array<char, 8> response{};
    Check(read(sockets[1], response.data(), response.size()) == 8,
          "WriteSome reaches the peer");
    std::array<iovec, 3> vectors{
        {{buffer.data(), 3}, {nullptr, 0}, {buffer.data() + 3, 5}}};
    sent = co_await stream.WriteSomeV(vectors);
    Check(sent.ok() && *sent == buffer.size(),
          "WriteSomeV reports transferred bytes");
    Check(read(sockets[1], response.data(), response.size()) == 8,
          "WriteSomeV reaches the peer");
    Check(stream.Close().ok(), "close read stream");
    Check(!(co_await stream.WriteSome(buffer)).ok(),
          "closed write fails immediately");
    Check(!(co_await stream.ReadSome(buffer)).ok(),
          "closed read fails immediately");
    close(sockets[1]);
  } else {
    Check(false, "read socketpair");
  }
  for (bool vectored : {false, true}) {
    for (bool disconnect : {false, true}) {
      (void)co_await CheckWrite(worker, vectored, disconnect);
    }
  }
  worker.RequestStop();
  co_return absl::OkStatus();
}
}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  CheckCompletionReentry(false);
  CheckCompletionReentry(true);
  bycorf::Runtime runtime;
  runtime.Start(1, [](unsigned, bycorf::Worker& worker) {
    if (!worker.Init().ok()) return 1;
    worker.Spawn(CheckStream(worker));
    worker.Run();
    worker.Shutdown();
    return failures == 0 ? 0 : 1;
  });
  runtime.WaitUntilStopped();
  return runtime.exit_code();
}
