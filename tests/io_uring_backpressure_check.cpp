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

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>

#include "bycorf/io/io_uring_backend.h"
#include "bycorf/runtime/worker.h"

namespace {

#define CHECK(condition)                                                \
  do {                                                                  \
    if (!(condition)) {                                                 \
      std::cerr << "FAIL line " << __LINE__ << ": " #condition << '\n'; \
      std::exit(1);                                                     \
    }                                                                   \
  } while (false)

struct Completion final : bycorf::IoCompletion {
  unsigned calls = 0;
  int result = 0;

  void Complete(bycorf::Worker&, int value, unsigned flags) override {
    CHECK(++calls == 1);
    CHECK(flags == bycorf::kCompletionNone);
    result = value;
  }
};

struct Fixture {
  Fixture() {
    const auto status = backend.Init(
        {.ring_entries_ = 2, .recv_buffer_count_ = 0}, &worker, -1);
    if (!status.ok()) std::cerr << status << '\n';
    CHECK(status.ok());
  }

  bycorf::Worker worker;
  bycorf::IoUringBackend backend;
};

template <typename Predicate>
void Drain(bycorf::IoUringBackend& backend, Predicate done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!done()) {
    CHECK(std::chrono::steady_clock::now() < deadline);
    const auto status = backend.Submit();
    CHECK(status.ok() || absl::IsUnavailable(status));
    backend.Poll();
  }
}

void CheckSendsAndWake() {
  Fixture source, peer;
  int sockets[2];
  CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                   sockets) == 0);
  constexpr std::size_t kCount = 64;
  std::array<Completion, kCount> tags;
  std::array<std::byte, kCount> payload;
  for (std::size_t i = 0; i < kCount; ++i) {
    payload[i] = std::byte(i);
    CHECK(source.backend
              .SubmitSend({.fd_ = sockets[0]}, std::span(payload).subspan(i, 1),
                          &tags[i])
              .ok());
  }
  source.backend.WakeRemote(peer.backend.WakeHandle());
  for (const auto& tag : tags) CHECK(tag.calls == 0);

  // A completion-only check misses hidden io_uring_enter calls that free SQ
  // slots without reaping. The peer must see no data before the event loop
  // explicitly submits this burst, even though it exceeds the ring capacity.
  std::array<std::byte, kCount> received;
  CHECK(recv(sockets[1], received.data(), received.size(), MSG_DONTWAIT) == -1);
  CHECK(errno == EAGAIN || errno == EWOULDBLOCK);
  CHECK(peer.backend.Submit().ok());
  CHECK(!peer.backend.Poll());

  bool peer_woken = false;
  Drain(source.backend, [&] {
    peer_woken |= peer.backend.Poll();
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return peer_woken;
  });
  for (const auto& tag : tags) CHECK(tag.result == 1);
  CHECK(recv(sockets[1], received.data(), received.size(), MSG_DONTWAIT) ==
        static_cast<ssize_t>(received.size()));
  CHECK(received == payload);
  CHECK(close(sockets[0]) == 0);
  CHECK(close(sockets[1]) == 0);
  std::cout << "PASS deferred sends, FIFO payloads and queued peer wake\n";
}

void CheckQueuedCancellation() {
  Fixture fixture;
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK(event >= 0);
  std::array<Completion, 16> tags;
  for (std::size_t i = 0; i + 1 < tags.size(); ++i)
    CHECK(fixture.backend.SubmitPoll(event, POLLIN, &tags[i]).ok());
  const __kernel_timespec timeout{.tv_sec = 60, .tv_nsec = 0};
  CHECK(fixture.backend.SubmitTimeout(timeout, &tags.back()).ok());
  // Both the target and its cancel are behind a full SQ. Cancellation must
  // still deliver the target's terminal completion before its tag is released.
  CHECK(fixture.backend.SubmitCancel(&tags.back()).ok());
  for (const auto& tag : tags) CHECK(tag.calls == 0);
  Drain(fixture.backend, [&] { return tags.back().calls != 0; });
  CHECK(tags.back().result == -ECANCELED);
  for (std::size_t i = 0; i + 1 < tags.size(); ++i) CHECK(tags[i].calls == 0);

  const std::uint64_t one = 1;
  CHECK(write(event, &one, sizeof(one)) == sizeof(one));
  Drain(fixture.backend, [&] {
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return true;
  });
  for (std::size_t i = 0; i + 1 < tags.size(); ++i)
    CHECK((tags[i].result & POLLIN) != 0);
  CHECK(fixture.backend.Submit().ok());
  fixture.backend.Poll();
  CHECK(tags.back().calls == 1);
  CHECK(close(event) == 0);
  std::cout << "PASS queued cancellation and exactly-once completions\n";
}

void CheckClosedDescriptorReuse() {
  Fixture fixture;
  int original[2], replacement[2];
  CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                   original) == 0);
  CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                   replacement) == 0);
  const std::array<std::byte, 1> payload{std::byte{0x5a}};
  std::array<Completion, 16> tags;
  // Initialization occupies one SQ slot; the first send is in the real SQ,
  // while later sends are software-queued. Neither may outlive this fd's
  // identity, and cancellation callbacks must remain deferred.
  for (auto& tag : tags)
    CHECK(fixture.backend.SubmitSend({.fd_ = original[0]}, payload, &tag).ok());
  fixture.backend.CancelUnsubmittedForFd(original[0]);
  for (const auto& tag : tags) CHECK(tag.calls == 0);
  CHECK(close(original[0]) == 0);
  CHECK(dup2(replacement[0], original[0]) == original[0]);
  CHECK(close(replacement[0]) == 0);

  Drain(fixture.backend, [&] {
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return true;
  });
  for (const auto& tag : tags) CHECK(tag.result == -ECANCELED);
  std::byte received{};
  CHECK(recv(original[1], &received, 1, MSG_DONTWAIT) == 0);
  CHECK(recv(replacement[1], &received, 1, MSG_DONTWAIT) == -1);
  CHECK(errno == EAGAIN || errno == EWOULDBLOCK);

  Completion fresh;
  CHECK(fixture.backend.SubmitSend({.fd_ = original[0]}, payload, &fresh).ok());
  Drain(fixture.backend, [&] { return fresh.calls != 0; });
  CHECK(fresh.result == 1);
  CHECK(recv(replacement[1], &received, 1, MSG_DONTWAIT) == 1);
  CHECK(received == payload.front());
  CHECK(close(original[0]) == 0);
  CHECK(close(original[1]) == 0);
  CHECK(close(replacement[1]) == 0);
  std::cout << "PASS cancellation before close protects reused descriptors\n";
}

void CheckQueuedReceiveCancellation() {
  Fixture fixture;
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK(event >= 0);
  std::array<Completion, 8> blockers;
  for (auto& tag : blockers)
    CHECK(fixture.backend.SubmitPoll(event, POLLIN, &tag).ok());

  int sockets[2];
  CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                   sockets) == 0);
  bycorf::Connection connection;
  connection.worker_ = &fixture.worker;
  connection.file_.fd_ = sockets[0];
  connection.closed_ = false;
  CHECK(send(sockets[1], "x", 1, MSG_NOSIGNAL) == 1);
  CHECK(fixture.backend.StartRecvMultishot(&connection).ok());
  CHECK(connection.recv_armed_ && connection.inflight_ops_ == 1);
  connection.recv_paused_ = true;
  Completion cancelled;
  CHECK(fixture.backend.SubmitCancelRecv(&connection, &cancelled).ok());
  CHECK(cancelled.calls == 0);
  CHECK(connection.recv_armed_ && connection.inflight_ops_ == 1);
  Drain(fixture.backend, [&] { return cancelled.calls != 0; });
  CHECK(cancelled.result == 0);
  CHECK(!connection.recv_armed_ && connection.inflight_ops_ == 0);
  CHECK(connection.received_buffers_.empty());
  char received = 0;
  CHECK(recv(sockets[0], &received, 1, MSG_DONTWAIT) == 1);
  CHECK(received == 'x');

  const std::uint64_t one = 1;
  CHECK(write(event, &one, sizeof(one)) == sizeof(one));
  Drain(fixture.backend, [&] {
    for (const auto& tag : blockers)
      if (tag.calls == 0) return false;
    return true;
  });
  CHECK(close(event) == 0);
  CHECK(close(sockets[0]) == 0);
  CHECK(close(sockets[1]) == 0);
  std::cout << "PASS queued receive cancellation drains connection ownership\n";
}

void CheckLateCancelBeforeTagReuse() {
  Fixture fixture;
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK(event >= 0);
  int sockets[2];
  CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                   sockets) == 0);
  bycorf::Connection connection;
  connection.worker_ = &fixture.worker;
  connection.file_.fd_ = sockets[0];
  connection.closed_ = false;
  CHECK(fixture.backend.StartRecvMultishot(&connection).ok());
  CHECK(fixture.backend.Submit().ok());
  CHECK(!fixture.backend.HasPending());

  std::array<Completion, 8> blockers;
  for (auto& tag : blockers)
    CHECK(fixture.backend.SubmitPoll(event, POLLIN, &tag).ok());
  Completion cancelled;
  CHECK(fixture.backend.SubmitCancelRecv(&connection, &cancelled).ok());
  CHECK(cancelled.calls == 0);
  CHECK(send(sockets[1], "x", 1, MSG_NOSIGNAL) == 1);
  // Run deferred kernel completion work without publishing or submitting the
  // full SQ/backlog. The receive therefore finishes naturally while its cancel
  // is still wholly owned by the backend's submission queue.
  alarm(2);
  CHECK(io_uring_enter(fixture.backend.WakeHandle(), 0, 1,
                       IORING_ENTER_GETEVENTS, nullptr) >= 0);
  alarm(0);
  CHECK(fixture.backend.Poll());
  CHECK(!connection.recv_armed_ && connection.inflight_ops_ == 0);
  CHECK(connection.received_buffers_.size() == 1);
  CHECK(connection.read_buffer_.front() == std::byte{'x'});
  // This must be delivered by the same Poll, before any queued cancel can
  // enter the kernel. Merely checking the next read would allow FIFO submission
  // to hide a stale cancel that happens to run before the reused tag is armed.
  CHECK(cancelled.calls == 1 && cancelled.result == -ENOENT);

  connection.received_buffers_.clear();
  CHECK(fixture.backend.StartRecvMultishot(&connection).ok());
  CHECK(connection.recv_armed_ && connection.inflight_ops_ == 1);
  CHECK(send(sockets[1], "y", 1, MSG_NOSIGNAL) == 1);
  const std::uint64_t one = 1;
  CHECK(write(event, &one, sizeof(one)) == sizeof(one));
  Drain(fixture.backend, [&] {
    for (const auto& tag : blockers)
      if (tag.calls == 0) return false;
    return !connection.received_buffers_.empty();
  });
  CHECK(cancelled.calls == 1);
  CHECK(!connection.recv_armed_ && connection.inflight_ops_ == 0);
  CHECK(connection.read_buffer_.front() == std::byte{'y'});
  CHECK(close(event) == 0);
  CHECK(close(sockets[0]) == 0);
  CHECK(close(sockets[1]) == 0);
  std::cout
      << "PASS natural completion retires queued cancel before tag reuse\n";
}

void CheckPendingWaitAndShutdown() {
  Fixture fixture;
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK(event >= 0);
  {
    std::array<Completion, 8> tags;
    for (auto& tag : tags)
      CHECK(fixture.backend.SubmitPoll(event, POLLIN, &tag).ok());
    CHECK(fixture.backend.SubmitCancel(&tags.back()).ok());
    CHECK(fixture.backend.HasPending());
    // None of these polls can complete. Blocking here would strand both the
    // unsubmitted work and the software cancellation completion behind it.
    alarm(2);
    CHECK(fixture.backend.Wait(-1));
    alarm(0);
    for (const auto& tag : tags) CHECK(tag.calls == 0);
    fixture.backend.Shutdown();
    CHECK(!fixture.backend.HasPending());
    CHECK(!fixture.backend.Poll());
  }
  CHECK(close(event) == 0);

  // Old tags are now destroyed. Reusing the backend must not issue abandoned
  // SQEs or dispatch their synthetic completions into the old caller's state.
  CHECK(fixture.backend
            .Init({.ring_entries_ = 2, .recv_buffer_count_ = 0},
                  &fixture.worker, -1)
            .ok());
  Completion fresh;
  const __kernel_timespec timeout{.tv_sec = 0, .tv_nsec = 1};
  CHECK(fixture.backend.SubmitTimeout(timeout, &fresh).ok());
  Drain(fixture.backend, [&] { return fresh.calls != 0; });
  CHECK(fresh.result == -ETIME);
  std::cout << "PASS pending work prevents park and shutdown drops old state\n";
}

void CheckStorageReads() {
  Fixture fixture;
  char path[] = "/tmp/bycorf-uring-backpressure-XXXXXX";
  const int file = mkstemp(path);
  CHECK(file >= 0);
  std::array<std::byte, 64> payload;
  for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = std::byte(i);
  CHECK(write(file, payload.data(), payload.size()) ==
        static_cast<ssize_t>(payload.size()));
  CHECK(fixture.backend.RegisterFixedFiles(1).ok());
  Completion opened;
  CHECK(fixture.backend.SubmitOpenDirect(path, O_RDONLY, 0, {0}, &opened).ok());
  Drain(fixture.backend, [&] { return opened.calls != 0; });
  CHECK(opened.result == 0);
  CHECK(unlink(path) == 0);
  CHECK(close(file) == 0);

  std::array<Completion, 16> tags;
  std::array<std::byte, 64> received{};
  const iovec region{.iov_base = received.data(), .iov_len = received.size()};
  CHECK(fixture.backend.RegisterBuffers(std::span(&region, 1)).ok());
  for (std::size_t i = 0; i < tags.size(); ++i) {
    const auto buffer = std::span(received).subspan(i * 4, 4);
    CHECK((i % 2 == 0
               ? fixture.backend.SubmitRead({0}, buffer, i * 4, &tags[i])
               : fixture.backend.SubmitReadFixed(
                     {0}, {.data_ = buffer.data(), .size_ = buffer.size()},
                     i * 4, &tags[i]))
              .ok());
  }
  for (const auto& tag : tags) CHECK(tag.calls == 0);
  Drain(fixture.backend, [&] {
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return true;
  });
  for (const auto& tag : tags) CHECK(tag.result == 4);
  CHECK(received == payload);
  Completion closed;
  CHECK(fixture.backend.SubmitCloseDirect({0}, &closed).ok());
  Drain(fixture.backend, [&] { return closed.calls != 0; });
  CHECK(closed.result == 0);
  std::cout << "PASS queued fixed-file reads preserve buffers and offsets\n";
}

}  // namespace

int main() {
  CheckSendsAndWake();
  CheckQueuedCancellation();
  CheckClosedDescriptorReuse();
  CheckQueuedReceiveCancellation();
  CheckLateCancelBeforeTagReuse();
  CheckPendingWaitAndShutdown();
  CheckStorageReads();
  return 0;
}
