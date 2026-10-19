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
#include <gtest/gtest.h>
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

struct Completion final : bycorf::IoCompletion {
  unsigned calls = 0;
  int result = 0;

  void Complete(bycorf::Worker&, int value, unsigned flags) override {
    ASSERT_TRUE(++calls == 1);
    ASSERT_TRUE(flags == bycorf::kCompletionNone);
    result = value;
  }
};

struct Fixture {
  void Init() {
    const auto status = backend.Init(
        {.ring_entries_ = 2, .recv_buffer_count_ = 0}, &worker, -1);
    if (!status.ok()) std::cerr << status << '\n';
    ASSERT_TRUE(status.ok());
  }

  bycorf::Worker worker;
  bycorf::IoUringBackend backend;
};

template <typename Predicate>
void Drain(bycorf::IoUringBackend& backend, Predicate done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!done()) {
    ASSERT_TRUE(std::chrono::steady_clock::now() < deadline);
    const auto status = backend.Submit();
    ASSERT_TRUE(status.ok() || absl::IsUnavailable(status));
    backend.Poll();
  }
}

TEST(IoUringBackpressure, SendsAndWake) {
  Fixture source, peer;
  ASSERT_NO_FATAL_FAILURE(source.Init());
  ASSERT_NO_FATAL_FAILURE(peer.Init());
  int sockets[2];
  ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                         sockets) == 0);
  constexpr std::size_t kCount = 64;
  std::array<Completion, kCount> tags;
  std::array<std::byte, kCount> payload;
  std::array<iovec, kCount> vectors;
  std::array<msghdr, kCount> messages{};
  // Submit the initial wake poll so both SQ slots are available to the burst.
  ASSERT_TRUE(source.backend.Submit().ok());
  for (std::size_t i = 0; i < kCount; ++i) {
    payload[i] = std::byte(i);
    vectors[i] = {.iov_base = &payload[i], .iov_len = 1};
    messages[i].msg_iov = &vectors[i];
    messages[i].msg_iovlen = 1;
    // Exercise both preparation callbacks in the real SQ and the pending FIFO.
    // Indirect sendmsg arguments remain caller-owned through completion.
    ASSERT_TRUE((i % 2 == 0 ? source.backend.SubmitSend(
                                  {.fd_ = sockets[0]},
                                  std::span(payload).subspan(i, 1), &tags[i])
                            : source.backend.SubmitSendMsg(
                                  {.fd_ = sockets[0]}, &messages[i], &tags[i]))
                    .ok());
  }
  source.backend.WakeRemote(peer.backend.WakeHandle());
  for (const auto& tag : tags) ASSERT_TRUE(tag.calls == 0);

  // A completion-only check misses hidden io_uring_enter calls that free SQ
  // slots without reaping. The peer must see no data before the event loop
  // explicitly submits this burst, even though it exceeds the ring capacity.
  std::array<std::byte, kCount> received;
  ASSERT_TRUE(
      recv(sockets[1], received.data(), received.size(), MSG_DONTWAIT) == -1);
  ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
  ASSERT_TRUE(peer.backend.Submit().ok());
  ASSERT_TRUE(!peer.backend.Poll());

  bool peer_woken = false;
  Drain(source.backend, [&] {
    // DEFER_TASKRUN can defer MSG_RING delivery until the target ring enters
    // the kernel, even when both rings share a thread. Drive the peer as its
    // worker would: Submit runs deferred task work; Poll consumes ready CQEs.
    const auto status = peer.backend.Submit();
    EXPECT_TRUE(status.ok() || absl::IsUnavailable(status)) << status;
    peer_woken |= peer.backend.Poll();
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return peer_woken;
  });
  for (const auto& tag : tags) ASSERT_TRUE(tag.result == 1);
  ASSERT_TRUE(recv(sockets[1], received.data(), received.size(),
                   MSG_DONTWAIT) == static_cast<ssize_t>(received.size()));
  ASSERT_TRUE(received == payload);
  ASSERT_TRUE(close(sockets[0]) == 0);
  ASSERT_TRUE(close(sockets[1]) == 0);
  std::cout << "PASS deferred sends, FIFO payloads and queued peer wake\n";
}

TEST(IoUringBackpressure, QueuedCancellation) {
  Fixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.Init());
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  ASSERT_TRUE(event >= 0);
  std::array<Completion, 16> tags;
  for (std::size_t i = 0; i + 1 < tags.size(); ++i)
    ASSERT_TRUE(fixture.backend.SubmitPoll(event, POLLIN, &tags[i]).ok());
  const __kernel_timespec timeout{.tv_sec = 60, .tv_nsec = 0};
  ASSERT_TRUE(fixture.backend.SubmitTimeout(timeout, &tags.back()).ok());
  // Both the target and its cancel are behind a full SQ. Cancellation must
  // still deliver the target's terminal completion before its tag is released.
  ASSERT_TRUE(fixture.backend.SubmitCancel(&tags.back()).ok());
  for (const auto& tag : tags) ASSERT_TRUE(tag.calls == 0);
  Drain(fixture.backend, [&] { return tags.back().calls != 0; });
  ASSERT_TRUE(tags.back().result == -ECANCELED);
  for (std::size_t i = 0; i + 1 < tags.size(); ++i)
    ASSERT_TRUE(tags[i].calls == 0);

  const std::uint64_t one = 1;
  ASSERT_TRUE(write(event, &one, sizeof(one)) == sizeof(one));
  Drain(fixture.backend, [&] {
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return true;
  });
  for (std::size_t i = 0; i + 1 < tags.size(); ++i)
    ASSERT_TRUE((tags[i].result & POLLIN) != 0);
  ASSERT_TRUE(fixture.backend.Submit().ok());
  fixture.backend.Poll();
  ASSERT_TRUE(tags.back().calls == 1);
  ASSERT_TRUE(close(event) == 0);
  std::cout << "PASS queued cancellation and exactly-once completions\n";
}

TEST(IoUringBackpressure, ClosedDescriptorReuse) {
  Fixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.Init());
  int original[2], replacement[2];
  ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                         original) == 0);
  ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                         replacement) == 0);
  const std::array<std::byte, 1> payload{std::byte{0x5a}};
  std::array<Completion, 16> tags;
  // Initialization occupies one SQ slot; the first send is in the real SQ,
  // while later sends are software-queued. Neither may outlive this fd's
  // identity, and cancellation callbacks must remain deferred.
  for (auto& tag : tags)
    ASSERT_TRUE(
        fixture.backend.SubmitSend({.fd_ = original[0]}, payload, &tag).ok());
  fixture.backend.CancelUnsubmittedForFd(original[0]);
  for (const auto& tag : tags) ASSERT_TRUE(tag.calls == 0);
  ASSERT_TRUE(close(original[0]) == 0);
  ASSERT_TRUE(dup2(replacement[0], original[0]) == original[0]);
  ASSERT_TRUE(close(replacement[0]) == 0);

  Drain(fixture.backend, [&] {
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return true;
  });
  for (const auto& tag : tags) ASSERT_TRUE(tag.result == -ECANCELED);
  std::byte received{};
  ASSERT_TRUE(recv(original[1], &received, 1, MSG_DONTWAIT) == 0);
  ASSERT_TRUE(recv(replacement[1], &received, 1, MSG_DONTWAIT) == -1);
  ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);

  Completion fresh;
  ASSERT_TRUE(
      fixture.backend.SubmitSend({.fd_ = original[0]}, payload, &fresh).ok());
  Drain(fixture.backend, [&] { return fresh.calls != 0; });
  ASSERT_TRUE(fresh.result == 1);
  ASSERT_TRUE(recv(replacement[1], &received, 1, MSG_DONTWAIT) == 1);
  ASSERT_TRUE(received == payload.front());
  ASSERT_TRUE(close(original[0]) == 0);
  ASSERT_TRUE(close(original[1]) == 0);
  ASSERT_TRUE(close(replacement[1]) == 0);
  std::cout << "PASS cancellation before close protects reused descriptors\n";
}

TEST(IoUringBackpressure, QueuedReceiveCancellation) {
  Fixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.Init());
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  ASSERT_TRUE(event >= 0);
  std::array<Completion, 8> blockers;
  for (auto& tag : blockers)
    ASSERT_TRUE(fixture.backend.SubmitPoll(event, POLLIN, &tag).ok());

  int sockets[2];
  ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                         sockets) == 0);
  bycorf::Connection connection;
  connection.worker_ = &fixture.worker;
  connection.file_.fd_ = sockets[0];
  connection.closed_ = false;
  ASSERT_TRUE(send(sockets[1], "x", 1, MSG_NOSIGNAL) == 1);
  ASSERT_TRUE(fixture.backend.StartRecvMultishot(&connection).ok());
  ASSERT_TRUE(connection.recv_armed_ && connection.inflight_ops_ == 1);
  connection.recv_paused_ = true;
  Completion cancelled;
  ASSERT_TRUE(fixture.backend.SubmitCancelRecv(&connection, &cancelled).ok());
  ASSERT_TRUE(cancelled.calls == 0);
  ASSERT_TRUE(connection.recv_armed_ && connection.inflight_ops_ == 1);
  Drain(fixture.backend, [&] { return cancelled.calls != 0; });
  ASSERT_TRUE(cancelled.result == 0);
  ASSERT_TRUE(!connection.recv_armed_ && connection.inflight_ops_ == 0);
  ASSERT_TRUE(connection.received_buffers_.empty());
  char received = 0;
  ASSERT_TRUE(recv(sockets[0], &received, 1, MSG_DONTWAIT) == 1);
  ASSERT_TRUE(received == 'x');

  const std::uint64_t one = 1;
  ASSERT_TRUE(write(event, &one, sizeof(one)) == sizeof(one));
  Drain(fixture.backend, [&] {
    for (const auto& tag : blockers)
      if (tag.calls == 0) return false;
    return true;
  });
  ASSERT_TRUE(close(event) == 0);
  ASSERT_TRUE(close(sockets[0]) == 0);
  ASSERT_TRUE(close(sockets[1]) == 0);
  std::cout << "PASS queued receive cancellation drains connection ownership\n";
}

TEST(IoUringBackpressure, LateCancelBeforeTagReuse) {
  Fixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.Init());
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  ASSERT_TRUE(event >= 0);
  int sockets[2];
  ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                         sockets) == 0);
  bycorf::Connection connection;
  connection.worker_ = &fixture.worker;
  connection.file_.fd_ = sockets[0];
  connection.closed_ = false;
  ASSERT_TRUE(fixture.backend.StartRecvMultishot(&connection).ok());
  ASSERT_TRUE(fixture.backend.Submit().ok());
  ASSERT_TRUE(!fixture.backend.HasPending());

  std::array<Completion, 8> blockers;
  for (auto& tag : blockers)
    ASSERT_TRUE(fixture.backend.SubmitPoll(event, POLLIN, &tag).ok());
  Completion cancelled;
  ASSERT_TRUE(fixture.backend.SubmitCancelRecv(&connection, &cancelled).ok());
  ASSERT_TRUE(cancelled.calls == 0);
  ASSERT_TRUE(send(sockets[1], "x", 1, MSG_NOSIGNAL) == 1);
  // Run deferred kernel completion work without publishing or submitting the
  // full SQ/backlog. The receive therefore finishes naturally while its cancel
  // is still wholly owned by the backend's submission queue.
  alarm(2);
  ASSERT_TRUE(io_uring_enter(fixture.backend.WakeHandle(), 0, 1,
                             IORING_ENTER_GETEVENTS, nullptr) >= 0);
  alarm(0);
  ASSERT_TRUE(fixture.backend.Poll());
  ASSERT_TRUE(!connection.recv_armed_ && connection.inflight_ops_ == 0);
  ASSERT_TRUE(connection.received_buffers_.size() == 1);
  ASSERT_TRUE(connection.read_buffer_.front() == std::byte{'x'});
  // This must be delivered by the same Poll, before any queued cancel can
  // enter the kernel. Merely checking the next read would allow FIFO submission
  // to hide a stale cancel that happens to run before the reused tag is armed.
  ASSERT_TRUE(cancelled.calls == 1 && cancelled.result == -ENOENT);

  connection.received_buffers_.clear();
  ASSERT_TRUE(fixture.backend.StartRecvMultishot(&connection).ok());
  ASSERT_TRUE(connection.recv_armed_ && connection.inflight_ops_ == 1);
  ASSERT_TRUE(send(sockets[1], "y", 1, MSG_NOSIGNAL) == 1);
  const std::uint64_t one = 1;
  ASSERT_TRUE(write(event, &one, sizeof(one)) == sizeof(one));
  Drain(fixture.backend, [&] {
    for (const auto& tag : blockers)
      if (tag.calls == 0) return false;
    return !connection.received_buffers_.empty();
  });
  ASSERT_TRUE(cancelled.calls == 1);
  ASSERT_TRUE(!connection.recv_armed_ && connection.inflight_ops_ == 0);
  ASSERT_TRUE(connection.read_buffer_.front() == std::byte{'y'});
  ASSERT_TRUE(close(event) == 0);
  ASSERT_TRUE(close(sockets[0]) == 0);
  ASSERT_TRUE(close(sockets[1]) == 0);
  std::cout
      << "PASS natural completion retires queued cancel before tag reuse\n";
}

TEST(IoUringBackpressure, PendingWaitAndShutdown) {
  Fixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.Init());
  const int event = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  ASSERT_TRUE(event >= 0);
  {
    std::array<Completion, 8> tags;
    for (auto& tag : tags)
      ASSERT_TRUE(fixture.backend.SubmitPoll(event, POLLIN, &tag).ok());
    ASSERT_TRUE(fixture.backend.SubmitCancel(&tags.back()).ok());
    ASSERT_TRUE(fixture.backend.HasPending());
    // None of these polls can complete. Blocking here would strand both the
    // unsubmitted work and the software cancellation completion behind it.
    alarm(2);
    ASSERT_TRUE(fixture.backend.Wait(-1));
    alarm(0);
    for (const auto& tag : tags) ASSERT_TRUE(tag.calls == 0);
    fixture.backend.Shutdown();
    ASSERT_TRUE(!fixture.backend.HasPending());
    ASSERT_TRUE(!fixture.backend.Poll());
  }
  ASSERT_TRUE(close(event) == 0);

  // Old tags are now destroyed. Reusing the backend must not issue abandoned
  // SQEs or dispatch their synthetic completions into the old caller's state.
  ASSERT_TRUE(fixture.backend
                  .Init({.ring_entries_ = 2, .recv_buffer_count_ = 0},
                        &fixture.worker, -1)
                  .ok());
  Completion fresh;
  const __kernel_timespec timeout{.tv_sec = 0, .tv_nsec = 1};
  ASSERT_TRUE(fixture.backend.SubmitTimeout(timeout, &fresh).ok());
  Drain(fixture.backend, [&] { return fresh.calls != 0; });
  ASSERT_TRUE(fresh.result == -ETIME);
  std::cout << "PASS pending work prevents park and shutdown drops old state\n";
}

TEST(IoUringBackpressure, StorageReads) {
  Fixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.Init());
  char path[] = "/tmp/bycorf-uring-backpressure-XXXXXX";
  const int file = mkstemp(path);
  ASSERT_TRUE(file >= 0);
  std::array<std::byte, 64> payload;
  for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = std::byte(i);
  ASSERT_TRUE(write(file, payload.data(), payload.size()) ==
              static_cast<ssize_t>(payload.size()));
  ASSERT_TRUE(fixture.backend.RegisterFixedFiles(1).ok());
  Completion opened;
  ASSERT_TRUE(
      fixture.backend.SubmitOpenDirect(path, O_RDONLY, 0, {0}, &opened).ok());
  Drain(fixture.backend, [&] { return opened.calls != 0; });
  ASSERT_TRUE(opened.result == 0);
  ASSERT_TRUE(unlink(path) == 0);
  ASSERT_TRUE(close(file) == 0);

  std::array<Completion, 16> tags;
  std::array<std::byte, 64> received{};
  const iovec region{.iov_base = received.data(), .iov_len = received.size()};
  ASSERT_TRUE(fixture.backend.RegisterBuffers(std::span(&region, 1)).ok());
  for (std::size_t i = 0; i < tags.size(); ++i) {
    const auto buffer = std::span(received).subspan(i * 4, 4);
    ASSERT_TRUE(
        (i % 2 == 0 ? fixture.backend.SubmitRead({0}, buffer, i * 4, &tags[i])
                    : fixture.backend.SubmitReadFixed(
                          {0}, {.data_ = buffer.data(), .size_ = buffer.size()},
                          i * 4, &tags[i]))
            .ok());
  }
  for (const auto& tag : tags) ASSERT_TRUE(tag.calls == 0);
  Drain(fixture.backend, [&] {
    for (const auto& tag : tags)
      if (tag.calls == 0) return false;
    return true;
  });
  for (const auto& tag : tags) ASSERT_TRUE(tag.result == 4);
  ASSERT_TRUE(received == payload);
  Completion closed;
  ASSERT_TRUE(fixture.backend.SubmitCloseDirect({0}, &closed).ok());
  Drain(fixture.backend, [&] { return closed.calls != 0; });
  ASSERT_TRUE(closed.result == 0);
  std::cout << "PASS queued fixed-file reads preserve buffers and offsets\n";
}

}  // namespace
