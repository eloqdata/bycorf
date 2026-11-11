// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#if BYCORF_ENABLE_RDMA
#include <sys/socket.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "bycorf/io/rdma_memory.h"
#include "bycorf/runtime/task.h"

namespace bycorf {
class Worker;
namespace rdma {
namespace detail {
struct Connection;
struct Listener;
class Engine;
class RcTestPeer;
}  // namespace detail

// Fixed per-QP capacities; a shared CQ reserves the provider's actual send+recv
// capacities before admitting the QP. No CQ growth or receive-buffer
// allocation.
struct RcOptions {
  std::uint32_t send_wr = 128;
  std::uint32_t recv_wr = 128;
  std::uint32_t cq_entries = 4096;
  std::chrono::milliseconds connect_timeout{5000};
};

// Owner-worker-only RC connection. Move-only; destruction requests asynchronous
// close. Await Close() to join resource retirement while the worker is running.
// Worker shutdown also closes all connections before reclaiming coroutine
// frames.
class RcConnection {
 public:
  RcConnection() = default;
  RcConnection(RcConnection&&) noexcept;
  RcConnection& operator=(RcConnection&&) noexcept;
  RcConnection(const RcConnection&) = delete;
  RcConnection& operator=(const RcConnection&) = delete;
  ~RcConnection();

  // Numeric IPv4/IPv6 only. Copies arguments before returning the lazy task.
  // The deadline covers address/route resolution and the RC handshake. Private
  // data is limited to 56 bytes for portable RC CM requests and responses.
  static Task<absl::StatusOr<RcConnection>> Connect(
      Worker& worker, std::string_view ip, std::uint16_t port,
      RcOptions options = {}, std::span<const std::byte> private_data = {});
  // Idempotent; one Close waiter at a time. Completion joins this connection's
  // timer/event references and QP/CM ID destruction, not merely cancellation.
  // Shared CM/device fd polls belong to the worker, not individual connections.
  Task<absl::Status> Close();
  bool IsOpen() const noexcept;
  // Preserves the specific CM/provider failure; local close reports Cancelled.
  absl::Status status() const;
  std::span<const std::byte> peer_private_data() const noexcept;
  // Retain this handle through registered memory use; it is the QP's actual PD.
  RdmaProtectionDomain protection_domain() const;

 private:
  friend class RcListener;
  friend class detail::RcTestPeer;
  explicit RcConnection(std::shared_ptr<detail::Connection> state);
  static Task<absl::StatusOr<RcConnection>> ConnectImpl(
      std::shared_ptr<detail::Engine> engine, sockaddr_storage address,
      RcOptions options, std::vector<std::byte> data);
  std::shared_ptr<detail::Connection> state_;
};

// A listener and every accepted connection stay on the listening worker.
// Accepted connections already returned to the application outlive the
// listener.
class RcListener {
 public:
  RcListener() = default;
  RcListener(RcListener&&) noexcept;
  RcListener& operator=(RcListener&&) noexcept;
  RcListener(const RcListener&) = delete;
  RcListener& operator=(const RcListener&) = delete;
  ~RcListener();

  // Binds a numeric local address; port zero selects an ephemeral port. Options
  // and response private data are copied. Requests beyond backlog are rejected.
  static absl::StatusOr<RcListener> Listen(
      Worker& worker, std::string_view ip, std::uint16_t port,
      RcOptions options = {}, std::span<const std::byte> private_data = {},
      int backlog = 128);
  // One waiter at a time. Returns after establishment or a specific request's
  // failure; queued and handshaking children are closed by listener Close().
  Task<absl::StatusOr<RcConnection>> Accept();
  Task<absl::Status> Close();
  std::uint16_t port() const noexcept;

 private:
  friend class detail::RcTestPeer;
  static Task<absl::StatusOr<RcConnection>> AcceptImpl(
      std::shared_ptr<detail::Listener> state);
  std::shared_ptr<detail::Listener> state_;
};
}  // namespace rdma
}  // namespace bycorf
#endif
