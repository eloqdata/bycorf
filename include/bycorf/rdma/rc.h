// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#if BYCORF_ENABLE_RDMA
#include <sys/socket.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "bycorf/base/local_shared_ptr.h"
#include "bycorf/io/rdma_memory.h"
#include "bycorf/runtime/task.h"

namespace bycorf {
class Worker;
namespace rdma {
namespace detail {
struct Connection;
struct Listener;
struct Operation;
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

// Owner-worker-only handle for one accepted SEND or RECV. The engine retains
// its state and MR lease even if this handle is dropped. Caller-owned payload
// must survive until terminal completion; dropping a handle does not cancel.
class RcOperation {
 public:
  RcOperation() = default;
  RcOperation(RcOperation&&) noexcept = default;
  RcOperation& operator=(RcOperation&&) noexcept = default;
  RcOperation(const RcOperation&) = delete;
  RcOperation& operator=(const RcOperation&) = delete;

  bool ready() const noexcept;
  // FailedPrecondition before completion or on an empty handle. Success is
  // bytes sent / actually received. Only local buffer reuse is guaranteed;
  // SEND completion does not mean the peer application has consumed the data.
  absl::StatusOr<std::size_t> result() const;
  // One waiter at a time. Copies state before returning the lazy task.
  Task<absl::StatusOr<std::size_t>> Wait();
  // Queued work is removed independently. Verbs cannot cancel a single posted
  // RC WR: cancelling posted work closes the connection and fails its other
  // operations. Await Wait() (or connection.Close()) before freeing payload.
  absl::Status Cancel();

 private:
  friend class RcConnection;
  explicit RcOperation(LocalSharedPtr<detail::Operation> state)
      : state_(std::move(state)) {}
  LocalSharedPtr<detail::Operation> state_;
};

// Owner-worker-only RC connection. Move-only; destruction requests asynchronous
// close. Await Close() to join resource retirement while the worker is running.
// Worker shutdown also closes all connections before reclaiming coroutine
// frames. Handles and returned Tasks use non-atomic ownership: access, moves
// and destruction stay on the creating thread, including after shutdown.
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

  // All data submission methods below require a nonempty connection handle:
  // default-constructed and moved-from handles must not be used. A closed
  // connection remains a valid handle and returns an error at runtime.
  // Nonblocking admission. Snapshots the slice's address, length and lkey and
  // retains a lease; never borrows the descriptor or an SGE array. Capacity
  // shortages queue FIFO per QP/direction. No completion is dispatched inline.
  // Errors returned here mean no operation was accepted. Every accepted send
  // requests a CQ completion. Recv requires local-write registration.
  absl::StatusOr<RcOperation> SubmitSend(const RegisteredMemorySlice& slice);
  absl::StatusOr<RcOperation> SubmitRecv(const RegisteredMemorySlice& slice);
  // Same admission, but local capacity exhaustion returns ResourceExhausted
  // immediately without accepting work or bypassing existing FIFO waiters.
  absl::StatusOr<RcOperation> TrySubmitSend(const RegisteredMemorySlice& slice);
  absl::StatusOr<RcOperation> TrySubmitRecv(const RegisteredMemorySlice& slice);
  // Lazy coroutine adapters copy the slice at invocation and submit when
  // awaited. Backpressure is automatic. Keep payload alive until completion;
  // cancel/close and join before destroying a suspended caller frame.
  Task<absl::StatusOr<std::size_t>> Send(const RegisteredMemorySlice& slice);
  Task<absl::StatusOr<std::size_t>> Recv(const RegisteredMemorySlice& slice);

 private:
  friend class RcListener;
  friend class detail::RcTestPeer;
  explicit RcConnection(LocalSharedPtr<detail::Connection> state);
  absl::StatusOr<RcOperation> Submit(const RegisteredMemorySlice& slice,
                                     bool receive, bool try_only);
  static Task<absl::StatusOr<RcConnection>> ConnectImpl(
      LocalSharedPtr<detail::Engine> engine, sockaddr_storage address,
      RcOptions options, std::vector<std::byte> data);
  LocalSharedPtr<detail::Connection> state_;
};

// A listener and every accepted connection stay on the listening worker.
// Accepted connections already returned to the application outlive the
// listener. Handles and returned Tasks follow the same creating-thread lifetime
// contract as RcConnection, including destruction after worker shutdown.
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
      LocalSharedPtr<detail::Listener> state);
  LocalSharedPtr<detail::Listener> state_;
};
}  // namespace rdma
}  // namespace bycorf
#endif
