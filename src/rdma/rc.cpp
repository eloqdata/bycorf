// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "bycorf/rdma/rc.h"

#include <arpa/inet.h>

#include <cassert>
#include <limits>
#include <string>
#include <utility>

#include "bycorf/runtime/worker.h"
#include "rc_internal.h"

namespace bycorf::rdma {
namespace {
absl::Status Validate(const RcOptions& options,
                      std::span<const std::byte> data) {
  if (options.send_wr == 0 || options.recv_wr == 0 || options.cq_entries == 0 ||
      options.cq_entries >
          static_cast<unsigned>(std::numeric_limits<int>::max()) ||
      options.connect_timeout.count() <= 0 ||
      options.connect_timeout.count() > std::numeric_limits<int>::max() ||
      data.size() > 56)
    return absl::InvalidArgumentError(
        "invalid RDMA capacity, timeout or private data length");
  return absl::OkStatus();
}
absl::StatusOr<sockaddr_storage> Address(std::string_view ip,
                                         std::uint16_t port) {
  sockaddr_storage address{};
  std::string text(ip);
  auto* v4 = reinterpret_cast<sockaddr_in*>(&address);
  if (inet_pton(AF_INET, text.c_str(), &v4->sin_addr) == 1) {
    v4->sin_family = AF_INET;
    v4->sin_port = htons(port);
    return address;
  }
  auto* v6 = reinterpret_cast<sockaddr_in6*>(&address);
  if (inet_pton(AF_INET6, text.c_str(), &v6->sin6_addr) == 1) {
    v6->sin6_family = AF_INET6;
    v6->sin6_port = htons(port);
    return address;
  }
  return absl::InvalidArgumentError(
      "RDMA requires a numeric IPv4 or IPv6 address");
}
Task<absl::StatusOr<RcConnection>> FailedConnect(absl::Status error) {
  co_return error;
}

template <typename State>
void Drop(const LocalSharedPtr<State>& state) {
  if (!state || state->closed) return;
  if (auto engine = state->engine.lock()) {
    // Destruction can request asynchronous close but cannot migrate ownership.
    // After worker shutdown all public states are already retired.
    if (!engine->OnOwner() && !engine->stopping) std::terminate();
    engine->Close(state, absl::CancelledError("RDMA handle closed"));
  }
}
template <typename State>
Task<absl::Status> CloseImpl(LocalSharedPtr<State> state) {
  if (!state || state->closed) co_return absl::OkStatus();
  auto engine = state->engine.lock();
  if (!engine) co_return absl::CancelledError("RDMA owner is gone");
  if (!engine->OnOwner())
    co_return absl::FailedPreconditionError("RDMA close on a different worker");
  engine->Close(state, absl::CancelledError("RDMA handle closed"));
  co_return co_await detail::Wait{state->retired};
}
struct ConnectGuard {
  LocalSharedPtr<detail::Connection> state;
  bool delivered = false;
  ~ConnectGuard() {
    if (!delivered) Drop(state);
  }
};

Task<absl::StatusOr<RcCompletion>> WaitOperation(
    LocalSharedPtr<detail::Operation> operation) {
  if (!operation)
    co_return absl::FailedPreconditionError("empty RDMA operation");
  auto status = co_await detail::Wait{operation->completed};
  if (!status.ok()) co_return status;
  co_return operation->result;
}
// Keep the ownership check in the caller's submission path even without LTO;
// Engine::Submit is the out-of-line boundary for the actual admission work.
[[gnu::always_inline]] inline absl::StatusOr<LocalSharedPtr<detail::Operation>>
SubmitOperation(const LocalSharedPtr<detail::Connection>& connection,
                const RegisteredMemorySlice& slice,
                const detail::Request& request, bool try_only) {
  assert(connection);
  auto engine = connection->engine.lock();
  if (!engine) [[unlikely]] {
    return absl::CancelledError("RDMA owner is gone");
  }
  return engine->Submit(connection, slice, request, try_only);
}
Task<absl::StatusOr<RcCompletion>> Transfer(
    LocalSharedPtr<detail::Connection> connection, RegisteredMemorySlice slice,
    detail::Request request) {
  auto operation = SubmitOperation(connection, slice, request, false);
  // The operation's lease is sufficient after admission. Do not keep an extra
  // descriptor reference in a suspended frame past terminal completion.
  slice = {};
  if (!operation.ok()) co_return operation.status();
  // Wait in this frame rather than constructing a second Task/frame just to
  // install the completion waiter. The operation remains owned across suspend.
  auto status = co_await detail::Wait{(*operation)->completed};
  if (!status.ok()) co_return status;
  co_return (*operation)->result;
}
}  // namespace

bool RcOperation::ready() const noexcept {
  return state_ && state_->completed.ready;
}
absl::StatusOr<RcCompletion> RcOperation::result() const {
  return state_ ? state_->result
                : absl::StatusOr<RcCompletion>(
                      absl::FailedPreconditionError("empty RDMA operation"));
}
Task<absl::StatusOr<RcCompletion>> RcOperation::Wait() {
  return WaitOperation(state_);
}
absl::Status RcOperation::Cancel() {
  if (!state_) return absl::FailedPreconditionError("empty RDMA operation");
  if (ready()) return absl::OkStatus();
  auto connection = state_->connection.lock();
  auto engine = connection ? connection->engine.lock() : nullptr;
  if (!engine) return absl::CancelledError("RDMA owner is gone");
  return engine->Cancel(state_);
}

RcConnection::RcConnection(LocalSharedPtr<detail::Connection> state)
    : state_(std::move(state)) {}
RcConnection::RcConnection(RcConnection&&) noexcept = default;
RcConnection& RcConnection::operator=(RcConnection&& other) noexcept {
  if (this != &other) {
    Drop(state_);
    state_ = std::move(other.state_);
  }
  return *this;
}
RcConnection::~RcConnection() { Drop(state_); }
Task<absl::StatusOr<RcConnection>> RcConnection::Connect(
    Worker& worker, std::string_view ip, std::uint16_t port, RcOptions options,
    std::span<const std::byte> private_data) {
  auto status = Validate(options, private_data);
  if (!status.ok()) return FailedConnect(status);
  if (port == 0)
    return FailedConnect(
        absl::InvalidArgumentError("RDMA connect port is zero"));
  auto address = Address(ip, port);
  if (!address.ok()) return FailedConnect(address.status());
  auto engine = worker.GetRdmaEngine();
  if (!engine.ok()) return FailedConnect(engine.status());
  return ConnectImpl(*engine, *address, options,
                     {private_data.begin(), private_data.end()});
}
Task<absl::StatusOr<RcConnection>> RcConnection::ConnectImpl(
    LocalSharedPtr<detail::Engine> engine, sockaddr_storage address,
    RcOptions options, std::vector<std::byte> data) {
  if (!engine->OnOwner())
    co_return absl::FailedPreconditionError(
        "RDMA connect on a different worker");
  auto started = engine->Connect(address, options, std::move(data));
  if (!started.ok()) co_return started.status();
  auto c = *started;
  ConnectGuard guard{c};
  auto status = co_await detail::Wait{c->connected};
  if (!status.ok()) co_return status;
  if (!c->error.ok()) co_return c->error;
  guard.delivered = true;
  co_return RcConnection(std::move(c));
}
Task<absl::Status> RcConnection::Close() { return CloseImpl(state_); }
bool RcConnection::IsOpen() const noexcept {
  return state_ && state_->established && !state_->closing;
}
absl::Status RcConnection::status() const {
  return state_ ? state_->error
                : absl::FailedPreconditionError("empty RDMA connection");
}
std::span<const std::byte> RcConnection::peer_private_data() const noexcept {
  return state_ ? std::span<const std::byte>(state_->peer_data)
                : std::span<const std::byte>{};
}
RdmaProtectionDomain RcConnection::protection_domain() const {
  return state_ && state_->device ? state_->device->domain
                                  : RdmaProtectionDomain{};
}

[[gnu::always_inline]] inline absl::StatusOr<RcOperation> RcConnection::Submit(
    const RegisteredMemorySlice& slice, const detail::Request& request,
    bool try_only) {
  auto operation = SubmitOperation(state_, slice, request, try_only);
  if (!operation.ok()) return operation.status();
  return RcOperation(*operation);
}
absl::StatusOr<RcOperation> RcConnection::SubmitSend(
    const RegisteredMemorySlice& slice) {
  return Submit(slice, {detail::Opcode::kSend}, false);
}
absl::StatusOr<RcOperation> RcConnection::SubmitRecv(
    const RegisteredMemorySlice& slice) {
  return Submit(slice, {detail::Opcode::kRecv}, false);
}
absl::StatusOr<RcOperation> RcConnection::TrySubmitSend(
    const RegisteredMemorySlice& slice) {
  return Submit(slice, {detail::Opcode::kSend}, true);
}
absl::StatusOr<RcOperation> RcConnection::TrySubmitRecv(
    const RegisteredMemorySlice& slice) {
  return Submit(slice, {detail::Opcode::kRecv}, true);
}
Task<absl::StatusOr<RcCompletion>> RcConnection::Send(
    const RegisteredMemorySlice& slice) {
  return Transfer(state_, slice, {detail::Opcode::kSend});
}
Task<absl::StatusOr<RcCompletion>> RcConnection::Recv(
    const RegisteredMemorySlice& slice) {
  return Transfer(state_, slice, {detail::Opcode::kRecv});
}

absl::StatusOr<RcOperation> RcConnection::SubmitRead(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote) {
  return Submit(slice, {detail::Opcode::kRead, remote}, false);
}

absl::StatusOr<RcOperation> RcConnection::TrySubmitRead(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote) {
  return Submit(slice, {detail::Opcode::kRead, remote}, true);
}

Task<absl::StatusOr<RcCompletion>> RcConnection::Read(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote) {
  return Transfer(state_, slice, {detail::Opcode::kRead, remote});
}

absl::StatusOr<RcOperation> RcConnection::SubmitWrite(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote) {
  return Submit(slice, {detail::Opcode::kWrite, remote}, false);
}

absl::StatusOr<RcOperation> RcConnection::TrySubmitWrite(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote) {
  return Submit(slice, {detail::Opcode::kWrite, remote}, true);
}

Task<absl::StatusOr<RcCompletion>> RcConnection::Write(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote) {
  return Transfer(state_, slice, {detail::Opcode::kWrite, remote});
}

absl::StatusOr<RcOperation> RcConnection::SubmitSendWithImm(
    const RegisteredMemorySlice& slice, std::uint32_t immediate) {
  return Submit(slice, {detail::Opcode::kSendWithImm, {}, immediate}, false);
}

absl::StatusOr<RcOperation> RcConnection::TrySubmitSendWithImm(
    const RegisteredMemorySlice& slice, std::uint32_t immediate) {
  return Submit(slice, {detail::Opcode::kSendWithImm, {}, immediate}, true);
}

Task<absl::StatusOr<RcCompletion>> RcConnection::SendWithImm(
    const RegisteredMemorySlice& slice, std::uint32_t immediate) {
  return Transfer(state_, slice, {detail::Opcode::kSendWithImm, {}, immediate});
}

absl::StatusOr<RcOperation> RcConnection::SubmitWriteWithImm(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote,
    std::uint32_t immediate) {
  return Submit(slice, {detail::Opcode::kWriteWithImm, remote, immediate},
                false);
}

absl::StatusOr<RcOperation> RcConnection::TrySubmitWriteWithImm(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote,
    std::uint32_t immediate) {
  return Submit(slice, {detail::Opcode::kWriteWithImm, remote, immediate},
                true);
}

Task<absl::StatusOr<RcCompletion>> RcConnection::WriteWithImm(
    const RegisteredMemorySlice& slice, const RemoteMemoryRegion& remote,
    std::uint32_t immediate) {
  return Transfer(state_, slice,
                  {detail::Opcode::kWriteWithImm, remote, immediate});
}

RcListener::RcListener(RcListener&&) noexcept = default;
RcListener& RcListener::operator=(RcListener&& other) noexcept {
  if (this != &other) {
    Drop(state_);
    state_ = std::move(other.state_);
  }
  return *this;
}
RcListener::~RcListener() { Drop(state_); }
absl::StatusOr<RcListener> RcListener::Listen(
    Worker& worker, std::string_view ip, std::uint16_t port, RcOptions options,
    std::span<const std::byte> private_data, int backlog) {
  auto status = Validate(options, private_data);
  if (!status.ok()) return status;
  if (backlog <= 0)
    return absl::InvalidArgumentError("RDMA backlog must be positive");
  auto address = Address(ip, port);
  if (!address.ok()) return address.status();
  auto engine = worker.GetRdmaEngine();
  if (!engine.ok()) return engine.status();
  auto started = (*engine)->Listen(
      *address, options, {private_data.begin(), private_data.end()}, backlog);
  if (!started.ok()) return started.status();
  RcListener listener;
  listener.state_ = *started;
  return listener;
}
Task<absl::StatusOr<RcConnection>> RcListener::Accept() {
  return AcceptImpl(state_);
}
Task<absl::StatusOr<RcConnection>> RcListener::AcceptImpl(
    LocalSharedPtr<detail::Listener> state) {
  if (!state) co_return absl::FailedPreconditionError("empty RDMA listener");
  auto engine = state->engine.lock();
  if (!engine || state->closing)
    co_return state->error.ok() ? absl::CancelledError("RDMA listener closed")
                                : state->error;
  if (!engine->OnOwner())
    co_return absl::FailedPreconditionError(
        "RDMA accept on a different worker");
  if (state->accepted.waiter)
    co_return absl::FailedPreconditionError("concurrent RDMA accept");
  while (state->ready.empty() && !state->closing) {
    state->accepted.ready = false;
    auto status = co_await detail::Wait{state->accepted};
    if (!status.ok()) co_return status;
  }
  if (state->closing) co_return state->error;
  auto child = state->children.find(state->ready.front());
  assert(child != state->children.end());
  // A failed connection may already be absent from the engine registry. Take
  // the listener's ownership before removing either index, without suspending.
  auto c = std::move(child->second);
  state->ready.pop_front();
  state->children.erase(child);
  c->listener.reset();
  if (!c->error.ok()) co_return c->error;
  co_return RcConnection(std::move(c));
}
Task<absl::Status> RcListener::Close() { return CloseImpl(state_); }
std::uint16_t RcListener::port() const noexcept {
  return state_ ? state_->port : 0;
}
}  // namespace bycorf::rdma
