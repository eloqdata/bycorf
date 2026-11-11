// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "bycorf/rdma/rc.h"

#include <arpa/inet.h>

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
void Drop(const std::shared_ptr<State>& state) {
  if (!state || state->closed) return;
  if (auto engine = state->engine.lock()) {
    // Destruction can request asynchronous close but cannot migrate ownership.
    // After worker shutdown all public states are already retired.
    if (!engine->OnOwner() && !engine->stopping) std::terminate();
    engine->Close(state, absl::CancelledError("RDMA handle closed"));
  }
}
template <typename State>
Task<absl::Status> CloseImpl(std::shared_ptr<State> state) {
  if (!state || state->closed) co_return absl::OkStatus();
  auto engine = state->engine.lock();
  if (!engine) co_return absl::CancelledError("RDMA owner is gone");
  if (!engine->OnOwner())
    co_return absl::FailedPreconditionError("RDMA close on a different worker");
  engine->Close(state, absl::CancelledError("RDMA handle closed"));
  co_return co_await detail::Wait{state->retired};
}
struct ConnectGuard {
  std::shared_ptr<detail::Connection> state;
  bool delivered = false;
  ~ConnectGuard() {
    if (!delivered) Drop(state);
  }
};
}  // namespace

RcConnection::RcConnection(std::shared_ptr<detail::Connection> state)
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
    std::shared_ptr<detail::Engine> engine, sockaddr_storage address,
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
    std::shared_ptr<detail::Listener> state) {
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
  auto c = std::move(state->ready.front());
  state->ready.pop_front();
  state->children.erase(c.get());
  c->listener.reset();
  if (!c->error.ok()) co_return c->error;
  co_return RcConnection(std::move(c));
}
Task<absl::Status> RcListener::Close() { return CloseImpl(state_); }
std::uint16_t RcListener::port() const noexcept {
  return state_ ? state_->port : 0;
}
}  // namespace bycorf::rdma
