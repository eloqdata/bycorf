// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "rdma_demo_session.h"

#include <algorithm>
#include <array>
#include <optional>
#include <vector>

#include "bycorf/runtime/worker.h"

namespace bycorf::example {
namespace {
using namespace rdma;
constexpr std::uint32_t kMagic = 0x42524431;  // BRD1, version 1
constexpr std::uint32_t kImmediate = 0x01020304;

// Only the example protocol serializes descriptors. Never send the native
// RemoteMemoryRegion object: its padding and host byte order are not a format.
void Put(std::span<std::byte> out, std::uint64_t value) {
  for (auto it = out.rbegin(); it != out.rend(); ++it) {
    *it = static_cast<std::byte>(value & 0xff);
    value >>= 8;
  }
}
std::uint64_t Get(std::span<const std::byte> in) {
  std::uint64_t value = 0;
  for (auto byte : in) value = (value << 8) | std::to_integer<unsigned>(byte);
  return value;
}
absl::Status Check(absl::StatusOr<RcCompletion> result, RcCompletionKind kind,
                   std::size_t bytes,
                   std::optional<std::uint32_t> immediate = std::nullopt) {
  if (!result.ok()) return result.status();
  if (result->kind != kind || result->bytes != bytes ||
      result->immediate != immediate)
    return absl::DataLossError("unexpected RDMA completion");
  return absl::OkStatus();
}

#define DEMO_TRY(expression)            \
  do {                                  \
    const auto status = (expression);   \
    if (!status.ok()) co_return status; \
  } while (false)

struct Session {
  explicit Session(std::size_t bytes) : payload(bytes) {}
  // Backing storage precedes registrations, so destruction releases MRs first.
  std::vector<std::byte> payload;
  std::array<std::byte, 32> tx{}, rx{};
  RegisteredMemorySlice data, send, receive;

  absl::Status Init(Worker& worker, RcConnection& connection, bool server) {
    auto access = RdmaAccess::kLocalWrite;
    if (server)
      access = access | RdmaAccess::kRemoteRead | RdmaAccess::kRemoteWrite;
    const auto domain = connection.protection_domain();
    auto bind = [&](std::span<std::byte> backing, RdmaAccess flags,
                    RegisteredMemorySlice& slice) -> absl::Status {
      auto mr = worker.RegisterRdmaMemory(domain, backing, flags);
      if (!mr.ok()) return mr.status();
      auto range = mr->Slice(0, backing.size());
      if (!range.ok()) return range.status();
      slice = *range;
      return absl::OkStatus();
    };
    auto status = bind(payload, access, data);
    if (status.ok()) status = bind(tx, RdmaAccess::kNone, send);
    if (status.ok()) status = bind(rx, RdmaAccess::kLocalWrite, receive);
    return status;
  }
  void Fill(unsigned seed) {
    for (std::size_t i = 0; i < payload.size(); ++i)
      payload[i] = static_cast<std::byte>((i + seed) % 251);
  }
  absl::Status Verify(unsigned seed) const {
    for (std::size_t i = 0; i < payload.size(); ++i)
      if (payload[i] != static_cast<std::byte>((i + seed) % 251))
        return absl::DataLossError("RDMA payload mismatch");
    return absl::OkStatus();
  }
  void Control(unsigned stage) {
    tx.fill(std::byte{});
    Put(std::span(tx).subspan(0, 4), kMagic);
    Put(std::span(tx).subspan(4, 4), stage);
  }
  Task<absl::Status> SendControl(RcConnection& connection, unsigned stage) {
    Control(stage);
    co_return Check(co_await connection.Send(send), RcCompletionKind::kSend,
                    tx.size());
  }
  Task<absl::Status> RecvControl(RcConnection& connection, unsigned stage) {
    DEMO_TRY(Check(co_await connection.Recv(receive), RcCompletionKind::kRecv,
                   rx.size()));
    if (Get(std::span(rx).subspan(0, 4)) != kMagic ||
        Get(std::span(rx).subspan(4, 4)) != stage)
      co_return absl::DataLossError("unexpected RDMA example control message");
    co_return absl::OkStatus();
  }
  Task<absl::Status> Server(RcConnection& connection) {
    Fill(1);
    auto remote =
        data.RemoteRegion(RdmaAccess::kRemoteRead | RdmaAccess::kRemoteWrite);
    if (!remote.ok()) co_return remote.status();
    Control(1);
    Put(std::span(tx).subspan(8, 8), remote->address);
    Put(std::span(tx).subspan(16, 8), remote->length);
    Put(std::span(tx).subspan(24, 4), remote->rkey);
    DEMO_TRY(Check(co_await connection.Send(send), RcCompletionKind::kSend,
                   tx.size()));
    DEMO_TRY(co_await RecvControl(connection, 2));
    DEMO_TRY(Verify(2));

    // Explicitly post the peer's receive credit before announcing readiness.
    auto message = connection.SubmitRecv(data);
    if (!message.ok()) co_return message.status();
    DEMO_TRY(co_await SendControl(connection, 3));
    DEMO_TRY(Check(co_await message->Wait(), RcCompletionKind::kRecv,
                   payload.size(), kImmediate));
    DEMO_TRY(Verify(3));

    rx.fill(std::byte{0x55});
    auto notification = connection.SubmitRecv(receive);
    if (!notification.ok()) co_return notification.status();
    DEMO_TRY(co_await SendControl(connection, 4));
    DEMO_TRY(Check(co_await notification->Wait(),
                   RcCompletionKind::kRecvWriteWithImm, payload.size(), 0));
    DEMO_TRY(Verify(4));
    if (!std::all_of(rx.begin(), rx.end(),
                     [](auto byte) { return byte == std::byte{0x55}; }))
      co_return absl::DataLossError(
          "WRITE_WITH_IMM modified its notification buffer");

    // The client validates this final response before it disconnects. Keep its
    // backing memory alive through peer close; CM disconnect may be dispatched
    // before the final SEND CQE, so that local result alone is not a delivery
    // ack.
    Control(5);
    auto response = connection.SubmitSend(send);
    if (!response.ok()) co_return response.status();
    auto eof = co_await connection.Recv(receive);
    if (eof.ok() || connection.IsOpen())
      co_return absl::DataLossError(
          "expected peer disconnect after final response");
    co_return absl::OkStatus();
  }
  Task<absl::Status> Client(RcConnection& connection) {
    DEMO_TRY(co_await RecvControl(connection, 1));
    RemoteMemoryRegion remote{
        Get(std::span(rx).subspan(8, 8)), Get(std::span(rx).subspan(16, 8)),
        static_cast<std::uint32_t>(Get(std::span(rx).subspan(24, 4)))};
    if (remote.length != payload.size())
      co_return absl::InvalidArgumentError(
          "client and server payload sizes differ");
    DEMO_TRY(Check(co_await connection.Read(data, remote),
                   RcCompletionKind::kRead, payload.size()));
    DEMO_TRY(Verify(1));
    Fill(2);
    DEMO_TRY(Check(co_await connection.Write(data, remote),
                   RcCompletionKind::kWrite, payload.size()));
    DEMO_TRY(co_await SendControl(connection, 2));
    DEMO_TRY(co_await RecvControl(connection, 3));
    Fill(3);
    DEMO_TRY(Check(co_await connection.SendWithImm(data, kImmediate),
                   RcCompletionKind::kSend, payload.size()));
    DEMO_TRY(co_await RecvControl(connection, 4));
    Fill(4);
    DEMO_TRY(Check(co_await connection.WriteWithImm(data, remote, 0),
                   RcCompletionKind::kWrite, payload.size()));
    co_return co_await RecvControl(connection, 5);
  }
};
#undef DEMO_TRY
}  // namespace

Task<absl::Status> RunRdmaDemo(Worker& worker, rdma::RcConnection& connection,
                               bool server, std::size_t bytes) {
  Session session(bytes);
  auto status = session.Init(worker, connection, server);
  if (status.ok())
    status = server ? co_await session.Server(connection)
                    : co_await session.Client(connection);
  // Also join outstanding receives on every early error before freeing memory.
  const auto closed = co_await connection.Close();
  co_return status.ok() ? closed : status;
}
}  // namespace bycorf::example
