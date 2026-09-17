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

#include "bycorf/rpc/rpc.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <string>

#include "bycorf/net/connection.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"

namespace bycorf::rpc {

namespace {

constexpr std::size_t kHeader = sizeof(WireHeader);
constexpr std::uint32_t kMaxPayloadBytes = 16U * 1024U * 1024U;

void Store16(std::byte* out, std::uint16_t value) noexcept {
  out[0] = static_cast<std::byte>(value & 0xffU);
  out[1] = static_cast<std::byte>((value >> 8) & 0xffU);
}

void Store32(std::byte* out, std::uint32_t value) noexcept {
  for (unsigned i = 0; i < 4; ++i) {
    out[i] = static_cast<std::byte>((value >> (i * 8)) & 0xffU);
  }
}

void Store64(std::byte* out, std::uint64_t value) noexcept {
  for (unsigned i = 0; i < 8; ++i) {
    out[i] = static_cast<std::byte>((value >> (i * 8)) & 0xffU);
  }
}

std::uint16_t Load16(const std::byte* in) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(in[0])) |
         (static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(in[1]))
          << 8);
}

std::uint32_t Load32(const std::byte* in) noexcept {
  std::uint32_t value = 0;
  for (unsigned i = 0; i < 4; ++i) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(in[i]))
             << (i * 8);
  }
  return value;
}

std::uint64_t Load64(const std::byte* in) noexcept {
  std::uint64_t value = 0;
  for (unsigned i = 0; i < 8; ++i) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(in[i]))
             << (i * 8);
  }
  return value;
}

void EncodeHeader(const WireHeader& header, std::byte* out) noexcept {
  Store64(out, header.req_id_);
  Store32(out + 8, header.len_);
  Store16(out + 12, header.verb_);
  out[14] = static_cast<std::byte>(header.type_);
  out[15] = static_cast<std::byte>(header.pad_);
}

WireHeader DecodeHeader(const std::byte* in) noexcept {
  return WireHeader{
      .req_id_ = Load64(in),
      .len_ = Load32(in + 8),
      .verb_ = Load16(in + 12),
      .type_ = std::to_integer<std::uint8_t>(in[14]),
      .pad_ = std::to_integer<std::uint8_t>(in[15]),
  };
}

void AppendFrame(Bytes& out, const WireHeader& header, BytesView payload) {
  const std::size_t off = out.size();
  out.resize(off + kHeader + payload.size());
  EncodeHeader(header, out.data() + off);
  if (!payload.empty()) {
    std::memcpy(out.data() + off + kHeader, payload.data(), payload.size());
  }
}

}  // namespace

// ---------------------------------------------------------------- RpcServer

void RpcServer::OnVerb(std::uint16_t verb, Handler handler) {
  handlers_[verb] = std::move(handler);
}

void RpcServer::OnVerbAsync(std::uint16_t verb, AsyncHandler handler) {
  async_handlers_[verb] = std::move(handler);
}

Task<absl::Status> RpcServer::Serve(TcpStream stream) {
  Bytes buf;
  std::array<std::byte, 16384> chunk{};

  while (stream.IsOpen()) {
    std::size_t pos = 0;
    Bytes out;
    while (buf.size() - pos >= kHeader) {
      const WireHeader h = DecodeHeader(buf.data() + pos);
      if (h.type_ != kRequest || h.pad_ != 0 || h.len_ > kMaxPayloadBytes) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid rpc request header");
      }
      if (buf.size() - pos - kHeader < h.len_) {
        break;  // partial frame; wait for more
      }
      BytesView payload(buf.data() + pos + kHeader, h.len_);
      Bytes resp;
      if (auto async = async_handlers_.find(h.verb_);
          async != async_handlers_.end()) {
        resp = co_await async->second(payload);
      } else if (auto sync = handlers_.find(h.verb_); sync != handlers_.end()) {
        resp = sync->second(payload);
      }
      if (resp.size() > kMaxPayloadBytes) {
        co_return absl::Status(absl::StatusCode::kOutOfRange,
                               "rpc response exceeds payload limit");
      }
      WireHeader rh{
          .req_id_ = h.req_id_,
          .len_ = static_cast<std::uint32_t>(resp.size()),
          .verb_ = h.verb_,
          .type_ = kResponse,
          .pad_ = 0,
      };
      AppendFrame(out, rh, BytesView(resp.data(), resp.size()));
      pos += kHeader + h.len_;
    }
    if (pos != 0) {
      buf.erase(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(pos));
    }
    if (!out.empty()) {
      auto st = co_await stream.WriteAll(BytesView(out.data(), out.size()));
      if (!st.ok()) co_return st;
    }

    auto r = co_await stream.ReadSome(chunk);
    if (!r.ok()) co_return r.status();
    if (*r == 0) break;  // peer closed
    buf.insert(buf.end(), chunk.data(), chunk.data() + *r);
  }
  co_return absl::OkStatus();
}

// ---------------------------------------------------------------- RpcClient

Task<absl::Status> RpcClient::Connect(std::string_view ip, std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    co_return absl::Status(absl::StatusCode::kInternal, "rpc socket failed");
  }
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, std::string(ip).c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid IPv4 address");
  }
  // Blocking connect at setup (cold path); the data path below is async.
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    co_return absl::Status(absl::StatusCode::kUnavailable,
                           "rpc connect failed");
  }
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    ::close(fd);
    co_return absl::Status(absl::StatusCode::kInternal,
                           "fcntl(O_NONBLOCK) failed");
  }

  Connection connection;
  connection.worker_ = ThisWorker().self_;
  connection.file_.fd_ = fd;
  connection.closed_ = false;
  Connection* registered =
      ThisWorker().self_->AddConnection(std::move(connection));
  if (registered == nullptr) {
    ::close(fd);
    co_return absl::Status(absl::StatusCode::kInternal,
                           "failed to register rpc connection");
  }
  stream_ = TcpStream(registered);
  ThisWorker().self_->Spawn(ReadLoop());
  co_return absl::OkStatus();
}

void RpcClient::SendFrame(const WireHeader& header, BytesView payload) {
  Bytes frame;
  AppendFrame(frame, header, payload);
  out_.push_back(std::move(frame));
  if (!writing_) {
    writing_ = true;
    ThisWorker().self_->Spawn(WriteLoop());
  }
}

Task<absl::Status> RpcClient::WriteLoop() {
  while (!out_.empty()) {
    // Coalesce all currently queued frames into one send.
    Bytes batch;
    for (const auto& frame : out_) {
      batch.insert(batch.end(), frame.begin(), frame.end());
    }
    out_.clear();
    auto st = co_await stream_.WriteAll(BytesView(batch.data(), batch.size()));
    if (!st.ok()) {
      writing_ = false;
      co_return st;
    }
  }
  writing_ = false;
  co_return absl::OkStatus();
}

Task<absl::StatusOr<Bytes>> RpcClient::Call(std::uint16_t verb,
                                            BytesView payload) {
  if (!stream_.IsOpen()) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "rpc client not connected");
  }
  if (payload.size() > kMaxPayloadBytes) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "rpc request exceeds payload limit");
  }

  Pending pending;
  const std::uint64_t id = next_req_id_++;
  pending_[id] = &pending;

  WireHeader h{
      .req_id_ = id,
      .len_ = static_cast<std::uint32_t>(payload.size()),
      .verb_ = verb,
      .type_ = kRequest,
      .pad_ = 0,
  };
  SendFrame(h, payload);

  struct PendingAwaiter {
    Pending* p_;
    bool await_ready() const noexcept { return p_->done_; }
    void await_suspend(std::coroutine_handle<> handle) noexcept {
      p_->waiter_ = handle;
    }
    void await_resume() const noexcept {}
  };
  co_await PendingAwaiter{&pending};  // ReadLoop fills *pending and resumes us

  if (!pending.status_.ok()) {
    co_return pending.status_;
  }
  co_return std::move(pending.result_);
}

Task<absl::Status> RpcClient::ReadLoop() {
  Bytes buf;
  std::array<std::byte, 16384> chunk{};

  while (stream_.IsOpen()) {
    std::size_t pos = 0;
    while (buf.size() - pos >= kHeader) {
      const WireHeader h = DecodeHeader(buf.data() + pos);
      if (h.type_ != kResponse || h.pad_ != 0 || h.len_ > kMaxPayloadBytes) {
        stream_.Close().IgnoreError();
        break;
      }
      if (buf.size() - pos - kHeader < h.len_) {
        break;
      }
      const std::byte* pl = buf.data() + pos + kHeader;
      auto it = pending_.find(h.req_id_);
      if (it != pending_.end()) {
        Pending* p = it->second;
        p->result_.assign(pl, pl + h.len_);
        p->done_ = true;
        pending_.erase(it);
        if (p->waiter_) {
          ThisWorker().self_->Enqueue(p->waiter_);
        }
      }
      pos += kHeader + h.len_;
    }
    if (pos != 0) {
      buf.erase(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(pos));
    }

    auto r = co_await stream_.ReadSome(chunk);
    if (!r.ok() || *r == 0) break;
    buf.insert(buf.end(), chunk.data(), chunk.data() + *r);
  }

  // Connection ended: fail every outstanding call so callers don't hang.
  for (auto& [id, p] : pending_) {
    (void)id;
    p->status_ =
        absl::Status(absl::StatusCode::kUnavailable, "rpc connection closed");
    p->done_ = true;
    if (p->waiter_) {
      ThisWorker().self_->Enqueue(p->waiter_);
    }
  }
  pending_.clear();
  co_return absl::OkStatus();
}

}  // namespace bycorf::rpc
