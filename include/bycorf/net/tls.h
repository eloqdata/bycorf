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

#ifndef BYCORF_NET_TLS_H_
#define BYCORF_NET_TLS_H_

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/runtime/task.h"

namespace bycorf {

class TcpStream;

enum class TlsClientAuth {
  kNo,
  kOptional,
  kRequired,
};

struct TlsServerOptions {
  std::string cert_file_;
  std::string key_file_;
  std::string ca_cert_file_;
  TlsClientAuth client_auth_ = TlsClientAuth::kNo;
};

struct TlsClientOptions {
  std::string ca_cert_file_;
  std::string cert_file_;
  std::string key_file_;
};

class TlsContext {
 public:
  ~TlsContext();

  TlsContext(const TlsContext&) = delete;
  TlsContext& operator=(const TlsContext&) = delete;

  static absl::StatusOr<std::shared_ptr<TlsContext>> CreateServer(
      const TlsServerOptions& options);
  static absl::StatusOr<std::shared_ptr<TlsContext>> CreateClient(
      const TlsClientOptions& options);

 private:
  struct Impl;
  explicit TlsContext(std::unique_ptr<Impl> impl);

  friend class TlsState;
  std::unique_ptr<Impl> impl_;
};

// One live OpenSSL connection. It deliberately owns no socket: encrypted
// bytes cross the existing TcpStream raw io_uring transport through a BIO
// pair. This also makes the TLS state transferable with a handed-off fd.
class TlsState {
 public:
  // Stores the SSL read/write loop in its caller's frame. Ciphertext transport
  // still uses the existing worker-local Tasks. The stream and payload must
  // outlive completion; this operation may only be awaited on their worker.
  class IoAwaitable {
   public:
    IoAwaitable(TlsState& state, TcpStream& stream, std::span<std::byte> buffer)
        : IoAwaitable(state, stream, std::span<const std::byte>(buffer), true) {
    }
    IoAwaitable(TlsState& state, TcpStream& stream,
                std::span<const std::byte> buffer)
        : IoAwaitable(state, stream, buffer, false) {}
    IoAwaitable(const IoAwaitable&) = delete;
    IoAwaitable& operator=(const IoAwaitable&) = delete;
    IoAwaitable(IoAwaitable&&) = delete;
    IoAwaitable& operator=(IoAwaitable&&) = delete;

    bool await_ready() const noexcept { return buffer_.empty(); }
    bool await_suspend(std::coroutine_handle<> awaiting);
    absl::StatusOr<std::size_t> await_resume();

   private:
    enum class Phase { kSsl, kFlushing, kReading };
    IoAwaitable(TlsState& state, TcpStream& stream,
                std::span<const std::byte> buffer, bool read)
        : state_(state), stream_(stream), buffer_(buffer), read_(read) {}
    bool Advance(absl::Status status);
    void OnComplete();

    TlsState& state_;
    TcpStream& stream_;
    std::span<const std::byte> buffer_;
    std::coroutine_handle<> awaiting_{};
    // A Task's callback runs inside final_suspend. Keep that frame alive until
    // the callback returns, even when Advance starts another child immediately.
    Task<absl::Status> completing_;
    Task<absl::Status> pending_;
    std::optional<absl::StatusOr<std::size_t>> result_;
    std::size_t size_ = 0;
    int error_ = 0;
    Phase phase_ = Phase::kSsl;
    bool read_;
    bool advancing_ = false;
  };

  ~TlsState();

  TlsState(const TlsState&) = delete;
  TlsState& operator=(const TlsState&) = delete;

 private:
  struct Impl;
  explicit TlsState(std::unique_ptr<Impl> impl);

  static absl::StatusOr<std::shared_ptr<TlsState>> Create(
      const std::shared_ptr<TlsContext>& context, bool server,
      std::string_view peer_name);

  Task<absl::Status> Handshake(TcpStream& stream);
  IoAwaitable ReadSome(TcpStream& stream, std::span<std::byte> buffer);
  IoAwaitable WriteSome(TcpStream& stream, std::span<const std::byte> buffer);
  Task<absl::Status> Shutdown(TcpStream& stream);
  absl::StatusOr<std::vector<std::string>> PeerCertificateUriSans() const;

  Task<absl::Status> FlushOutput(TcpStream& stream);
  Task<absl::Status> ReadCiphertext(TcpStream& stream);

  friend class TcpStream;
  template <typename>
  friend class StreamOperation;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bycorf

#endif  // BYCORF_NET_TLS_H_
