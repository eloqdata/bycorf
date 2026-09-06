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

#ifndef CELER_NET_TLS_H_
#define CELER_NET_TLS_H_

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/runtime/task.h"

namespace celer {

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
  Task<absl::StatusOr<std::size_t>> ReadSome(TcpStream& stream,
                                             std::span<std::byte> buffer);
  Task<absl::StatusOr<std::size_t>> WriteSome(
      TcpStream& stream, std::span<const std::byte> buffer);
  Task<absl::Status> Shutdown(TcpStream& stream);
  absl::StatusOr<std::vector<std::string>> PeerCertificateUriSans() const;

  Task<absl::Status> FlushOutput(TcpStream& stream);
  Task<absl::Status> ReadCiphertext(TcpStream& stream);

  friend class TcpStream;
  std::unique_ptr<Impl> impl_;
};

}  // namespace celer

#endif  // CELER_NET_TLS_H_
