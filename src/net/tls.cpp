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

#include "celer/net/tls.h"

#include <arpa/inet.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/sync.h"

namespace celer {
namespace {

constexpr std::size_t kTlsIoBufferBytes = 16 * 1024;

std::string DrainErrorQueue() {
  std::string message;
  while (const unsigned long error = ERR_get_error()) {
    std::array<char, 256> text{};
    ERR_error_string_n(error, text.data(), text.size());
    if (!message.empty()) message.append("; ");
    message.append(text.data());
  }
  return message.empty() ? "unknown OpenSSL error" : message;
}

absl::Status TlsError(std::string_view operation) {
  return absl::InternalError(absl::StrCat(operation, ": ", DrainErrorQueue()));
}

bool IsIpAddress(std::string_view value, int* family) {
  in_addr address4{};
  if (::inet_pton(AF_INET, std::string(value).c_str(), &address4) == 1) {
    if (family != nullptr) *family = AF_INET;
    return true;
  }
  in6_addr address6{};
  if (::inet_pton(AF_INET6, std::string(value).c_str(), &address6) == 1) {
    if (family != nullptr) *family = AF_INET6;
    return true;
  }
  return false;
}

absl::Status ConfigureCommonContext(SSL_CTX* context) {
  if (SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION) != 1) {
    return TlsError("failed to set minimum TLS version");
  }
  SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION);
  SSL_CTX_set_mode(context, SSL_MODE_RELEASE_BUFFERS);
  return absl::OkStatus();
}

absl::Status LoadIdentity(SSL_CTX* context, std::string_view cert_file,
                          std::string_view key_file, bool required) {
  if (cert_file.empty() && key_file.empty() && !required) {
    return absl::OkStatus();
  }
  if (cert_file.empty() || key_file.empty()) {
    return absl::InvalidArgumentError(
        "TLS certificate and private key must be configured together");
  }
  if (SSL_CTX_use_certificate_chain_file(context,
                                         std::string(cert_file).c_str()) != 1) {
    return TlsError("failed to load TLS certificate");
  }
  if (SSL_CTX_use_PrivateKey_file(context, std::string(key_file).c_str(),
                                  SSL_FILETYPE_PEM) != 1) {
    return TlsError("failed to load TLS private key");
  }
  if (SSL_CTX_check_private_key(context) != 1) {
    return TlsError("TLS private key does not match certificate");
  }
  return absl::OkStatus();
}

}  // namespace

struct TlsContext::Impl {
  SSL_CTX* context_ = nullptr;
  bool server_ = false;

  ~Impl() {
    if (context_ != nullptr) SSL_CTX_free(context_);
  }
};

TlsContext::TlsContext(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
TlsContext::~TlsContext() = default;

absl::StatusOr<std::shared_ptr<TlsContext>> TlsContext::CreateServer(
    const TlsServerOptions& options) {
  ERR_clear_error();
  SSL_CTX* raw = SSL_CTX_new(TLS_server_method());
  if (raw == nullptr) return TlsError("failed to create server TLS context");
  auto impl = std::make_unique<Impl>();
  impl->context_ = raw;
  impl->server_ = true;

  absl::Status status = ConfigureCommonContext(raw);
  if (status.ok()) {
    status = LoadIdentity(raw, options.cert_file_, options.key_file_, true);
  }
  if (status.ok() && options.client_auth_ != TlsClientAuth::kNo) {
    if (options.ca_cert_file_.empty()) {
      status = absl::InvalidArgumentError(
          "TLS client authentication requires a CA certificate");
    } else if (SSL_CTX_load_verify_locations(raw, options.ca_cert_file_.c_str(),
                                             nullptr) != 1) {
      status = TlsError("failed to load TLS client CA certificate");
    } else {
      STACK_OF(X509_NAME)* client_ca =
          SSL_load_client_CA_file(options.ca_cert_file_.c_str());
      if (client_ca == nullptr) {
        status = TlsError("failed to load TLS client CA names");
      } else {
        SSL_CTX_set_client_CA_list(raw, client_ca);
      }
    }
  }
  if (!status.ok()) return status;

  int verify_mode = SSL_VERIFY_NONE;
  if (options.client_auth_ == TlsClientAuth::kOptional) {
    verify_mode = SSL_VERIFY_PEER;
  } else if (options.client_auth_ == TlsClientAuth::kRequired) {
    verify_mode = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
  }
  SSL_CTX_set_verify(raw, verify_mode, nullptr);
  return std::shared_ptr<TlsContext>(new TlsContext(std::move(impl)));
}

absl::StatusOr<std::shared_ptr<TlsContext>> TlsContext::CreateClient(
    const TlsClientOptions& options) {
  if (options.ca_cert_file_.empty()) {
    return absl::InvalidArgumentError("TLS client requires a CA certificate");
  }
  ERR_clear_error();
  SSL_CTX* raw = SSL_CTX_new(TLS_client_method());
  if (raw == nullptr) return TlsError("failed to create client TLS context");
  auto impl = std::make_unique<Impl>();
  impl->context_ = raw;

  absl::Status status = ConfigureCommonContext(raw);
  if (status.ok()) {
    status = LoadIdentity(raw, options.cert_file_, options.key_file_, false);
  }
  if (status.ok() && SSL_CTX_load_verify_locations(
                         raw, options.ca_cert_file_.c_str(), nullptr) != 1) {
    status = TlsError("failed to load TLS server CA certificate");
  }
  if (!status.ok()) return status;
  SSL_CTX_set_verify(raw, SSL_VERIFY_PEER, nullptr);
  return std::shared_ptr<TlsContext>(new TlsContext(std::move(impl)));
}

struct TlsState::Impl {
  std::shared_ptr<TlsContext> context_;
  SSL* ssl_ = nullptr;
  BIO* network_bio_ = nullptr;
  bool handshake_complete_ = false;
  bool shutdown_started_ = false;
  AsyncMutex output_mutex_;

  ~Impl() {
    if (network_bio_ != nullptr) BIO_free(network_bio_);
    if (ssl_ != nullptr) SSL_free(ssl_);
  }
};

TlsState::TlsState(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
TlsState::~TlsState() = default;

absl::StatusOr<std::vector<std::string>> TlsState::PeerCertificateUriSans()
    const {
  if (!impl_->handshake_complete_) {
    return absl::FailedPreconditionError("TLS handshake is not complete");
  }
  X509* certificate = SSL_get1_peer_certificate(impl_->ssl_);
  if (certificate == nullptr) {
    return absl::UnauthenticatedError("TLS peer supplied no certificate");
  }
  GENERAL_NAMES* names = static_cast<GENERAL_NAMES*>(
      X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr));
  std::vector<std::string> result;
  if (names != nullptr) {
    const int count = sk_GENERAL_NAME_num(names);
    for (int ii = 0; ii < count; ++ii) {
      const GENERAL_NAME* name = sk_GENERAL_NAME_value(names, ii);
      if (name->type != GEN_URI) continue;
      const ASN1_IA5STRING* uri = name->d.uniformResourceIdentifier;
      const unsigned char* data = ASN1_STRING_get0_data(uri);
      const int length = ASN1_STRING_length(uri);
      if (data == nullptr || length < 0 ||
          std::memchr(data, '\0', static_cast<std::size_t>(length)) !=
              nullptr) {
        GENERAL_NAMES_free(names);
        X509_free(certificate);
        return absl::UnauthenticatedError(
            "TLS peer URI SAN contains invalid bytes");
      }
      result.emplace_back(reinterpret_cast<const char*>(data),
                          static_cast<std::size_t>(length));
    }
    GENERAL_NAMES_free(names);
  }
  X509_free(certificate);
  return result;
}

absl::StatusOr<std::shared_ptr<TlsState>> TlsState::Create(
    const std::shared_ptr<TlsContext>& context, bool server,
    std::string_view peer_name) {
  if (context == nullptr || context->impl_ == nullptr ||
      context->impl_->context_ == nullptr) {
    return absl::InvalidArgumentError("TLS context is not initialized");
  }
  if (context->impl_->server_ != server) {
    return absl::InvalidArgumentError("TLS context role does not match stream");
  }

  ERR_clear_error();
  SSL* ssl = SSL_new(context->impl_->context_);
  if (ssl == nullptr) return TlsError("failed to create TLS session");
  BIO* internal = nullptr;
  BIO* network = nullptr;
  if (BIO_new_bio_pair(&internal, 0, &network, 0) != 1) {
    SSL_free(ssl);
    return TlsError("failed to create TLS BIO pair");
  }
  BIO_up_ref(internal);
  SSL_set0_rbio(ssl, internal);
  SSL_set0_wbio(ssl, internal);
  SSL_set_mode(ssl, SSL_MODE_ENABLE_PARTIAL_WRITE |
                        SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |
                        SSL_MODE_RELEASE_BUFFERS);

  if (server) {
    SSL_set_accept_state(ssl);
  } else {
    if (peer_name.empty()) {
      BIO_free(network);
      SSL_free(ssl);
      return absl::InvalidArgumentError(
          "TLS client peer name must not be empty");
    }
    X509_VERIFY_PARAM* verify = SSL_get0_param(ssl);
    if (IsIpAddress(peer_name, nullptr)) {
      if (X509_VERIFY_PARAM_set1_ip_asc(verify,
                                        std::string(peer_name).c_str()) != 1) {
        BIO_free(network);
        SSL_free(ssl);
        return TlsError("failed to configure TLS peer IP verification");
      }
    } else {
      if (SSL_set_tlsext_host_name(ssl, std::string(peer_name).c_str()) != 1 ||
          SSL_set1_host(ssl, std::string(peer_name).c_str()) != 1) {
        BIO_free(network);
        SSL_free(ssl);
        return TlsError("failed to configure TLS peer name verification");
      }
    }
    SSL_set_connect_state(ssl);
  }

  auto impl = std::make_unique<Impl>();
  impl->context_ = context;
  impl->ssl_ = ssl;
  impl->network_bio_ = network;
  return std::shared_ptr<TlsState>(new TlsState(std::move(impl)));
}

Task<absl::Status> TlsState::FlushOutput(TcpStream& stream) {
  if (stream.connection_ == nullptr || stream.connection_->worker_ == nullptr) {
    co_return absl::FailedPreconditionError("TLS stream is not bound");
  }
  co_await impl_->output_mutex_.Lock();
  UnlockGuard unlock(&impl_->output_mutex_, stream.connection_->worker_);

  while (BIO_ctrl_pending(impl_->network_bio_) != 0) {
    const std::size_t pending =
        static_cast<std::size_t>(BIO_ctrl_pending(impl_->network_bio_));
    std::vector<std::byte> ciphertext(std::min(pending, kTlsIoBufferBytes));
    const int consumed = BIO_read(impl_->network_bio_, ciphertext.data(),
                                  static_cast<int>(ciphertext.size()));
    if (consumed <= 0) {
      co_return TlsError("failed to drain TLS output BIO");
    }
    ciphertext.resize(static_cast<std::size_t>(consumed));
    absl::Status sent = co_await stream.WriteRawAll(ciphertext);
    if (!sent.ok()) co_return sent;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> TlsState::ReadCiphertext(TcpStream& stream) {
  std::array<std::byte, kTlsIoBufferBytes> ciphertext{};
  auto read = co_await stream.ReadRawSome(ciphertext);
  if (!read.ok()) co_return read.status();
  if (*read == 0) {
    co_return absl::UnavailableError("TLS peer closed without close_notify");
  }

  std::size_t offset = 0;
  while (offset < *read) {
    const int written =
        BIO_write(impl_->network_bio_, ciphertext.data() + offset,
                  static_cast<int>(*read - offset));
    if (written <= 0) {
      co_return TlsError("failed to feed TLS input BIO");
    }
    offset += static_cast<std::size_t>(written);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> TlsState::Handshake(TcpStream& stream) {
  while (!impl_->handshake_complete_) {
    ERR_clear_error();
    const int result = SSL_do_handshake(impl_->ssl_);
    const int error =
        result == 1 ? SSL_ERROR_NONE : SSL_get_error(impl_->ssl_, result);
    absl::Status flushed = co_await FlushOutput(stream);
    if (!flushed.ok()) co_return flushed;
    if (result == 1) {
      if (SSL_get_verify_result(impl_->ssl_) != X509_V_OK) {
        co_return absl::PermissionDeniedError(
            X509_verify_cert_error_string(SSL_get_verify_result(impl_->ssl_)));
      }
      impl_->handshake_complete_ = true;
      co_return absl::OkStatus();
    }
    if (error == SSL_ERROR_WANT_READ) {
      absl::Status read = co_await ReadCiphertext(stream);
      if (!read.ok()) co_return read;
      continue;
    }
    if (error == SSL_ERROR_WANT_WRITE) continue;
    co_return TlsError("TLS handshake failed");
  }
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::size_t>> TlsState::ReadSome(
    TcpStream& stream, std::span<std::byte> buffer) {
  if (buffer.empty()) co_return std::size_t{0};
  while (true) {
    std::size_t size = 0;
    ERR_clear_error();
    const int result =
        SSL_read_ex(impl_->ssl_, buffer.data(), buffer.size(), &size);
    const int error =
        result == 1 ? SSL_ERROR_NONE : SSL_get_error(impl_->ssl_, result);
    absl::Status flushed = co_await FlushOutput(stream);
    if (!flushed.ok()) co_return flushed;
    if (result == 1) co_return size;
    if (error == SSL_ERROR_ZERO_RETURN) co_return std::size_t{0};
    if (error == SSL_ERROR_WANT_WRITE) continue;
    if (error == SSL_ERROR_WANT_READ) {
      absl::Status read = co_await ReadCiphertext(stream);
      if (!read.ok()) co_return read;
      continue;
    }
    co_return TlsError("TLS read failed");
  }
}

Task<absl::StatusOr<std::size_t>> TlsState::WriteSome(
    TcpStream& stream, std::span<const std::byte> buffer) {
  if (buffer.empty()) co_return std::size_t{0};
  while (true) {
    std::size_t size = 0;
    ERR_clear_error();
    const int result =
        SSL_write_ex(impl_->ssl_, buffer.data(), buffer.size(), &size);
    const int error =
        result == 1 ? SSL_ERROR_NONE : SSL_get_error(impl_->ssl_, result);
    absl::Status flushed = co_await FlushOutput(stream);
    if (!flushed.ok()) co_return flushed;
    if (result == 1) co_return size;
    if (error == SSL_ERROR_WANT_WRITE) continue;
    if (error == SSL_ERROR_WANT_READ) {
      absl::Status read = co_await ReadCiphertext(stream);
      if (!read.ok()) co_return read;
      continue;
    }
    if (error == SSL_ERROR_ZERO_RETURN) {
      co_return absl::UnavailableError("TLS peer closed connection");
    }
    co_return TlsError("TLS write failed");
  }
}

Task<absl::Status> TlsState::Shutdown(TcpStream& stream) {
  if (impl_->shutdown_started_) co_return absl::OkStatus();
  impl_->shutdown_started_ = true;
  ERR_clear_error();
  const int result = SSL_shutdown(impl_->ssl_);
  const int error =
      result >= 0 ? SSL_ERROR_NONE : SSL_get_error(impl_->ssl_, result);
  absl::Status flushed = co_await FlushOutput(stream);
  if (!flushed.ok()) co_return flushed;
  if (result >= 0 || error == SSL_ERROR_WANT_READ ||
      error == SSL_ERROR_WANT_WRITE) {
    co_return absl::OkStatus();
  }
  co_return TlsError("TLS shutdown failed");
}

}  // namespace celer
