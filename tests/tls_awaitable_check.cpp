/*
 * Copyright (C) 2026 EloqData Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fcntl.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

#include "bycorf/net/tcp_stream.h"
#include "bycorf/net/tls.h"
#include "bycorf/runtime/runtime.h"

namespace {
using bycorf::Task;
int failures = 0;
void Check(bool ok, const char* message) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL " << message << '\n';
  }
}

// Each process owns its temporary identity; no external openssl executable or
// shared fixture files are needed when sanitizer and normal tests run together.
struct Identity {
  char cert_path[64] = "/tmp/bycorf-tls-cert-XXXXXX";
  char key_path[64] = "/tmp/bycorf-tls-key-XXXXXX";
  bool Create() {
    EVP_PKEY_CTX* gen = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
    EVP_PKEY* key = nullptr;
    if (!gen) return false;
    bool ok = EVP_PKEY_keygen_init(gen) > 0 &&
              EVP_PKEY_CTX_set_rsa_keygen_bits(gen, 2048) > 0 &&
              EVP_PKEY_keygen(gen, &key) > 0;
    EVP_PKEY_CTX_free(gen);
    if (!ok) return false;
    X509* cert = X509_new();
    X509_set_version(cert, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
    X509_gmtime_adj(X509_get_notBefore(cert), -60);
    X509_gmtime_adj(X509_get_notAfter(cert), 3600);
    X509_set_pubkey(cert, key);
    auto* name = X509_get_subject_name(cert);
    X509_NAME_add_entry_by_txt(
        name, "CN", MBSTRING_ASC,
        reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
    X509_set_issuer_name(cert, name);
    ok = X509_sign(cert, key, EVP_sha256()) > 0;
    const int cert_fd = mkstemp(cert_path);
    const int key_fd = mkstemp(key_path);
    if (cert_fd >= 0) {
      FILE* file = fdopen(cert_fd, "w");
      ok &= PEM_write_X509(file, cert) == 1;
      fclose(file);
    } else {
      ok = false;
    }
    if (key_fd >= 0) {
      FILE* file = fdopen(key_fd, "w");
      ok &= PEM_write_PrivateKey(file, key, nullptr, nullptr, 0, nullptr,
                                 nullptr) == 1;
      fclose(file);
    } else {
      ok = false;
    }
    X509_free(cert);
    EVP_PKEY_free(key);
    return ok;
  }
  ~Identity() {
    unlink(cert_path);
    unlink(key_path);
  }
};

struct NextRound {
  bycorf::Worker& worker;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) {
    worker.EnqueueNext(handle);
  }
  void await_resume() const noexcept {}
};

Task<absl::Status> DuplexWriter(bycorf::TcpStream& stream,
                                const std::string& payload, bool& done) {
  auto status = co_await stream.WriteAll(std::as_bytes(std::span(payload)));
  Check(status.ok(), "concurrent TLS writer");
  done = true;
  co_return status;
}

Task<absl::Status> Exercise(bycorf::Worker& worker,
                            std::shared_ptr<bycorf::TlsContext> context,
                            bool abrupt, bool background, bool duplex = false) {
  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0)
    co_return absl::InternalError("socketpair");
  const int capacity = 4096;
  setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &capacity, sizeof(capacity));
  fcntl(sockets[0], F_SETFL, O_NONBLOCK);
  bycorf::Connection initial;
  initial.worker_ = &worker;
  initial.closed_ = false;
  initial.file_.fd_ = sockets[0];
  auto* connection = worker.AddConnection(std::move(initial));
  bycorf::TcpStream stream(connection);
  auto borrow = stream.BorrowStorage();
#if defined(BYCORF_ASAN)
  // Two TLS records still exercise refill and immediate completion without
  // turning sanitizer runs into a million-allocation microbenchmark.
  constexpr std::size_t kBytes = 32 * 1024;
#else
  constexpr std::size_t kBytes = 256 * 1024;
#endif
  std::string payload(kBytes, 'x');
  for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = char(i % 251);
  bool peer_ok = true;
  std::atomic<bool> read_finished{false};
  std::thread peer([&] {
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    SSL* ssl = SSL_new(ctx);
    SSL_set_fd(ssl, sockets[1]);
    peer_ok = SSL_connect(ssl) == 1;
    auto send = [&] {
      std::size_t offset = 0;
      while (peer_ok && offset < payload.size()) {
        const int n = SSL_write(ssl, payload.data() + offset,
                                static_cast<int>(payload.size() - offset));
        if (n <= 0) {
          peer_ok = false;
          break;
        }
        offset += n;
      }
    };
    auto receive = [&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      std::string received(payload.size(), '\0');
      std::size_t offset = 0;
      while (peer_ok && offset < received.size()) {
        const int n = SSL_read(ssl, received.data() + offset,
                               static_cast<int>(received.size() - offset));
        if (n <= 0) {
          peer_ok = false;
          break;
        }
        offset += n;
      }
      peer_ok &= received == payload;
    };
    if (duplex) receive();
    send();
    if (!abrupt) {
      if (!duplex) receive();
      if (peer_ok && SSL_shutdown(ssl) == 0) SSL_shutdown(ssl);
    }
    if (abrupt) {
      while (!read_finished.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    close(sockets[1]);
  });
  bool writer_done = !duplex;
  auto handshake = co_await stream.StartTls(context, true);
  Check(handshake.ok(), "TLS handshake");
  if (!handshake.ok()) std::cerr << handshake << '\n';
  if (handshake.ok()) {
    Check((co_await stream.ReadSome({})).value() == 0, "empty TLS read");
    Check((co_await stream.WriteSome({})).value() == 0, "empty TLS write");
    std::array<iovec, 2> empty{};
    Check((co_await stream.WriteSomeV(empty)).value() == 0, "empty TLS vector");
    if (duplex) {
      worker.Spawn(DuplexWriter(stream, payload, writer_done));
      co_await NextRound{worker};
      Check(connection->write_inflight_, "TLS writer holds the output lock");
    }
    std::string received(payload.size(), '\0');
    auto start = std::chrono::steady_clock::now();
    std::size_t offset = 0;
    // Small reads exercise already-decrypted data as well as record refill.
#if defined(BYCORF_ASAN)
    constexpr std::size_t kReadBytes = 8;
#else
    constexpr std::size_t kReadBytes = 1;
#endif
    while (offset < received.size()) {
      auto result = co_await stream.ReadSome(std::as_writable_bytes(
          std::span(received.data() + offset,
                    std::min(kReadBytes, received.size() - offset))));
      if (!result.ok() || *result == 0) {
        Check(false, "TLS payload read");
        break;
      }
      offset += *result;
    }
    read_finished.store(true);
    const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
    std::cout << "TLS read ns/byte=" << double(nanos) / payload.size()
              << " background=" << background << " abrupt=" << abrupt << '\n';
    Check(received == payload, "TLS record boundaries preserve bytes");
    Check((bycorf::CurrentTaskClass() == bycorf::TaskClass::kBackground) ==
              background,
          "TLS preserves scheduling class");
    if (!abrupt && !duplex) {
      std::array<iovec, 4> vectors{
          {{nullptr, 0},
           {payload.data(), 13},
           {nullptr, 0},
           {payload.data() + 13, payload.size() - 13}}};
      Check((co_await stream.WriteAllV(vectors)).ok(),
            "TLS write with backpressure");
    }
    std::byte byte;
    auto eof = co_await stream.ReadSome(std::span(&byte, 1));
    Check(abrupt ? !eof.ok() : eof.ok() && *eof == 0,
          "TLS distinguishes truncation from close_notify");
    if (abrupt) {
      Check(!(co_await stream.WriteAll(std::as_bytes(std::span(payload)))).ok(),
            "TLS write propagates peer disconnect");
    } else {
      Check((co_await stream.ShutdownTls()).ok(), "TLS shutdown");
    }
  }
  read_finished.store(true);
  stream.Close().IgnoreError();
  if (handshake.ok()) {
    while (!writer_done) co_await NextRound{worker};
  }
  peer.join();
  Check(peer_ok, "TLS peer transfer");
  Check(!connection->write_inflight_, "TLS sends drained");
  co_return absl::OkStatus();
}

Task<absl::Status> Run(bycorf::Worker& worker,
                       std::shared_ptr<bycorf::TlsContext> context,
                       bool background) {
  for (bool abrupt : {false, true}) {
    Check((co_await Exercise(worker, context, abrupt, background)).ok(),
          "TLS exercise");
  }
  Check((co_await Exercise(worker, context, false, background, true)).ok(),
        "TLS reader waits for concurrent ciphertext flush");
  worker.RequestStop();
  co_return absl::OkStatus();
}
}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::cout << std::unitbuf;
  Identity identity;
  if (!identity.Create()) return 1;
  auto context = bycorf::TlsContext::CreateServer(
      {.cert_file_ = identity.cert_path, .key_file_ = identity.key_path});
  if (!context.ok()) {
    std::cerr << context.status() << '\n';
    return 1;
  }
  std::cout << "awaitable sizes: read="
            << sizeof(bycorf::TcpStream::ReadAwaitable)
            << " write=" << sizeof(bycorf::TcpStream::WriteAwaitable) << '\n';
  for (bool background : {false, true}) {
    bycorf::Runtime runtime;
    runtime.Start(1, [&](unsigned, bycorf::Worker& worker) {
      if (!worker.Init().ok()) return 1;
      if (background)
        worker.SpawnBackground(Run(worker, *context, true));
      else
        worker.Spawn(Run(worker, *context, false));
      worker.Run();
      worker.Shutdown();
      return failures == 0 ? 0 : 1;
    });
    runtime.WaitUntilStopped();
    if (runtime.exit_code() != 0) return 1;
  }
  return failures == 0 ? 0 : 1;
}
