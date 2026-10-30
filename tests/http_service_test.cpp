// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "bycorf/net/http_service.h"

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "bycorf/net/tcp_stream.h"
#include "runtime_test.h"

namespace bycorf {
class HttpServiceTestPeer {
 public:
  static Task<absl::Status> Serve(HttpService& service, TcpStream stream) {
    co_return co_await service.Serve(std::move(stream));
  }
};
namespace {
std::string Flatten(const HttpResponse& response) {
  if (response.body_parts_.empty()) return response.body_;
  std::string result;
  for (const auto& part : response.body_parts_)
    result.append(response.Part(part));
  return result;
}

TEST(HttpResponse, BorrowedTextSurvivesOwnedScratchGrowthAndMove) {
  HttpResponse response;
  response.body_ = "prefix";
  constexpr std::string_view shared = "shared";
  response.AppendBorrowedBody(shared);
  response.AppendBody("42");
  response.AppendBody("\n");
  response.AppendBorrowedBody(shared);
  response.AppendBody(std::string(100000, 'x'));
  const auto expected = "prefixshared42\nshared" + std::string(100000, 'x');
  HttpResponse moved = std::move(response);
  EXPECT_EQ(Flatten(moved), expected);
  EXPECT_EQ(moved.BodySize(), expected.size());
  EXPECT_EQ(moved.body_parts_[1].data_, shared.data());
  EXPECT_LT(moved.body_.size(), moved.BodySize());
}

Task<absl::Status> CheckHttp(Worker& worker) {
  HttpService service(0);
  service.Prepare(1);
  const std::string shared(128, 's');
  std::string expected;
  for (unsigned i = 0; i < 4096; ++i) expected += shared + "7\n";
  service.RegisterGet("/parts",
                      [&shared](const HttpRequest&,
                                HttpResponse* response) -> Task<absl::Status> {
                        for (unsigned i = 0; i < 4096; ++i) {
                          response->AppendBorrowedBody(shared);
                          response->AppendBody("7\n");
                        }
                        co_return absl::OkStatus();
                      });
  service.RegisterGet("/fail",
                      [&shared](const HttpRequest&,
                                HttpResponse* response) -> Task<absl::Status> {
                        response->AppendBorrowedBody(shared);
                        response->AppendBody("must not leak");
                        co_return absl::InternalError("test failure");
                      });
  // Repeated requests reuse scratch; a failed handler must discard all pieces.
  for (const auto path :
       {"/parts", "/abort", "/parts", "/fail", "/parts", "/missing"}) {
    int sockets[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0)
      co_return absl::InternalError("socketpair failed");
    int send_buffer = 4096;
    setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer,
               sizeof(send_buffer));
    Connection initial;
    initial.worker_ = &worker;
    initial.file_.fd_ = sockets[0];
    initial.closed_ = false;
    auto* connection = worker.AddConnection(std::move(initial));
    auto lifetime = TcpStream(connection).BorrowStorage();
    std::string received;
    std::atomic<bool> peer_done{false};
    std::thread peer([&] {
      const auto request =
          std::string("GET ") +
          (std::string_view(path) == "/abort" ? "/parts" : path) +
          " HTTP/1.1\r\n\r\n";
      EXPECT_EQ(send(sockets[1], request.data(), request.size(), MSG_NOSIGNAL),
                request.size());
      char buffer[511];
      for (;;) {
        const auto size = read(sockets[1], buffer, sizeof(buffer));
        if (size <= 0) break;
        received.append(buffer, size);
        if (std::string_view(path) == "/abort") break;
      }
      close(sockets[1]);
      peer_done.store(true);
    });
    auto status =
        co_await HttpServiceTestPeer::Serve(service, TcpStream(connection));
    (void)TcpStream(connection).Close();
    while (!peer_done.load()) {
      (void)co_await SleepFor(worker, std::chrono::milliseconds(1));
    }
    peer.join();
    if (std::string_view(path) == "/abort") {
      EXPECT_FALSE(status.ok());
      continue;
    }
    if (!status.ok()) co_return status;
    const auto body = received.find("\r\n\r\n");
    EXPECT_NE(body, std::string::npos);
    if (body == std::string::npos)
      co_return absl::InternalError("missing HTTP header");
    const std::string payload = received.substr(body + 4);
    EXPECT_NE(received.find(
                  "Content-Length: " + std::to_string(payload.size()) + "\r\n"),
              std::string::npos);
    if (std::string_view(path) == "/parts")
      EXPECT_EQ(payload, expected);
    else if (std::string_view(path) == "/fail")
      EXPECT_EQ(payload, "request handler failed\n");
    else
      EXPECT_EQ(payload, "not found\n");
  }
  co_return absl::OkStatus();
}

TEST(HttpService, SegmentsPartialWritesAndErrorReuse) {
  test::RunOnWorker(CheckHttp);
}
}  // namespace
}  // namespace bycorf
