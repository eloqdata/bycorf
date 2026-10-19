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

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <memory>
#include <utility>

#include "bycorf/io/storage.h"
#include "bycorf/net/connection.h"
#include "runtime_test.h"

namespace {

using namespace std::chrono_literals;
bycorf::Task<absl::Status> CheckStorage(bycorf::Worker& worker) {
  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                 sockets) != 0) {
    ADD_FAILURE() << "create socket pair";
    co_return absl::InternalError("socketpair failed");
  }

  bycorf::Connection initial;
  initial.worker_ = &worker;
  initial.file_.fd_ = sockets[0];
  initial.closed_ = false;
  // An aliasing shared_ptr lets the test observe destruction without reading
  // potentially reclaimed memory. No TLS state is used by this raw connection.
  auto sentinel = std::make_shared<int>(0);
  std::weak_ptr<int> storage_alive = sentinel;
  initial.tls_state_ =
      std::shared_ptr<bycorf::TlsState>(std::move(sentinel), nullptr);
  auto* connection = worker.AddConnection(std::move(initial));
  bycorf::BorrowConnectionStorage(connection);
  bycorf::BorrowConnectionStorage(connection);
  EXPECT_TRUE(worker.EnsureRecvArmed(connection).ok())
      << "arm real pending receive";
  worker.BeginClose(connection, absl::CancelledError("test close"),
                    bycorf::CloseMode::kLocalClose);
  EXPECT_TRUE(fcntl(sockets[0], F_GETFD) == -1 && errno == EBADF)
      << "storage borrow allows immediate transport close";
  close(sockets[1]);

  // Returning to the event loop lets cancellation completions and reclamation
  // run while this session is suspended on a timer instead of socket I/O.
  (void)co_await bycorf::SleepFor(worker, 20ms);
  EXPECT_TRUE(!storage_alive.expired())
      << "closed storage survives suspended session";
  if (!storage_alive.expired()) {
    EXPECT_TRUE(connection->closed_ && !connection->recv_armed_)
        << "receive cancellation drains while storage stays borrowed";
    bycorf::ReleaseConnectionStorage(connection);
    (void)co_await bycorf::SleepFor(worker, 20ms);
    EXPECT_TRUE(!storage_alive.expired())
        << "one remaining borrower prevents reclaim";
    if (!storage_alive.expired()) {
      bycorf::ReleaseConnectionStorage(connection);
      (void)co_await bycorf::SleepFor(worker, 20ms);
      EXPECT_TRUE(storage_alive.expired())
          << "last release permits worker reclamation";
    }
  }
  co_return absl::OkStatus();
}

TEST(ConnectionStorage, BorrowSurvivesCloseUntilLastRelease) {
  bycorf::test::RunOnWorker(CheckStorage);
}

}  // namespace
