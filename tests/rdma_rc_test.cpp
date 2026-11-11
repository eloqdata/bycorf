// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <cstdlib>
#include <optional>

#include "../src/rdma/rc_internal.h"
#include "bycorf/rdma/rc.h"
#include "bycorf/runtime/worker.h"
#ifdef BYCORF_RDMA_FAKE
#include "rdma_fake.h"
#define RcLoop RcLoopFake
#endif

namespace bycorf::rdma::detail {
class RcTestPeer {
 public:
  static std::shared_ptr<Connection> State(RcConnection& c) { return c.state_; }
  static std::shared_ptr<Listener> State(RcListener& l) { return l.state_; }
};
}  // namespace bycorf::rdma::detail
namespace {
using namespace bycorf;
using namespace bycorf::rdma;
using Peer = bycorf::rdma::detail::RcTestPeer;

template <typename T>
Task<absl::Status> Collect(Task<T> task, std::optional<T>* result) {
  result->emplace(co_await std::move(task));
  co_return absl::OkStatus();
}
class RcLoop : public testing::Test {
 protected:
  void SetUp() override {
#ifdef BYCORF_RDMA_FAKE
    rdma_fake::Reset();
    ip = "192.0.2.1";
#else
    ip = std::getenv("BYCORF_RDMA_TEST_IP");
    if (!ip || !*ip) GTEST_SKIP() << "Set BYCORF_RDMA_TEST_IP to opt in";
#endif
    cc.mailbox(0).wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    ASSERT_GE(cc.mailbox(0).wake_fd_, 0);
    worker.BindCrossCore(0, &cc);
    SetThisWorker(0, &cc, &worker);
    ASSERT_TRUE(worker.Init({.recv_buffer_count_ = 0}).ok());
    alarm(15);
    initialized = true;
  }
  void TearDown() override {
    if (!initialized) return;
    alarm(0);
    worker.Shutdown();
    worker.DestroyDetachedTasks();
    MutableThisWorker() = {};
    close(cc.mailbox(0).wake_fd_);
#ifdef BYCORF_RDMA_FAKE
    rdma_fake::CheckEmpty();
#endif
  }
  template <typename Predicate>
  void Until(Predicate done) {
    while (!done()) ASSERT_TRUE(worker.RunOnce(true));
  }
  template <typename T>
  T Run(Task<T> task) {
    std::optional<T> result;
    worker.Spawn(Collect(std::move(task), &result));
    while (!result) worker.RunOnce(true);
    return std::move(*result);
  }
  absl::StatusOr<std::pair<RcConnection, RcConnection>> Pair(
      RcOptions options = {}) {
    auto listener = RcListener::Listen(worker, ip, 0, options);
    if (!listener.ok()) return listener.status();
    auto client =
        Run(RcConnection::Connect(worker, ip, listener->port(), options));
    if (!client.ok()) return client.status();
    auto server = Run(listener->Accept());
    if (!server.ok()) return server.status();
    auto status = Run(listener->Close());
    if (!status.ok()) return status;
    return std::pair(std::move(*client), std::move(*server));
  }
  const char* ip = nullptr;
  bool initialized = false;
  CrossCore cc{1};
  Worker worker;
};

TEST_F(RcLoop, IdleConnectAcceptPrivateDataAndSharedCq) {
  const std::array data{std::byte{0x31}, std::byte{0x72}};
  auto listening = RcListener::Listen(worker, ip, 0, {}, data);
  ASSERT_TRUE(listening.ok()) << listening.status();
  auto listener = std::move(*listening);
  std::optional<absl::StatusOr<RcConnection>> connected, accepted;
  worker.Spawn(
      Collect(RcConnection::Connect(worker, ip, listener.port(), {}, data),
              &connected));
  worker.Spawn(Collect(listener.Accept(), &accepted));
  Until([&] { return connected && accepted; });
  ASSERT_TRUE(connected->ok()) << connected->status();
  ASSERT_TRUE(accepted->ok()) << accepted->status();
  auto client = std::move(**connected), server = std::move(**accepted);
  EXPECT_TRUE(client.IsOpen());
  EXPECT_TRUE(server.IsOpen());
  ASSERT_GE(client.peer_private_data().size(), data.size());
  ASSERT_GE(server.peer_private_data().size(), data.size());
  EXPECT_TRUE(
      std::equal(data.begin(), data.end(), client.peer_private_data().begin()));
  EXPECT_TRUE(
      std::equal(data.begin(), data.end(), server.peer_private_data().begin()));
  EXPECT_EQ(client.protection_domain().native_pd(),
            server.protection_domain().native_pd());
  auto a = Peer::State(client), b = Peer::State(server);
  EXPECT_EQ(a->device->cq, b->device->cq);
  EXPECT_EQ(a->qp->qp_type, IBV_QPT_RC);
  EXPECT_EQ(a->qp->pd, client.protection_domain().native_pd());
  EXPECT_NE(a->id->verbs, a->qp->context);
  auto device = a->device;
  EXPECT_EQ(device->reserved, a->budget + b->budget);
  EXPECT_TRUE(Run(listener.Close()).ok());
  EXPECT_TRUE(server.IsOpen());
  EXPECT_TRUE(Run(client.Close()).ok());
  EXPECT_TRUE(a->closed);
  EXPECT_EQ(a->id, nullptr);
  EXPECT_EQ(a->qp, nullptr);
  Until([&] { return b->closed; });
  EXPECT_FALSE(server.IsOpen());
  EXPECT_FALSE(server.status().ok());
  EXPECT_EQ(device->reserved, 0);
  EXPECT_TRUE(Run(server.Close()).ok());
}

TEST_F(RcLoop, CloseWakesAcceptAndShutdownRetiresPolls) {
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok()) << listener.status();
  std::optional<absl::StatusOr<RcConnection>> accepted;
  worker.Spawn(Collect(listener->Accept(), &accepted));
  worker.RunOnce(false);
  auto state = Peer::State(*listener);
  auto engine = state->engine.lock();
  ASSERT_TRUE(engine->cm_poll.armed);
  EXPECT_TRUE(Run(listener->Close()).ok());
  Until([&] { return accepted.has_value(); });
  EXPECT_TRUE(absl::IsCancelled(accepted->status()));
  engine->BeginShutdown();
  EXPECT_TRUE(engine->cm_poll.armed)
      << "cancel request must retain the poll tag";
  worker.Shutdown();
  EXPECT_FALSE(engine->cm_poll.armed);
  EXPECT_EQ(engine->cm, nullptr);
}

TEST_F(RcLoop, RefusedConnectionReportsCmError) {
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  const auto port = listener->port();
  ASSERT_TRUE(Run(listener->Close()).ok());
  auto result = Run(RcConnection::Connect(worker, ip, port));
  EXPECT_FALSE(result.ok());
  EXPECT_NE(std::string(result.status().message()).find("RDMA_CM_EVENT_"),
            std::string::npos)
      << result.status();
}

TEST_F(RcLoop, SeparateWorkersOwnDistinctContextsAndAcceptedConnections) {
  CrossCore other_cc{1};
  Worker other;
  other_cc.mailbox(0).wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ASSERT_GE(other_cc.mailbox(0).wake_fd_, 0);
  other.BindCrossCore(0, &other_cc);
  std::optional<absl::StatusOr<RcConnection>> client, server;
  auto listener = RcListener::Listen(worker, ip, 0);
  // On assertion failure quiesce both workers before result/Task destruction.
  struct Cleanup {
    Worker& a;
    Worker& b;
    CrossCore& ca;
    CrossCore& cb;
    ~Cleanup() {
      SetThisWorker(0, &cb, &b);
      b.Shutdown();
      b.DestroyDetachedTasks();
      close(cb.mailbox(0).wake_fd_);
      SetThisWorker(0, &ca, &a);
      a.Shutdown();
      a.DestroyDetachedTasks();
    }
  } cleanup{worker, other, cc, other_cc};
  ASSERT_TRUE(listener.ok());
  worker.Spawn(Collect(listener->Accept(), &server));
  SetThisWorker(0, &other_cc, &other);
  ASSERT_TRUE(other.Init({.recv_buffer_count_ = 0}).ok());
  other.Spawn(
      Collect(RcConnection::Connect(other, ip, listener->port()), &client));
  while (!client || !server) {
    SetThisWorker(0, &cc, &worker);
    worker.RunOnce(false);
    SetThisWorker(0, &other_cc, &other);
    other.RunOnce(false);
  }
  ASSERT_TRUE(client->ok()) << client->status();
  ASSERT_TRUE(server->ok()) << server->status();
  auto a = Peer::State(**client), b = Peer::State(**server);
  EXPECT_EQ(a->engine.lock()->worker, &other);
  EXPECT_EQ(b->engine.lock()->worker, &worker);
  EXPECT_NE(a->qp->context, b->qp->context);
  EXPECT_NE(a->device->cq, b->device->cq);
  SetThisWorker(0, &cc, &worker);
  worker.Shutdown();
  EXPECT_TRUE(b->closed);
  SetThisWorker(0, &other_cc, &other);
  while (!a->closed) other.RunOnce(true);
  SetThisWorker(0, &cc, &worker);
}

TEST_F(RcLoop, ConcurrentAcceptFailsAndCloseWakesOriginalWaiter) {
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  std::optional<absl::StatusOr<RcConnection>> first;
  worker.Spawn(Collect(listener->Accept(), &first));
  worker.RunOnce(false);
  auto second = Run(listener->Accept());
  EXPECT_TRUE(absl::IsFailedPrecondition(second.status()));
  EXPECT_TRUE(Run(listener->Close()).ok());
  Until([&] { return first.has_value(); });
  EXPECT_TRUE(absl::IsCancelled(first->status()));
}

TEST_F(RcLoop, InvalidOptionsAndPrivateDataAreRejected) {
  RcOptions options;
  options.connect_timeout = std::chrono::milliseconds(0);
  EXPECT_TRUE(absl::IsInvalidArgument(
      Run(RcConnection::Connect(worker, ip, 123, options)).status()));
  options = {};
  options.send_wr = 0;
  EXPECT_TRUE(absl::IsInvalidArgument(
      RcListener::Listen(worker, ip, 0, options).status()));
  std::array<std::byte, 57> oversized{};
  EXPECT_TRUE(absl::IsInvalidArgument(
      RcListener::Listen(worker, ip, 0, {}, oversized).status()));
  EXPECT_TRUE(absl::IsInvalidArgument(
      Run(RcConnection::Connect(worker, "hostname", 123)).status()));
}

#ifdef BYCORF_RDMA_FAKE
TEST_F(RcLoop, NotifyRecheckRaceAndRepeatedIdleNotifications) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok()) << pair.status();
  auto state = Peer::State(pair->first);
  auto* cq = state->device->cq;
  const int polls = rdma_fake::CqPolls(cq);
  rdma_fake::CompleteOnArm(cq, state->qp->qp_num);
  rdma_fake::Completion(cq, state->qp->qp_num);
  Until([&] { return rdma_fake::AckedCqEvents() >= 2; });
  EXPECT_GE(rdma_fake::CqPolls(cq), polls + 2);
  EXPECT_EQ(rdma_fake::Notifications(cq), 2);
  rdma_fake::Completion(cq, state->qp->qp_num);
  Until([&] { return rdma_fake::AckedCqEvents() == 3; });
  EXPECT_TRUE(pair->first.IsOpen());
}

TEST_F(RcLoop, TimeoutRetiresIdQpAndTimerBeforeReturn) {
  rdma_fake::SuppressConnect(true);
  RcOptions options;
  options.connect_timeout = std::chrono::milliseconds(10);
  auto result = Run(RcConnection::Connect(worker, ip, 23456, options));
  EXPECT_TRUE(absl::IsDeadlineExceeded(result.status())) << result.status();
  EXPECT_EQ(rdma_fake::LiveIds(), 0);
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
  rdma_fake::SuppressConnect(false);
  EXPECT_TRUE(Pair().ok());
}

TEST_F(RcLoop, SharedCqCapacityReleasedOnlyAfterQpRetirement) {
  RcOptions options;
  options.send_wr = options.recv_wr = 4;
  options.cq_entries = 16;
  auto first = Pair(options);
  ASSERT_TRUE(first.ok()) << first.status();
  auto device = Peer::State(first->first)->device;
  EXPECT_EQ(device->reserved, 16);
  auto exhausted = Run(RcConnection::Connect(worker, ip, 12345, options));
  EXPECT_TRUE(absl::IsResourceExhausted(exhausted.status()))
      << exhausted.status();
  EXPECT_EQ(device->reserved, 16);
  EXPECT_TRUE(Run(first->first.Close()).ok());
  EXPECT_TRUE(Run(first->second.Close()).ok());
  EXPECT_EQ(device->reserved, 0);
  EXPECT_TRUE(Pair(options).ok());
}

TEST_F(RcLoop, ProviderRoundedQpCapacityCannotOvercommitCq) {
  rdma_fake::RoundQp(9);
  RcOptions options;
  options.send_wr = options.recv_wr = 4;
  options.cq_entries = 16;
  auto result = Run(RcConnection::Connect(worker, ip, 12345, options));
  EXPECT_TRUE(absl::IsResourceExhausted(result.status())) << result.status();
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
  EXPECT_EQ(rdma_fake::LiveIds(), 0);
}

TEST_F(RcLoop, FailedQpCreationUnwindsAndCanRetry) {
  rdma_fake::FailNextQp();
  auto result = Run(RcConnection::Connect(worker, ip, 12345));
  EXPECT_FALSE(result.ok());
  EXPECT_NE(std::string(result.status().message()).find("ibv_create_qp"),
            std::string::npos);
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
  EXPECT_EQ(rdma_fake::LiveIds(), 0);
  EXPECT_TRUE(Pair().ok());
}

TEST_F(RcLoop, FailedNotificationSetupRetainsNoConnection) {
  rdma_fake::FailNextNotify();
  auto result = Run(RcConnection::Connect(worker, ip, 12345));
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(rdma_fake::LiveIds(), 0);
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
}

TEST_F(RcLoop, CqFailureTerminatesAllSharingConnectionsAfterAck) {
  auto first = Pair(), second = Pair();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto state = Peer::State(first->first);
  rdma_fake::Async(state->qp->context, IBV_EVENT_CQ_ERR, state->qp);
  Until([&] { return !first->first.IsOpen() && !second->first.IsOpen(); });
  EXPECT_EQ(rdma_fake::AckedAsyncEvents(), 1);
  EXPECT_FALSE(first->second.IsOpen());
  EXPECT_FALSE(second->second.IsOpen());
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
  EXPECT_NE(std::string(second->first.status().message())
                .find(ibv_event_type_str(IBV_EVENT_CQ_ERR)),
            std::string::npos);
}

TEST_F(RcLoop, DeviceRemovalWithStatusFailsEveryAffectedConnection) {
  auto first = Pair(), second = Pair();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto state = Peer::State(first->first);
  rdma_fake::Cm(state->id, RDMA_CM_EVENT_DEVICE_REMOVAL, EIO);
  Until([&] { return !first->first.IsOpen() && !second->first.IsOpen(); });
  EXPECT_FALSE(first->second.IsOpen());
  EXPECT_FALSE(second->second.IsOpen());
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
}

TEST_F(RcLoop, QpFailureLeavesOtherConnectionsUsable) {
  auto first = Pair(), second = Pair();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto state = Peer::State(first->first);
  rdma_fake::Async(state->qp->context, IBV_EVENT_QP_FATAL, state->qp);
  Until([&] { return Peer::State(first->second)->closed; });
  EXPECT_EQ(rdma_fake::AckedAsyncEvents(), 1);
  EXPECT_TRUE(second->first.IsOpen());
  EXPECT_TRUE(second->second.IsOpen());
}

TEST_F(RcLoop, WorkerShutdownRetainsSharedPollsUntilRingQuiescence) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  auto state = Peer::State(pair->first);
  auto engine = state->engine.lock();
  auto device = state->device;
  rdma_fake::Completion(device->cq, state->qp->qp_num);
  engine->BeginShutdown();
  EXPECT_TRUE(engine->cm_poll.armed);
  EXPECT_TRUE(device->completion_poll.armed);
  EXPECT_TRUE(device->async_poll.armed);
  EXPECT_NE(device->cq, nullptr);
  worker.Shutdown();
  EXPECT_FALSE(engine->cm_poll.armed);
  EXPECT_FALSE(device->completion_poll.armed);
  EXPECT_FALSE(device->async_poll.armed);
  EXPECT_TRUE(state->closed);
  EXPECT_EQ(rdma_fake::AckedCqEvents(), 1);
}

TEST_F(RcLoop, ListenerCloseJoinsHandshakingChildren) {
  rdma_fake::SuppressEstablish(true);
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  auto client = Run(RcConnection::Connect(worker, ip, listener->port()));
  ASSERT_TRUE(client.ok()) << client.status();
  auto state = Peer::State(*listener);
  ASSERT_EQ(state->children.size(), 1U);
  auto pending = state->children.begin()->second;
  ASSERT_TRUE(pending->timer.armed);
  EXPECT_TRUE(Run(listener->Close()).ok());
  EXPECT_FALSE(pending->timer.armed);
  EXPECT_TRUE(pending->closed);
  EXPECT_EQ(pending->id, nullptr);
  EXPECT_TRUE(state->children.empty());
  Until([&] { return Peer::State(*client)->closed; });
}

TEST_F(RcLoop, CancellationKeepsConnectionUntilTimerTerminalCompletion) {
  rdma_fake::SuppressConnect(true);
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  auto engine = Peer::State(*listener)->engine.lock();
  std::optional<absl::StatusOr<RcConnection>> result;
  worker.Spawn(
      Collect(RcConnection::Connect(worker, ip, listener->port()), &result));
  Until([&] { return rdma_fake::LiveQps() > 0; });
  auto state = engine->connections.begin()->second;
  engine->Close(state, absl::CancelledError("test cancel"));
  EXPECT_TRUE(state->timer.armed);
  EXPECT_NE(state->id, nullptr);
  EXPECT_FALSE(state->closed);
  // An event queued before the original poll retires must not resurrect it.
  rdma_fake::Cm(state->id, RDMA_CM_EVENT_CONNECT_RESPONSE);
  Until([&] { return result.has_value(); });
  EXPECT_TRUE(absl::IsCancelled(result->status()));
  EXPECT_TRUE(state->closed);
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
}

TEST_F(RcLoop, ShutdownWithSuspendedConnectAndAcceptReclaimsFrames) {
  rdma_fake::SuppressConnect(true);
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  auto engine = Peer::State(*listener)->engine.lock();
  std::optional<absl::StatusOr<RcConnection>> connected, accepted;
  worker.Spawn(
      Collect(RcConnection::Connect(worker, ip, listener->port()), &connected));
  worker.Spawn(Collect(listener->Accept(), &accepted));
  Until([&] { return rdma_fake::LiveQps() > 0; });
  auto state = engine->connections.begin()->second;
  worker.Shutdown();
  EXPECT_TRUE(state->closed);
  EXPECT_FALSE(state->timer.armed);
  EXPECT_TRUE(engine->connections.empty());
  worker.DestroyDetachedTasks();
  EXPECT_FALSE(state->connected.waiter);
  EXPECT_FALSE(Peer::State(*listener)->accepted.waiter);
  EXPECT_EQ(rdma_fake::LiveIds(), 0);
}

TEST_F(RcLoop, DeviceFatalAndNotifyRearmFailureTerminateConnections) {
  auto first = Pair();
  ASSERT_TRUE(first.ok());
  auto state = Peer::State(first->first);
  rdma_fake::Async(state->qp->context, IBV_EVENT_DEVICE_FATAL);
  Until([&] { return state->closed; });
  EXPECT_FALSE(first->second.IsOpen());
  // Drive terminal cancelled polls so failed device resources can retire.
  auto engine = state->engine.lock();
  Until([&] { return engine->devices.empty(); });
  auto second = Pair();
  ASSERT_TRUE(second.ok()) << second.status();
  auto state2 = Peer::State(second->first);
  rdma_fake::FailNextNotify();
  rdma_fake::Completion(state2->device->cq, state2->qp->qp_num);
  Until([&] { return state2->closed; });
  EXPECT_FALSE(second->second.IsOpen());
  EXPECT_NE(
      std::string(second->first.status().message()).find("ibv_req_notify_cq"),
      std::string::npos);
}
#endif
}  // namespace
