// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <limits>
#include <optional>
#include <thread>
#include <vector>

#include "../example/rdma_demo_session.h"
#include "../src/rdma/rc_internal.h"
#include "bycorf/rdma/rc.h"
#include "bycorf/runtime/worker.h"
#ifdef BYCORF_RDMA_FAKE
#include "bycorf/net/server.h"
#include "bycorf/net/tcp_listener.h"
#include "bycorf/net/tcp_stream.h"
#include "rdma_fake.h"
#include "worker_wait_test_peer.h"
#define RcLoop RcLoopFake
#endif

namespace bycorf::rdma::detail {
class RcTestPeer {
 public:
  static LocalSharedPtr<Connection> State(RcConnection& c) { return c.state_; }
  static LocalSharedPtr<Listener> State(RcListener& l) { return l.state_; }
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
  RegisteredMemorySlice Buffer(RcConnection& connection,
                               std::span<std::byte> bytes,
                               RdmaAccess access = RdmaAccess::kLocalWrite) {
    auto memory = worker.RegisterRdmaMemory(connection.protection_domain(),
                                            bytes, access);
    EXPECT_TRUE(memory.ok()) << memory.status();
    if (!memory.ok()) return {};
    return *memory->Slice(0, bytes.size());
  }
  const char* ip = nullptr;
  bool initialized = false;
  CrossCore cc{2};
  Worker worker;
};

TEST_F(RcLoop, ExampleExercisesAllOpcodesAndJoinsPeerClose) {
  for (std::size_t bytes : {1U, 65536U, 1048576U}) {
    SCOPED_TRACE(bytes);
    auto pair = Pair();
    ASSERT_TRUE(pair.ok()) << pair.status();
    std::optional<absl::Status> server, client;
    worker.Spawn(Collect(
        example::RunRdmaDemo(worker, pair->second, true, bytes), &server));
    worker.Spawn(Collect(
        example::RunRdmaDemo(worker, pair->first, false, bytes), &client));
    Until([&] { return server && client; });
    EXPECT_TRUE(server->ok()) << *server;
    EXPECT_TRUE(client->ok()) << *client;
    EXPECT_TRUE(Peer::State(pair->first)->closed);
    EXPECT_TRUE(Peer::State(pair->second)->closed);
  }
}

TEST_F(RcLoop, ExampleMismatchRetiresPendingReceives) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok()) << pair.status();
  std::optional<absl::Status> server, client;
  worker.Spawn(
      Collect(example::RunRdmaDemo(worker, pair->second, true, 64), &server));
  worker.Spawn(
      Collect(example::RunRdmaDemo(worker, pair->first, false, 32), &client));
  Until([&] { return server && client; });
  EXPECT_TRUE(absl::IsInvalidArgument(*client)) << *client;
  EXPECT_FALSE(server->ok());
  EXPECT_TRUE(Peer::State(pair->first)->closed);
  EXPECT_TRUE(Peer::State(pair->second)->closed);
}

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

TEST_F(RcLoop, BidirectionalSendRecvOnSharedCq) {
  auto first = Pair(), second = Pair();
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(second.ok()) << second.status();
  std::array<std::array<std::byte, 8>, 4> source{};
  std::array<std::array<std::byte, 16>, 4> destination{};
  std::array<RcConnection*, 4> peers{&first->first, &first->second,
                                     &second->first, &second->second};
  std::array<std::optional<absl::StatusOr<RcCompletion>>, 4> sent, received;
  for (unsigned i = 0; i < peers.size(); ++i) {
    std::fill(source[i].begin(), source[i].end(),
              std::byte{static_cast<unsigned char>(i + 1)});
    auto send = Buffer(*peers[i], source[i], RdmaAccess::kNone);
    auto recv = Buffer(*peers[i ^ 1], destination[i]);
    worker.Spawn(Collect(peers[i ^ 1]->Recv(recv), &received[i]));
    worker.Spawn(Collect(peers[i]->Send(send), &sent[i]));
    // Both lazy adapters must own their invocation-time descriptors.
    send = {};
    recv = {};
  }
  Until([&] {
    return std::all_of(sent.begin(), sent.end(),
                       [](auto& r) { return r.has_value(); }) &&
           std::all_of(received.begin(), received.end(),
                       [](auto& r) { return r.has_value(); });
  });
  for (unsigned i = 0; i < peers.size(); ++i) {
    ASSERT_TRUE(sent[i]->ok()) << sent[i]->status();
    ASSERT_TRUE(received[i]->ok()) << received[i]->status();
    EXPECT_EQ((*sent[i])->bytes, source[i].size());
    EXPECT_EQ((*received[i])->bytes, source[i].size());
    EXPECT_TRUE(
        std::equal(source[i].begin(), source[i].end(), destination[i].begin()));
    EXPECT_EQ(destination[i].back(), std::byte{0});
  }
}

TEST_F(RcLoop, ReceiveValidatesRegistrationBeforePosting) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 8> bytes{};
  auto readonly = Buffer(pair->first, bytes, RdmaAccess::kNone);
  EXPECT_TRUE(
      absl::IsPermissionDenied(pair->first.SubmitRecv(readonly).status()));
  EXPECT_TRUE(absl::IsFailedPrecondition(pair->first.SubmitSend({}).status()));
  auto send = pair->first.SubmitSend(readonly);
  ASSERT_TRUE(send.ok());
  EXPECT_TRUE(send->Cancel().ok());
  EXPECT_FALSE(Run(send->Wait()).ok());
}

TEST_F(RcLoop, ReadWriteWithoutReceiveCreditsAndLazyDescriptorSnapshots) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok()) << pair.status();
  std::array<std::byte, 8> input{}, output{};
  std::array<std::byte, 32> remote_bytes{};
  for (unsigned i = 0; i < input.size(); ++i)
    input[i] = static_cast<std::byte>(i + 1);
  auto source = Buffer(pair->first, input, RdmaAccess::kNone);
  auto target = Buffer(pair->first, output);
  auto remote_memory =
      Buffer(pair->second, remote_bytes,
             RdmaAccess::kLocalWrite | RdmaAccess::kRemoteRead |
                 RdmaAccess::kRemoteWrite);
  auto exported = remote_memory.RemoteRegion(RdmaAccess::kRemoteRead |
                                             RdmaAccess::kRemoteWrite);
  ASSERT_TRUE(exported.ok());
  auto region = *exported->Slice(5, 12);
  auto writing = pair->first.Write(source, region);
  source = {};
  region = {};
  auto written = Run(std::move(writing));
  ASSERT_TRUE(written.ok()) << written.status();
  EXPECT_EQ(written->kind, RcCompletionKind::kWrite);
  EXPECT_EQ(written->bytes, input.size());
  EXPECT_FALSE(written->immediate);
  EXPECT_TRUE(std::equal(input.begin(), input.end(), remote_bytes.begin() + 5));
  EXPECT_EQ(remote_bytes[4], std::byte{0});
  EXPECT_EQ(remote_bytes[13], std::byte{0});
  region = *exported->Slice(5, input.size());
  auto reading = pair->first.Read(target, region);
  target = {};
  region = {};
  auto read = Run(std::move(reading));
  ASSERT_TRUE(read.ok()) << read.status();
  EXPECT_EQ(read->kind, RcCompletionKind::kRead);
  EXPECT_EQ(read->bytes, input.size());
  EXPECT_FALSE(read->immediate);
  EXPECT_EQ(input, output);
  EXPECT_EQ(Peer::State(pair->second)->recv.posted, 0);
  EXPECT_TRUE(Peer::State(pair->second)->recv.pending.empty());
}

TEST_F(RcLoop, ImmediateMessagesAndWriteNotificationsUseDifferentDestinations) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 8> input{std::byte{1}, std::byte{2}, std::byte{3},
                                 std::byte{4}};
  std::array<std::byte, 16> message{}, remote_bytes{};
  std::array<std::byte, 1> notification{std::byte{0x7f}};
  auto source = Buffer(pair->first, input, RdmaAccess::kNone);
  auto receive = Buffer(pair->second, message);
  auto notify = Buffer(pair->second, notification);
  auto remote_memory =
      Buffer(pair->second, remote_bytes,
             RdmaAccess::kLocalWrite | RdmaAccess::kRemoteWrite);
  auto exported = remote_memory.RemoteRegion(RdmaAccess::kRemoteWrite);
  ASSERT_TRUE(exported.ok());
  const auto region = *exported->Slice(3, input.size());
  for (std::uint32_t imm : {0U, 0x01020304U, 0xffffffffU}) {
    auto receiving = pair->second.SubmitRecv(receive);
    ASSERT_TRUE(receiving.ok());
    auto sent = Run(pair->first.SendWithImm(source, imm));
    ASSERT_TRUE(sent.ok()) << sent.status();
    EXPECT_EQ(sent->kind, RcCompletionKind::kSend);
    EXPECT_EQ(sent->bytes, input.size());
    EXPECT_FALSE(sent->immediate);
    auto received = Run(receiving->Wait());
    ASSERT_TRUE(received.ok());
    EXPECT_EQ(received->kind, RcCompletionKind::kRecv);
    EXPECT_EQ(received->bytes, input.size());
    ASSERT_TRUE(received->immediate.has_value());
    EXPECT_EQ(*received->immediate, imm);
    EXPECT_TRUE(std::equal(input.begin(), input.end(), message.begin()));
#ifdef BYCORF_RDMA_FAKE
    EXPECT_EQ(rdma_fake::WireImmediate(), htonl(imm));
#endif
    auto notifying = pair->second.SubmitRecv(notify);
    ASSERT_TRUE(notifying.ok());
    auto written = Run(pair->first.WriteWithImm(source, region, imm));
    ASSERT_TRUE(written.ok()) << written.status();
    EXPECT_EQ(written->kind, RcCompletionKind::kWrite);
    EXPECT_EQ(written->bytes, input.size());
    EXPECT_FALSE(written->immediate);
    auto notified = Run(notifying->Wait());
    ASSERT_TRUE(notified.ok()) << notified.status();
    EXPECT_EQ(notified->kind, RcCompletionKind::kRecvWriteWithImm);
    EXPECT_EQ(notified->bytes, input.size());
    ASSERT_TRUE(notified->immediate.has_value());
    EXPECT_EQ(*notified->immediate, imm);
    EXPECT_EQ(notification.front(), std::byte{0x7f});
    EXPECT_TRUE(
        std::equal(input.begin(), input.end(), remote_bytes.begin() + 3));
    EXPECT_EQ(remote_bytes[2], std::byte{0});
    EXPECT_EQ(remote_bytes[11], std::byte{0});
#ifdef BYCORF_RDMA_FAKE
    EXPECT_EQ(rdma_fake::WireImmediate(), htonl(imm));
#endif
  }
  // A plain receive never reports stale immediate data from a previous CQE.
  auto receiving = pair->second.SubmitRecv(receive);
  ASSERT_TRUE(receiving.ok());
  ASSERT_TRUE(Run(pair->first.Send(source)).ok());
  auto plain = Run(receiving->Wait());
  ASSERT_TRUE(plain.ok());
  EXPECT_EQ(plain->kind, RcCompletionKind::kRecv);
  EXPECT_FALSE(plain->immediate);
}

#ifdef BYCORF_RDMA_FAKE
TEST_F(RcLoop, ExampleShutdownQuiescesFrameOwnedPayload) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  rdma_fake::HoldTransfers(true);
  std::optional<absl::Status> server, client;
  worker.Spawn(Collect(example::RunRdmaDemo(worker, pair->second, true, 65536),
                       &server));
  worker.Spawn(Collect(example::RunRdmaDemo(worker, pair->first, false, 65536),
                       &client));
  for (int i = 0; i < 8; ++i) worker.RunOnce(false);
  EXPECT_FALSE(server);
  EXPECT_FALSE(client);
  EXPECT_GT(rdma_fake::LiveMrs(), 0);
  worker.Shutdown();
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
  worker.DestroyDetachedTasks();
  EXPECT_EQ(rdma_fake::LiveMrs(), 0);
}

TEST_F(RcLoop, MixedSqFifoSnapshotsAndQueuedCancellation) {
  RcOptions options;
  options.send_wr = options.recv_wr = 1;
  auto pair = Pair(options);
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 4> input{std::byte{10}, std::byte{20}, std::byte{30},
                                 std::byte{40}};
  std::array<std::byte, 2> remote_bytes{};
  std::array<std::byte, 1> output{};
  std::array<std::byte, 3> messages{std::byte{0}, std::byte{0},
                                    std::byte{0x7f}};
  auto source = Buffer(pair->first, input, RdmaAccess::kNone);
  auto target = Buffer(pair->first, output);
  auto receives = Buffer(pair->second, messages);
  auto remote_memory =
      Buffer(pair->second, remote_bytes,
             RdmaAccess::kLocalWrite | RdmaAccess::kRemoteRead |
                 RdmaAccess::kRemoteWrite);
  auto region = *remote_memory.RemoteRegion(RdmaAccess::kRemoteRead |
                                            RdmaAccess::kRemoteWrite);
  auto send = pair->first.SubmitSend(*source.Slice(0, 1));
  auto write = pair->first.SubmitWrite(*source.Slice(1, 1), region);
  auto cancelled = pair->first.SubmitWrite(*source.Slice(3, 1), region);
  auto read = pair->first.SubmitRead(target, region);
  std::uint32_t imm = 0x01020304;
  auto send_imm = pair->first.SubmitSendWithImm(*source.Slice(2, 1), imm);
  region = *region.Slice(1, 1);
  auto write_imm =
      pair->first.SubmitWriteWithImm(*source.Slice(3, 1), region, imm + 1);
  ASSERT_TRUE(send.ok() && write.ok() && cancelled.ok() && read.ok() &&
              send_imm.ok() && write_imm.ok());
  EXPECT_EQ(Peer::State(pair->first)->send.posted, 1);
  EXPECT_EQ(Peer::State(pair->first)->send.pending.size(), 5);
  EXPECT_TRUE(absl::IsResourceExhausted(
      pair->first.TrySubmitRead(target, region).status()));
  EXPECT_TRUE(absl::IsResourceExhausted(
      pair->first.TrySubmitWrite(*source.Slice(0, 1), region).status()));
  EXPECT_TRUE(absl::IsResourceExhausted(
      pair->first.TrySubmitSendWithImm(source, imm).status()));
  EXPECT_TRUE(absl::IsResourceExhausted(
      pair->first.TrySubmitWriteWithImm(*source.Slice(0, 1), region, imm)
          .status()));
  EXPECT_TRUE(cancelled->Cancel().ok());
  region = {};
  imm = 0;
  source = target = {};
  std::vector<RcOperation> receiving;
  for (unsigned i = 0; i < messages.size(); ++i) {
    auto op = pair->second.SubmitRecv(*receives.Slice(i, 1));
    ASSERT_TRUE(op.ok());
    receiving.push_back(std::move(*op));
  }
  Until([&] { return write_imm->ready() && receiving.back().ready(); });
  for (auto* op : {&*send, &*write, &*read, &*send_imm, &*write_imm}) {
    ASSERT_TRUE(op->result().ok()) << op->result().status();
    EXPECT_EQ(op->result()->bytes, 1);
    EXPECT_FALSE(op->result()->immediate);
  }
  EXPECT_EQ(read->result()->kind, RcCompletionKind::kRead);
  EXPECT_EQ(write->result()->kind, RcCompletionKind::kWrite);
  EXPECT_EQ(output[0], std::byte{20});
  EXPECT_EQ(remote_bytes[0], std::byte{20});
  EXPECT_EQ(remote_bytes[1], std::byte{40});
  EXPECT_EQ(messages[0], std::byte{10});
  EXPECT_EQ(messages[1], std::byte{30});
  EXPECT_EQ(messages[2], std::byte{0x7f});
  EXPECT_FALSE(receiving[0].result()->immediate);
  EXPECT_EQ(receiving[1].result()->kind, RcCompletionKind::kRecv);
  EXPECT_EQ(receiving[1].result()->immediate, 0x01020304U);
  EXPECT_EQ(receiving[2].result()->kind, RcCompletionKind::kRecvWriteWithImm);
  EXPECT_EQ(receiving[2].result()->immediate, 0x01020305U);
  EXPECT_TRUE(absl::IsCancelled(cancelled->result().status()));
  EXPECT_TRUE(Peer::State(pair->first)->operations.empty());
  EXPECT_TRUE(Peer::State(pair->second)->operations.empty());
}

TEST_F(RcLoop, WriteWithImmWaitsForExplicitReceiveCredit) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 4> input{std::byte{42}}, output{};
  std::array<std::byte, 1> notification{std::byte{0x7f}};
  auto source = Buffer(pair->first, input, RdmaAccess::kNone);
  auto remote_memory = Buffer(
      pair->second, output, RdmaAccess::kLocalWrite | RdmaAccess::kRemoteWrite);
  auto region = *remote_memory.RemoteRegion(RdmaAccess::kRemoteWrite);
  auto write = pair->first.TrySubmitWriteWithImm(source, region, 123);
  ASSERT_TRUE(write.ok());
  for (int i = 0; i < 8; ++i) worker.RunOnce(false);
  EXPECT_FALSE(write->ready());
  EXPECT_EQ(rdma_fake::Posts(true), 0);
  auto receive = pair->second.TrySubmitRecv(Buffer(pair->second, notification));
  ASSERT_TRUE(receive.ok());
  EXPECT_TRUE(Run(write->Wait()).ok());
  auto result = Run(receive->Wait());
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->kind, RcCompletionKind::kRecvWriteWithImm);
  EXPECT_EQ(result->immediate, 123U);
  EXPECT_EQ(output, input);
  EXPECT_EQ(notification[0], std::byte{0x7f});
}

TEST_F(RcLoop, RemoteRangesReadPermissionsAndNegotiatedCapacityFailBeforePost) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 8> local{}, remote_bytes{};
  auto writable = Buffer(pair->first, local);
  auto readonly = Buffer(pair->first, local, RdmaAccess::kNone);
  auto memory = Buffer(pair->second, remote_bytes,
                       RdmaAccess::kLocalWrite | RdmaAccess::kRemoteRead |
                           RdmaAccess::kRemoteWrite);
  const auto region =
      *memory.RemoteRegion(RdmaAccess::kRemoteRead | RdmaAccess::kRemoteWrite);
  const int posts = rdma_fake::Posts(false);
  for (const RemoteMemoryRegion bad :
       {RemoteMemoryRegion{},
        RemoteMemoryRegion{region.address, 7, region.rkey},
        RemoteMemoryRegion{std::numeric_limits<std::uint64_t>::max() - 1, 8,
                           region.rkey}}) {
    EXPECT_TRUE(absl::IsInvalidArgument(
        pair->first.SubmitRead(writable, bad).status()));
    EXPECT_TRUE(absl::IsInvalidArgument(
        pair->first.SubmitWrite(readonly, bad).status()));
    EXPECT_TRUE(absl::IsInvalidArgument(
        pair->first.SubmitWriteWithImm(readonly, bad, 1).status()));
  }
  EXPECT_TRUE(absl::IsPermissionDenied(
      pair->first.SubmitRead(readonly, region).status()));
  auto state = Peer::State(pair->first);
  const auto depth = state->initiator_depth;
  state->initiator_depth = 0;
  EXPECT_TRUE(absl::IsFailedPrecondition(
      pair->first.SubmitRead(writable, region).status()));
  state->initiator_depth = depth;
  EXPECT_EQ(rdma_fake::Posts(false), posts);
  EXPECT_TRUE(state->operations.empty());
}

TEST_F(RcLoop, RemoteAccessErrorsCompleteWithoutRetryAndCloseQp) {
  for (int failure = 0; failure < 3; ++failure) {
    auto pair = Pair();
    ASSERT_TRUE(pair.ok());
    std::array<std::byte, 4> local{}, remote_bytes{};
    auto buffer = Buffer(pair->first, local);
    // Export a descriptor for read-only remote memory, then deliberately issue
    // WRITE or corrupt its key/range to exercise provider completion failures.
    auto memory = Buffer(pair->second, remote_bytes, RdmaAccess::kRemoteRead);
    auto region = *memory.RemoteRegion(RdmaAccess::kRemoteRead);
    if (failure == 1) region.rkey ^= 0x80000000U;
    if (failure == 2) ++region.address;
    auto op = failure == 0 ? pair->first.TrySubmitWrite(buffer, region)
                           : pair->first.TrySubmitRead(buffer, region);
    ASSERT_TRUE(op.ok()) << op.status();
    auto result = Run(op->Wait());
    EXPECT_TRUE(absl::IsUnavailable(result.status()));
    EXPECT_NE(std::string(result.status().message()).find("remote access"),
              std::string::npos);
    EXPECT_FALSE(pair->first.IsOpen());
    Until([&] { return Peer::State(pair->second)->closed; });
    EXPECT_EQ(remote_bytes, (std::array<std::byte, 4>{}));
  }
}

TEST_F(RcLoop, NewOpcodesRetainLeasesThroughFlushAndShutdown) {
  for (bool shutdown : {false, true}) {
    rdma_fake::HoldTransfers(true);
    RcOptions options;
    options.send_wr = 4;
    options.recv_wr = 1;
    auto pair = Pair(options);
    ASSERT_TRUE(pair.ok());
    std::array<std::byte, 8> local{}, remote_bytes{};
    std::array<std::byte, 2> notifications{};
    auto local_memory = Buffer(pair->first, local);
    auto remote_memory =
        Buffer(pair->second, remote_bytes,
               RdmaAccess::kLocalWrite | RdmaAccess::kRemoteRead |
                   RdmaAccess::kRemoteWrite);
    auto receives = Buffer(pair->second, notifications);
    auto region = *remote_memory.RemoteRegion(RdmaAccess::kRemoteRead |
                                              RdmaAccess::kRemoteWrite);
    std::vector<RcOperation> ops;
    for (unsigned i = 0; i < 8; ++i) {
      auto slice = *local_memory.Slice(i, 1);
      auto remote = *region.Slice(i, 1);
      absl::StatusOr<RcOperation> op = absl::UnknownError("test operation");
      switch (i % 4) {
        case 0:
          op = pair->first.SubmitRead(slice, remote);
          break;
        case 1:
          op = pair->first.SubmitWrite(slice, remote);
          break;
        case 2:
          op = pair->first.SubmitSendWithImm(slice, i);
          break;
        case 3:
          op = pair->first.SubmitWriteWithImm(slice, remote, i);
          break;
      }
      ASSERT_TRUE(op.ok());
      ops.push_back(std::move(*op));
    }
    for (unsigned i = 0; i < notifications.size(); ++i) {
      auto op = pair->second.SubmitRecv(*receives.Slice(i, 1));
      ASSERT_TRUE(op.ok());
      ops.push_back(std::move(*op));
    }
    local_memory = receives = {};
    auto state = Peer::State(pair->first);
    EXPECT_EQ(state->send.posted, 4);
    EXPECT_EQ(state->send.pending.size(), 4);
    if (shutdown) {
      worker.Shutdown();
    } else {
      // A posted cancellation closes this QP and flushes every opcode.
      EXPECT_TRUE(ops.front().Cancel().ok());
      Until([&] { return Peer::State(pair->second)->closed; });
    }
    for (auto& op : ops) {
      ASSERT_TRUE(op.ready());
      EXPECT_FALSE(op.result().ok());
      EXPECT_TRUE(op.Cancel().ok());
    }
    EXPECT_TRUE(state->operations.empty());
    EXPECT_EQ(rdma_fake::LiveMrs(), 1);  // caller still owns the remote MR
    remote_memory = {};
    EXPECT_EQ(rdma_fake::LiveMrs(), 0);
  }
}

TEST_F(RcLoop, AsyncAdaptersRetryBothSmallQueuesWithoutCallerRetries) {
  RcOptions options;
  options.send_wr = options.recv_wr = 1;
  auto pair = Pair(options);
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 64> input{}, output{};
  for (unsigned i = 0; i < input.size(); ++i)
    input[i] = static_cast<std::byte>(i);
  auto source = Buffer(pair->first, input, RdmaAccess::kNone);
  auto target = Buffer(pair->second, output);
  std::array<std::optional<absl::StatusOr<RcCompletion>>, 64> sent, received;
  for (unsigned i = 0; i < input.size(); ++i) {
    worker.Spawn(Collect(pair->first.Send(*source.Slice(i, 1)), &sent[i]));
    worker.Spawn(Collect(pair->second.Recv(*target.Slice(i, 1)), &received[i]));
  }
  source = target = {};
  Until([&] { return sent.back() && received.back(); });
  for (unsigned i = 0; i < input.size(); ++i) {
    ASSERT_TRUE(sent[i] && received[i]);
    ASSERT_TRUE(sent[i]->ok()) << sent[i]->status();
    ASSERT_TRUE(received[i]->ok()) << received[i]->status();
    EXPECT_EQ((*sent[i])->bytes, 1);
    EXPECT_EQ((*received[i])->bytes, 1);
  }
  EXPECT_EQ(output, input);
  EXPECT_EQ(rdma_fake::Posts(false), input.size());
  EXPECT_EQ(rdma_fake::Posts(true), input.size());
  EXPECT_EQ(rdma_fake::LiveMrs(), 0);
}

TEST_F(RcLoop, QueueFifoSnapshotsCancellationAndOtherConnectionProgress) {
  RcOptions options;
  options.send_wr = options.recv_wr = 1;
  auto pair = Pair(options), other = Pair(options);
  ASSERT_TRUE(pair.ok());
  ASSERT_TRUE(other.ok());
  std::array<std::byte, 4> source{std::byte{11}, std::byte{22}, std::byte{33},
                                  std::byte{44}};
  std::array<std::byte, 4> destination{};
  auto send_buffer = Buffer(pair->first, source, RdmaAccess::kNone);
  auto recv_buffer = Buffer(pair->second, destination);
  std::vector<RcOperation> sends, recvs;
  for (unsigned i = 0; i < 4; ++i) {
    auto descriptor = *send_buffer.Slice(i, 1);
    auto op = pair->first.SubmitSend(descriptor);
    ASSERT_TRUE(op.ok()) << op.status();
    sends.push_back(std::move(*op));
    descriptor = *send_buffer.Slice(0, 4);
  }
  EXPECT_EQ(Peer::State(pair->first)->send.posted, 1);
  EXPECT_EQ(Peer::State(pair->first)->send.pending.size(), 3);
  EXPECT_TRUE(absl::IsResourceExhausted(
      pair->first.TrySubmitSend(send_buffer).status()));
  EXPECT_FALSE(sends.front().ready());
  EXPECT_TRUE(sends[1].Cancel().ok());
  EXPECT_TRUE(absl::IsCancelled(sends[1].result().status()));
  // SQ blockage does not block the RQ or another connection on the shared CQ.
  std::array<std::byte, 4> opposite_buffer{};
  auto opposite =
      pair->first.TrySubmitRecv(Buffer(pair->first, opposite_buffer));
  ASSERT_TRUE(opposite.ok());
  auto other_recv =
      other->second.SubmitRecv(Buffer(other->second, destination));
  ASSERT_TRUE(other_recv.ok());
  EXPECT_TRUE(Run(other->first.Send(Buffer(other->first, source))).ok());
  EXPECT_EQ(Run(other_recv->Wait())->bytes, 4);
  std::fill(destination.begin(), destination.end(), std::byte{0});
  for (unsigned i = 0; i < 4; ++i) {
    auto descriptor = *recv_buffer.Slice(i, 1);
    auto op = pair->second.SubmitRecv(descriptor);
    ASSERT_TRUE(op.ok());
    recvs.push_back(std::move(*op));
    descriptor = {};
  }
  EXPECT_TRUE(absl::IsResourceExhausted(
      pair->second.TrySubmitRecv(recv_buffer).status()));
  EXPECT_TRUE(recvs[1].Cancel().ok());
  send_buffer = recv_buffer = {};
  Until([&] { return sends.back().ready() && recvs.back().ready(); });
  EXPECT_EQ(destination[0], source[0]);
  EXPECT_EQ(destination[1], std::byte{0});
  EXPECT_EQ(destination[2], source[2]);
  EXPECT_EQ(destination[3], source[3]);
  for (unsigned i : {0U, 2U, 3U}) {
    EXPECT_EQ(sends[i].result()->bytes, 1);
    EXPECT_EQ(recvs[i].result()->bytes, 1);
  }
  EXPECT_EQ(Peer::State(pair->first)->send.posted, 0);
  EXPECT_TRUE(Peer::State(pair->first)->send.pending.empty());
  EXPECT_EQ(Peer::State(pair->second)->recv.posted, 0);
  EXPECT_TRUE(Peer::State(pair->second)->recv.pending.empty());
  EXPECT_TRUE(opposite->Cancel().ok());
}

TEST_F(RcLoop, ProviderPostErrorsAreTerminalWithoutCapacityRetry) {
  RcOptions options;
  options.send_wr = options.recv_wr = 1;
  auto pair = Pair(options);
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 8> bytes{}, output{};
  auto send = Buffer(pair->first, bytes);
  auto recv = Buffer(pair->second, output);
  for (int error : {EINVAL, ENOMEM, EAGAIN}) {
    for (bool receive : {false, true}) {
      const int posts = rdma_fake::Posts(receive);
      rdma_fake::FailNextPost(error, receive);
      auto op = receive ? pair->second.SubmitRecv(recv)
                        : pair->first.SubmitSend(send);
      ASSERT_FALSE(op.ok());
      EXPECT_EQ(rdma_fake::Posts(receive), posts + 1);
      EXPECT_TRUE(Peer::State(pair->first)->operations.empty());
      EXPECT_TRUE(Peer::State(pair->second)->operations.empty());
    }
  }
  auto first = pair->first.SubmitSend(send);
  auto failed = pair->first.SubmitSend(send);
  auto last = pair->first.SubmitSend(send);
  ASSERT_TRUE(first.ok() && failed.ok() && last.ok());
  auto receive = pair->second.SubmitRecv(recv);
  ASSERT_TRUE(receive.ok());
  rdma_fake::FailNextPost(ENOMEM);
  Until([&] { return failed->ready(); });
  EXPECT_FALSE(failed->result().ok());
  EXPECT_TRUE(first->result().ok());
  EXPECT_FALSE(last->ready());
  EXPECT_EQ(Run(pair->second.Recv(recv))->bytes, bytes.size());
  EXPECT_EQ(Run(last->Wait())->bytes, bytes.size());
}

TEST_F(RcLoop, CloseRetiresPostedAndQueuedOperationsExactlyOnce) {
  for (bool drop_flush : {false, true}) {
    rdma_fake::DropFlush(drop_flush);
    rdma_fake::HoldTransfers(true);
    RcOptions options;
    options.send_wr = options.recv_wr = 1;
    auto pair = Pair(options);
    ASSERT_TRUE(pair.ok());
    std::array<std::byte, 4> a{}, b{};
    auto sa = Buffer(pair->first, a), sb = Buffer(pair->second, b);
    std::vector<RcOperation> operations;
    for (unsigned i = 0; i < 2; ++i) {
      for (auto* peer : {&pair->first, &pair->second}) {
        auto& slice = peer == &pair->first ? sa : sb;
        auto send = peer->SubmitSend(*slice.Slice(i, 1));
        auto recv = peer->SubmitRecv(*slice.Slice(i + 2, 1));
        ASSERT_TRUE(send.ok() && recv.ok());
        operations.push_back(std::move(*send));
        operations.push_back(std::move(*recv));
      }
    }
    std::array<int, 8> counts{};
    auto count = [](Task<absl::StatusOr<RcCompletion>> wait,
                    int* count) -> Task<absl::Status> {
      auto result = co_await std::move(wait);
      EXPECT_FALSE(result.ok());
      ++*count;
      co_return absl::OkStatus();
    };
    for (unsigned i = 0; i < operations.size(); ++i)
      worker.Spawn(count(operations[i].Wait(), &counts[i]));
    worker.RunOnce(false);
    sa = sb = {};
    EXPECT_EQ(rdma_fake::LiveMrs(), 2);
    EXPECT_TRUE(Run(pair->first.Close()).ok());
    Until([&] {
      return std::all_of(counts.begin(), counts.end(),
                         [](int n) { return n == 1; });
    });
    for (auto& op : operations) {
      EXPECT_TRUE(op.ready());
      EXPECT_TRUE(op.Cancel().ok());
    }
    EXPECT_TRUE(Run(pair->second.Close()).ok());
    EXPECT_EQ(rdma_fake::LiveMrs(), 0);
    EXPECT_TRUE(Peer::State(pair->first)->operations.empty());
    EXPECT_TRUE(Peer::State(pair->second)->operations.empty());
    EXPECT_TRUE(Peer::State(pair->first)->engine.lock()->operations.empty());
    for (int count : counts) EXPECT_EQ(count, 1);
  }
}

TEST_F(RcLoop, ShutdownStopsDmaBeforeReclaimingSuspendedTransferFrames) {
  rdma_fake::HoldTransfers(true);
  RcOptions options;
  options.send_wr = options.recv_wr = 1;
  auto pair = Pair(options);
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 4> bytes{};
  auto memory = Buffer(pair->first, bytes);
  auto posted = pair->first.SubmitSend(*memory.Slice(0, 1));
  auto queued = pair->first.SubmitSend(*memory.Slice(1, 1));
  auto receive = pair->first.SubmitRecv(*memory.Slice(2, 1));
  ASSERT_TRUE(posted.ok() && queued.ok() && receive.ok());
  std::optional<absl::StatusOr<RcCompletion>> result;
  worker.Spawn(Collect(pair->first.Recv(*memory.Slice(3, 1)), &result));
  worker.RunOnce(false);
  auto engine = Peer::State(pair->first)->engine.lock();
  EXPECT_EQ(engine->operations.size(), 4);
  memory = {};
  worker.Shutdown();
  EXPECT_EQ(rdma_fake::LiveQps(), 0);
  EXPECT_EQ(rdma_fake::LiveMrs(), 0);
  EXPECT_TRUE(engine->operations.empty());
  EXPECT_EQ(engine->worker, nullptr);
  for (auto* op : {&*posted, &*queued, &*receive}) {
    EXPECT_TRUE(op->ready());
    EXPECT_TRUE(absl::IsCancelled(op->result().status()));
    EXPECT_TRUE(op->Cancel().ok());
  }
  EXPECT_FALSE(result.has_value());  // shutdown does not dispatch callbacks
  worker.DestroyDetachedTasks();
  EXPECT_FALSE(RcListener::Listen(worker, ip, 0).ok());
  pair = absl::CancelledError("release handles after shutdown");
}

TEST_F(RcLoop, ShutdownReclaimsFrameOwnedPayloadAfterDmaAndCallbacksStop) {
  rdma_fake::HoldTransfers(true);
  RcOptions options;
  options.send_wr = options.recv_wr = 1;
  auto pair = Pair(options);
  ASSERT_TRUE(pair.ok());
  bool reclaimed = false;
  auto run = [](Worker* worker, RcConnection* connection,
                bool* reclaimed) -> Task<absl::Status> {
    struct Payload {
      bool* reclaimed;
      std::array<std::byte, 8> bytes{};
      ~Payload() {
        EXPECT_EQ(rdma_fake::LiveQps(), 0);
        EXPECT_EQ(rdma_fake::LiveMrs(), 0);
        *reclaimed = true;
      }
    } payload{reclaimed};
    auto memory =
        worker->RegisterRdmaMemory(connection->protection_domain(),
                                   payload.bytes, RdmaAccess::kLocalWrite);
    if (!memory.ok()) co_return memory.status();
    auto posted = connection->SubmitSend(*memory->Slice(0, 1));
    auto queued = connection->SubmitSend(*memory->Slice(1, 1));
    auto recv = connection->SubmitRecv(*memory->Slice(2, 1));
    if (!posted.ok() || !queued.ok() || !recv.ok())
      co_return absl::InternalError("test admission failed");
    auto result = co_await connection->Recv(*memory->Slice(3, 1));
    ADD_FAILURE() << "shutdown must not resume this coroutine";
    co_return result.status();
  };
  worker.Spawn(run(&worker, &pair->first, &reclaimed));
  worker.RunOnce(false);
  EXPECT_EQ(Peer::State(pair->first)->operations.size(), 4);
  worker.Shutdown();
  EXPECT_FALSE(reclaimed);
  worker.DestroyDetachedTasks();
  EXPECT_TRUE(reclaimed);
}

TEST_F(RcLoop, QpAndCqFailuresRetireDataOperationsAndPreserveFailureScope) {
  for (auto failure : {IBV_EVENT_QP_FATAL, IBV_EVENT_CQ_ERR}) {
    RcOptions options;
    options.send_wr = options.recv_wr = 1;
    auto pair = Pair(options), other = Pair(options);
    ASSERT_TRUE(pair.ok() && other.ok());
    std::array<std::byte, 4> bytes{};
    auto memory = Buffer(pair->first, bytes);
    auto posted = pair->first.SubmitSend(*memory.Slice(0, 1));
    auto queued = pair->first.SubmitSend(*memory.Slice(1, 1));
    auto recv = pair->first.SubmitRecv(*memory.Slice(2, 1));
    auto queued_recv = pair->first.SubmitRecv(*memory.Slice(3, 1));
    ASSERT_TRUE(posted.ok() && queued.ok() && recv.ok() && queued_recv.ok());
    memory = {};
    auto state = Peer::State(pair->first);
    rdma_fake::Async(state->qp->context, failure, state->qp);
    Until([&] { return state->closed; });
    for (auto* op : {&*posted, &*queued, &*recv, &*queued_recv}) {
      ASSERT_TRUE(op->ready());
      EXPECT_TRUE(absl::IsUnavailable(op->result().status()));
    }
    EXPECT_EQ(rdma_fake::LiveMrs(), 0);
    if (failure == IBV_EVENT_QP_FATAL) {
      std::array<std::byte, 4> output{};
      auto receive = other->second.SubmitRecv(Buffer(other->second, output));
      ASSERT_TRUE(receive.ok());
      EXPECT_TRUE(Run(other->first.Send(Buffer(other->first, bytes))).ok());
      EXPECT_TRUE(Run(receive->Wait()).ok());
    } else {
      EXPECT_FALSE(other->first.IsOpen());
      EXPECT_FALSE(other->second.IsOpen());
    }
    EXPECT_TRUE(Run(pair->first.Close()).ok());
    EXPECT_TRUE(Run(pair->second.Close()).ok());
    EXPECT_TRUE(Run(other->first.Close()).ok());
    EXPECT_TRUE(Run(other->second.Close()).ok());
  }
}

TEST_F(RcLoop, CompletionErrorClosesQpAndRetiresQueuedWork) {
  RcOptions options;
  options.send_wr = options.recv_wr = 1;
  auto pair = Pair(options);
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 8> source{};
  std::array<std::byte, 1> destination{};
  auto buffer = Buffer(pair->second, destination);
  auto recv = pair->second.SubmitRecv(buffer);
  auto queued = pair->second.SubmitRecv(buffer);
  ASSERT_TRUE(recv.ok() && queued.ok());
  auto send = pair->first.SubmitSend(Buffer(pair->first, source));
  ASSERT_TRUE(send.ok());
  Until([&] { return queued->ready(); });
  EXPECT_TRUE(absl::IsUnavailable(recv->result().status()));
  EXPECT_EQ(recv->result().status(), queued->result().status());
  EXPECT_FALSE(pair->second.IsOpen());
}

TEST_F(RcLoop, WorkerPollPathsRespectCqeBudgetAndPreventSleep) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  auto state = Peer::State(pair->first);
  auto* cq = state->device->cq;
  constexpr auto budget = bycorf::rdma::detail::Engine::kCqPollBudget;
  for (unsigned i = 0; i < 4 * budget; ++i)
    rdma_fake::Completion(cq, state->qp->qp_num);
  auto before = rdma_fake::PendingCompletions(cq);
  ASSERT_TRUE(worker.RunOnce(true));
  auto after = rdma_fake::PendingCompletions(cq);
  EXPECT_LT(after, before);
  EXPECT_LE(before - after, budget);
  before = after;
  ASSERT_TRUE(WorkerWaitTestPeer::BusyPoll(worker));
  after = rdma_fake::PendingCompletions(cq);
  EXPECT_LT(after, before);
  EXPECT_LE(before - after, budget);
  before = after;
  bool waited = false;
  ASSERT_TRUE(WorkerWaitTestPeer::Wait(worker, [&](auto phase) {
    waited |= phase == WorkerWaitTestPeer::Phase::kWaiting;
  }));
  EXPECT_FALSE(waited);
  after = rdma_fake::PendingCompletions(cq);
  EXPECT_LT(after, before);
  EXPECT_LE(before - after, budget);
  Until([&] { return rdma_fake::PendingCompletions(cq) == 0; });
}

TEST_F(RcLoop, PollBudgetRotatesAcrossDevicesAndSurvivesDeviceRemoval) {
  std::vector<std::pair<RcConnection, RcConnection>> pairs;
  std::array<ibv_cq*, 3> cqs{};
  for (unsigned i = 0; i < cqs.size(); ++i) {
    rdma_fake::SelectDevice(i);
    auto pair = Pair();
    ASSERT_TRUE(pair.ok());
    cqs[i] = Peer::State(pair->first)->device->cq;
    pairs.push_back(std::move(*pair));
  }
  constexpr auto budget = bycorf::rdma::detail::Engine::kCqPollBudget;
  for (unsigned i = 0; i < cqs.size(); ++i)
    for (unsigned j = 0; j < 2 * budget; ++j)
      rdma_fake::Completion(cqs[i], Peer::State(pairs[i].first)->qp->qp_num);
  auto engine = Peer::State(pairs[0].first)->engine.lock();
  for (unsigned round = 0; round < 3; ++round) {
    std::size_t before = 0, after = 0;
    for (auto* cq : cqs) before += rdma_fake::PendingCompletions(cq);
    ASSERT_TRUE(engine->PollCqs());
    for (auto* cq : cqs) after += rdma_fake::PendingCompletions(cq);
    EXPECT_EQ(before - after, budget);
  }
  for (auto* cq : cqs) EXPECT_EQ(rdma_fake::PendingCompletions(cq), budget);
  auto failed = Peer::State(pairs[1].first);
  rdma_fake::Async(failed->qp->context, IBV_EVENT_DEVICE_FATAL);
  Until([&] { return engine->devices.size() == 2; });
  EXPECT_FALSE(pairs[1].first.IsOpen());
  EXPECT_TRUE(pairs[0].first.IsOpen());
  EXPECT_TRUE(pairs[2].first.IsOpen());
  Until([&] {
    return rdma_fake::PendingCompletions(cqs[0]) == 0 &&
           rdma_fake::PendingCompletions(cqs[2]) == 0;
  });
}

TEST_F(RcLoop, BusyRdmaAllowsTcpAndPeerMessagesToProgress) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  auto state = Peer::State(pair->first);
  auto* cq = state->device->cq;
  TcpListener listener;
  ASSERT_TRUE(listener.Bind(&worker, "127.0.0.1", 0).ok());
  sockaddr_in address{};
  socklen_t length = sizeof(address);
  ASSERT_EQ(getsockname(listener.NativeFd(),
                        reinterpret_cast<sockaddr*>(&address), &length),
            0);
  auto tcp = Run(ConnectTcp(worker, "127.0.0.1", ntohs(address.sin_port),
                            std::chrono::seconds(1)));
  ASSERT_TRUE(tcp.ok());
  auto accepted = Run(listener.Accept());
  ASSERT_TRUE(accepted.ok());
  TcpStream server(*accepted);
  ASSERT_TRUE(listener.Close().ok());
  // A peer worker publishes through an actual SPSC lane. Its ring is not
  // needed: this worker stays busy and therefore does not require a wake CQE.
  Worker peer;
  peer.BindCrossCore(1, &cc);
  unsigned messages = 0;
  std::array<std::byte, 8> input{std::byte{0x6a}}, output{};
  std::array<std::byte, 8> rdma_input{std::byte{0x39}}, rdma_output{};
  auto source = Buffer(pair->first, rdma_input);
  auto destination = Buffer(pair->second, rdma_output);
  std::optional<absl::Status> written;
  std::optional<absl::StatusOr<std::size_t>> read;
  worker.Spawn(Collect(server.ReadSome(output), &read));
  worker.Spawn(Collect(tcp->WriteAll(input), &written));
  for (unsigned round = 0; round < 256 && (!read || !written || !messages);
       ++round) {
    // Keep more than one round of CQEs queued even while the ring processes
    // readable completion-channel events.
    while (rdma_fake::PendingCompletions(cq) <
           2 * bycorf::rdma::detail::Engine::kCqPollBudget)
      rdma_fake::Completion(cq, state->qp->qp_num);
    auto recv = pair->second.SubmitRecv(destination);
    auto send = pair->first.SubmitSend(source);
    ASSERT_TRUE(recv.ok());
    ASSERT_TRUE(send.ok());
    SetThisWorker(1, &cc, &peer);
    PostNotification(&cc, 0,
                     {.context_ = &messages,
                      .run_fn_ = +[](void* value, std::uint64_t) noexcept {
                        ++*static_cast<unsigned*>(value);
                      }});
    SetThisWorker(0, &cc, &worker);
    ASSERT_TRUE(worker.RunOnce(false));
    EXPECT_GT(rdma_fake::PendingCompletions(cq), 0U);
  }
  ASSERT_TRUE(read.has_value());
  ASSERT_TRUE(written.has_value());
  EXPECT_TRUE(written->ok());
  ASSERT_TRUE(read->ok());
  EXPECT_EQ(**read, input.size());
  EXPECT_EQ(output, input);
  EXPECT_EQ(rdma_output, rdma_input);
  EXPECT_GT(messages, 0U);
  EXPECT_TRUE(tcp->Close().ok());
  EXPECT_TRUE(server.Close().ok());
  // Retire kernel callbacks before stack-owned listener state disappears.
  worker.Shutdown();
}

TEST_F(RcLoop, ReceiveCompletionWakesSleepingWorkerWithoutInlineResume) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  for (int i = 0; i < 8; ++i) worker.RunOnce(false);
  std::array<std::byte, 4> input{std::byte{7}}, output{};
  auto recv = pair->second.SubmitRecv(Buffer(pair->second, output));
  ASSERT_TRUE(recv.ok());
  std::optional<absl::StatusOr<RcCompletion>> received;
  worker.Spawn(Collect(recv->Wait(), &received));
  worker.RunOnce(false);
  auto source = Buffer(pair->first, input);
  std::optional<RcOperation> send;
  std::jthread wake;
  bool waiting = false;
  // Produce the completion on the owner after the final CQ recheck, then
  // delay only the eventfd wake on another thread. Active polling must not
  // mask a broken sleep handshake by consuming the completion beforehand.
  for (int i = 0; i < 8; ++i) worker.RunOnce(false);
  const auto start = std::chrono::steady_clock::now();
  ASSERT_TRUE(WorkerWaitTestPeer::Wait(worker, [&](auto phase) {
    if (phase != WorkerWaitTestPeer::Phase::kWaiting) return;
    waiting = true;
    const int fd =
        rdma_fake::DeferNotification(Peer::State(pair->first)->device->cq);
    auto submitted = pair->first.SubmitSend(source);
    ASSERT_TRUE(submitted.ok());
    send.emplace(std::move(*submitted));
    EXPECT_FALSE(received.has_value());
    EXPECT_FALSE(send->ready());
    wake = std::jthread([fd] {
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
      std::uint64_t one = 1;
      EXPECT_EQ(write(fd, &one, sizeof(one)), sizeof(one));
    });
  }));
  EXPECT_TRUE(waiting);
  Until([&] { return received.has_value(); });
  EXPECT_GE(std::chrono::steady_clock::now() - start,
            std::chrono::milliseconds(15));
  ASSERT_TRUE(received->ok()) << received->status();
  EXPECT_EQ((*received)->bytes, input.size());
  EXPECT_EQ(output, input);
}

TEST_F(RcLoop, RetainedHandlesCanBeReleasedAfterBackendDestruction) {
  CrossCore owned_cc{1};
  owned_cc.mailbox(0).wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ASSERT_GE(owned_cc.mailbox(0).wake_fd_, 0);
  auto owner = std::make_unique<Worker>();
  owner->BindCrossCore(0, &owned_cc);
  SetThisWorker(0, &owned_cc, owner.get());
  struct Cleanup {
    std::unique_ptr<Worker>& owner;
    Worker& previous;
    CrossCore& previous_cc;
    CrossCore& owned_cc;
    ~Cleanup() {
      if (owner) {
        owner->Shutdown();
        owner->DestroyDetachedTasks();
        owner.reset();
      }
      SetThisWorker(0, &previous_cc, &previous);
      close(owned_cc.mailbox(0).wake_fd_);
    }
  } cleanup{owner, worker, cc, owned_cc};
  ASSERT_TRUE(owner->Init({.recv_buffer_count_ = 0}).ok());
  auto listener = RcListener::Listen(*owner, ip, 0);
  ASSERT_TRUE(listener.ok());
  std::optional<absl::StatusOr<RcConnection>> client, server;
  owner->Spawn(
      Collect(RcConnection::Connect(*owner, ip, listener->port()), &client));
  owner->Spawn(Collect(listener->Accept(), &server));
  while (!client || !server) ASSERT_TRUE(owner->RunOnce(true));
  ASSERT_TRUE(client->ok() && server->ok());
  std::array<std::byte, 1> bytes{};
  auto memory = owner->RegisterRdmaMemory((**client).protection_domain(), bytes,
                                          RdmaAccess::kLocalWrite);
  ASSERT_TRUE(memory.ok());
  auto operation = (**client).SubmitRecv(*memory->Slice(0, 1));
  ASSERT_TRUE(operation.ok());
  // Lazy tasks retain states past their public handles and the owning backend.
  auto waiting = operation->Wait();
  auto closing = (**client).Close();
  owner->Shutdown();
  owner->DestroyDetachedTasks();
  owner.reset();
  SetThisWorker(0, &cc, &worker);
  EXPECT_TRUE(operation->ready());
  EXPECT_TRUE(operation->Cancel().ok());
  waiting = {};
  closing = {};
  client.reset();
  server.reset();
  listener = absl::CancelledError("release listener");
  operation = absl::CancelledError("release operation");
  EXPECT_EQ(rdma_fake::LiveMrs(), 1);
  memory = RegisteredMemory{};
  EXPECT_EQ(rdma_fake::LiveMrs(), 0);
}

TEST(RcThreadOwnership, ServerShutdownReclaimsSuspendedAcceptOnOwnerThread) {
  rdma_fake::Reset();
  class ListeningService final : public Service {
   public:
    std::atomic<bool> entered{false};
    void Prepare(unsigned) override {}
    void Stop() noexcept override {}
    Task<absl::Status> Run(Worker& worker, ServiceContext) override {
      auto listener = RcListener::Listen(worker, "192.0.2.1", 0);
      if (!listener.ok()) co_return listener.status();
      entered.store(true, std::memory_order_release);
      // Shutdown destroys this suspended frame and its local listener on the
      // worker, before the control thread destroys Server and Worker objects.
      auto accepted = co_await listener->Accept();
      co_return accepted.status();
    }
  } service;
  {
    Server server;
    server.AddService(&service);
    ASSERT_TRUE(server
                    .Start({.thread_count_ = 1,
                            .pin_workers_ = false,
                            .recv_buffer_count_ = 0})
                    .ok());
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!service.entered.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::yield();
    EXPECT_TRUE(service.entered.load(std::memory_order_acquire));
    server.RequestStop();
    server.WaitUntilStopped();
    EXPECT_EQ(server.exit_code(), 0);
  }
  rdma_fake::CheckEmpty();
}

TEST_F(RcLoop, QueuedAcceptPreservesOrderAndRetainsFailedResults) {
  RcOptions options;
  options.send_wr = options.recv_wr = 4;
  auto listener = RcListener::Listen(worker, ip, 0, options);
  ASSERT_TRUE(listener.ok());
  auto state = Peer::State(*listener);
  auto engine = state->engine.lock();
  std::vector<RcConnection> clients;
  std::vector<LocalWeakPtr<bycorf::rdma::detail::Connection>> queued;
  // Queue a burst before Accept so both the child table and FIFO must grow.
  for (unsigned i = 0; i < 12; ++i) {
    const std::array data{static_cast<std::byte>(i)};
    auto client =
        Run(RcConnection::Connect(worker, ip, listener->port(), options, data));
    ASSERT_TRUE(client.ok()) << client.status();
    clients.push_back(std::move(*client));
    Until([&] { return state->ready.size() == i + 1; });
    queued.push_back(state->children.at(state->ready[i]));
  }
  {
    auto failed = queued[3].lock();
    ASSERT_TRUE(failed);
    const auto id = failed->id;
    rdma_fake::Cm(id, RDMA_CM_EVENT_DISCONNECTED);
    Until([&] { return failed->closed; });
    EXPECT_FALSE(engine->connections.contains(id));
  }
  // Only the listener retains this retired state until Accept reads its error.
  EXPECT_FALSE(queued[3].expired());
  std::vector<RcConnection> accepted;
  for (unsigned i = 0; i < queued.size(); ++i) {
    auto connection = Run(listener->Accept());
    if (i == 3) {
      EXPECT_TRUE(absl::IsUnavailable(connection.status()));
      EXPECT_TRUE(queued[i].expired());
    } else {
      ASSERT_TRUE(connection.ok()) << connection.status();
      ASSERT_FALSE(connection->peer_private_data().empty());
      EXPECT_EQ(connection->peer_private_data().front(),
                static_cast<std::byte>(i));
      accepted.push_back(std::move(*connection));
    }
  }
  EXPECT_TRUE(state->ready.empty());
  EXPECT_TRUE(state->children.empty());
  EXPECT_TRUE(Run(listener->Close()).ok());
  for (const auto& connection : accepted) EXPECT_TRUE(connection.IsOpen());
}

TEST_F(RcLoop, LazyAcceptRetainsStateAfterListenerHandleDestruction) {
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  LocalWeakPtr<bycorf::rdma::detail::Listener> state = Peer::State(*listener);
  auto accepting = listener->Accept();
  *listener = RcListener{};
  EXPECT_FALSE(state.expired());
  EXPECT_TRUE(absl::IsCancelled(Run(std::move(accepting)).status()));
  EXPECT_TRUE(state.expired());
}

TEST_F(RcLoop, ListenerCloseReleasesUnacceptedReadyStates) {
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  auto state = Peer::State(*listener);
  auto client = Run(RcConnection::Connect(worker, ip, listener->port()));
  ASSERT_TRUE(client.ok()) << client.status();
  Until([&] { return !state->ready.empty(); });
  LocalWeakPtr<bycorf::rdma::detail::Connection> queued =
      state->children.at(state->ready.front());
  EXPECT_TRUE(Run(listener->Close()).ok());
  EXPECT_TRUE(state->ready.empty());
  EXPECT_TRUE(state->children.empty());
  EXPECT_TRUE(queued.expired());
  EXPECT_TRUE(absl::IsCancelled(Run(listener->Accept()).status()));
}

TEST_F(RcLoop, BusyPollingDoesNotRearmAndStaleEventsCannotClearNewArm) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  auto state = Peer::State(pair->first);
  auto device = state->device;
  auto engine = state->engine.lock();
  auto* cq = device->cq;
  ASSERT_TRUE(engine->PrepareWait());
  engine->FinishWait();
  const int arms = rdma_fake::NotificationArms(cq);
  const int acks = rdma_fake::AckedCqEvents();
  rdma_fake::Completion(cq, state->qp->qp_num);
  ASSERT_TRUE(engine->PollCqs());
  EXPECT_EQ(rdma_fake::PendingCompletions(cq), 0U);
  // Rearm while the old event is still queued, then dispatch that old event.
  ASSERT_TRUE(engine->PrepareWait());
  engine->OnPoll(device->completion_poll, POLLIN);
  engine->FinishWait();
  EXPECT_EQ(rdma_fake::AckedCqEvents(), acks + 1);
  EXPECT_EQ(rdma_fake::NotificationArms(cq), arms + 1);
  rdma_fake::Completion(cq, state->qp->qp_num);
  for (unsigned i = 0; i < 10; ++i) worker.RunOnce(false);
  EXPECT_EQ(rdma_fake::AckedCqEvents(), acks + 2);
  EXPECT_EQ(rdma_fake::PendingCompletions(cq), 0U);
  EXPECT_EQ(rdma_fake::NotificationArms(cq), arms + 1);
}

TEST_F(RcLoop, CmDeviceCreatedDuringIdleRecheckIsArmed) {
  auto listener = RcListener::Listen(worker, ip, 0);
  ASSERT_TRUE(listener.ok());
  auto engine = Peer::State(*listener)->engine.lock();
  EXPECT_TRUE(engine->devices.empty());
  ASSERT_TRUE(engine->PrepareWait());
  std::optional<absl::StatusOr<RcConnection>> connected;
  worker.Spawn(
      Collect(RcConnection::Connect(worker, ip, listener->port()), &connected));
  // Drive the same CM/ring path used by the recheck without beginning another
  // wait attempt. Device creation must honor the preparation already in force.
  while (!connected) ASSERT_TRUE(worker.RunOnce(false));
  ASSERT_TRUE(connected->ok());
  auto* cq = Peer::State(**connected)->device->cq;
  EXPECT_EQ(rdma_fake::NotificationArms(cq), 1);
  engine->FinishWait();
  EXPECT_TRUE(Run(listener->Close()).ok());
}

TEST_F(RcLoop, CompletionBeforeAndDuringArmAbandonsSleep) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  auto state = Peer::State(pair->first);
  auto* cq = state->device->cq;
  for (bool during_arm : {false, true}) {
    for (int i = 0; i < 8; ++i) worker.RunOnce(false);
    if (during_arm)
      rdma_fake::CompleteOnArm(cq, state->qp->qp_num);
    else
      rdma_fake::Completion(cq, state->qp->qp_num);
    bool waited = false;
    ASSERT_TRUE(WorkerWaitTestPeer::Wait(worker, [&](auto phase) {
      waited |= phase == WorkerWaitTestPeer::Phase::kWaiting;
    }));
    EXPECT_FALSE(waited);
    EXPECT_EQ(rdma_fake::PendingCompletions(cq), 0U);
  }
}

TEST_F(RcLoop, ReceiveRacingEachSleepPhaseIsConsumedOrWakesRing) {
  using Phase = WorkerWaitTestPeer::Phase;
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  std::array<std::byte, 8> input{std::byte{0x53}}, output{};
  auto source = Buffer(pair->first, input);
  auto destination = Buffer(pair->second, output);
  for (auto target : {Phase::kPrepared, Phase::kRechecked, Phase::kParked,
                      Phase::kSubmitted, Phase::kWaiting}) {
    SCOPED_TRACE(static_cast<int>(target));
    for (int i = 0; i < 8; ++i) worker.RunOnce(false);
    output.fill(std::byte{});
    auto recv = pair->second.SubmitRecv(destination);
    ASSERT_TRUE(recv.ok());
    std::optional<RcOperation> send;
    bool waiting = false;
    ASSERT_TRUE(WorkerWaitTestPeer::Wait(worker, [&](auto phase) {
      waiting |= phase == Phase::kWaiting;
      if (phase != target) return;
      auto submitted = pair->first.SubmitSend(source);
      ASSERT_TRUE(submitted.ok());
      send.emplace(std::move(*submitted));
      EXPECT_FALSE(send->ready());
      EXPECT_FALSE(recv->ready());
    }));
    ASSERT_TRUE(send.has_value());
    if (target == Phase::kPrepared) EXPECT_FALSE(waiting);
    Until([&] { return send->ready() && recv->ready(); });
    EXPECT_TRUE(send->result().ok());
    EXPECT_TRUE(recv->result().ok());
    EXPECT_EQ(output, input);
  }
}

TEST_F(RcLoop, AbandonedWaitThenCloseAndShutdownRetiresQueuedNotifications) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  auto state = Peer::State(pair->first);
  auto device = state->device;
  auto engine = state->engine.lock();
  std::array<std::byte, 8> buffer{};
  auto recv = pair->first.SubmitRecv(Buffer(pair->first, buffer));
  ASSERT_TRUE(recv.ok());
  std::optional<absl::StatusOr<RcCompletion>> result;
  worker.Spawn(Collect(recv->Wait(), &result));
  for (int i = 0; i < 8; ++i) worker.RunOnce(false);
  bool waited = false;
  ASSERT_TRUE(WorkerWaitTestPeer::Wait(worker, [&](auto phase) {
    waited |= phase == WorkerWaitTestPeer::Phase::kWaiting;
    if (phase != WorkerWaitTestPeer::Phase::kPrepared) return;
    rdma_fake::Completion(device->cq, state->qp->qp_num);
    engine->Close(state, absl::CancelledError("close during idle recheck"));
  }));
  EXPECT_FALSE(waited);
  const int arms = rdma_fake::NotificationArms(device->cq);
  worker.Shutdown();
  EXPECT_EQ(rdma_fake::NotificationArms(device->cq), arms);
  EXPECT_EQ(rdma_fake::AckedCqEvents(), rdma_fake::Notifications(device->cq));
  EXPECT_FALSE(device->completion_poll.armed);
  EXPECT_TRUE(engine->devices.empty());
  EXPECT_TRUE(state->closed);
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

TEST_F(RcLoop, FailedIdleArmClosesDeviceAndDoesNotSleep) {
  auto pair = Pair();
  ASSERT_TRUE(pair.ok());
  auto state = Peer::State(pair->first);
  rdma_fake::FailNextNotify();
  bool waited = false;
  ASSERT_TRUE(WorkerWaitTestPeer::Wait(worker, [&](auto phase) {
    waited |= phase == WorkerWaitTestPeer::Phase::kWaiting;
  }));
  EXPECT_FALSE(waited);
  EXPECT_FALSE(pair->first.IsOpen());
  Until([&] { return state->closed; });
  EXPECT_NE(
      std::string(pair->first.status().message()).find("ibv_req_notify_cq"),
      std::string::npos);
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
  ASSERT_TRUE(engine->PrepareWait());
  engine->FinishWait();
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
