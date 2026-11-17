// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <linux/time_types.h>
#include <rdma/rdma_cma.h>

#include <coroutine>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "bycorf/base/inline_ring_queue.h"
#include "bycorf/io/completion.h"
#include "bycorf/rdma/rc.h"

namespace bycorf::rdma::detail {
struct Device;
struct Signal {
  bool ready = false;
  std::coroutine_handle<> waiter{};
  void Publish(Worker* worker);
};

// The waiter is owned by its coroutine, not by an fd tag. Shutdown may destroy
// that coroutine only after the ring is quiesced; this guard detaches it.
struct Wait {
  Signal& signal;
  std::coroutine_handle<> handle{};
  bool conflict = false;
  ~Wait();
  bool await_ready() const noexcept { return signal.ready; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::Status await_resume();
};

struct Poll final : IoCompletion {
  enum class Kind { kCm, kCq, kAsync };
  Engine* engine = nullptr;
  Device* device = nullptr;
  Kind kind = Kind::kCm;
  int fd = -1;
  bool armed = false;
  bool stopping = false;
  absl::Status Arm();
  void Stop();
  void Complete(Worker&, int result, unsigned flags) override;
};

struct Deadline final : IoCompletion {
  Engine* engine = nullptr;
  Connection* connection = nullptr;
  bool armed = false;
  __kernel_timespec expires{};
  void Complete(Worker&, int result, unsigned flags) override;
};

struct Connection {
  LocalWeakPtr<Engine> engine;
  LocalWeakPtr<Listener> listener;
  rdma_cm_id* id = nullptr;
  ibv_qp* qp = nullptr;
  LocalSharedPtr<Device> device;
  std::uint64_t budget = 0;
  RcOptions options;
  std::vector<std::byte> local_data, peer_data;
  Deadline timer;
  std::chrono::steady_clock::time_point expires;
  bool established = false;
  bool cm_started = false;
  std::uint8_t initiator_depth = 1;
  std::uint8_t responder_resources = 1;
  bool handshake_done = false;
  bool closing = false;
  bool queued = false;
  bool closed = false;
  absl::Status error;
  Signal connected, retired;
};

struct Listener {
  LocalWeakPtr<Engine> engine;
  rdma_cm_id* id = nullptr;
  RcOptions options;
  std::vector<std::byte> private_data;
  std::uint16_t port = 0;
  int backlog = 0;
  bool closing = false;
  bool closed = false;
  absl::Status error;
  // Own unaccepted children even after failure removes them from the engine.
  // ready only borrows these states: Accept transfers ownership before erasing
  // a child, and listener retirement clears ready before releasing children.
  absl::flat_hash_map<Connection*, LocalSharedPtr<Connection>> children;
  InlineRingQueue<Connection*, 1> ready;
  Signal accepted, retired;
};

struct Device {
  LocalWeakPtr<Device> self;
  std::shared_ptr<ibv_context> context;
  RdmaProtectionDomain domain;
  ibv_comp_channel* channel = nullptr;
  ibv_cq* cq = nullptr;
  std::uint32_t requested_cqe = 0;
  std::uint64_t reserved = 0;
  absl::Status error;
  Poll completion_poll, async_poll;
  // CM has process-shared contexts. This context is opened privately on the
  // same actual device so its async event stream has exactly one worker reader.
  ~Device();
};

class Engine {
 public:
  LocalWeakPtr<Engine> self;
  explicit Engine(Worker& owner) : worker(&owner) {}
  ~Engine();
  absl::Status Init();
  absl::StatusOr<LocalSharedPtr<Listener>> Listen(
      const sockaddr_storage& address, RcOptions options,
      std::vector<std::byte> data, int backlog);
  absl::StatusOr<LocalSharedPtr<Connection>> Connect(
      const sockaddr_storage& address, RcOptions options,
      std::vector<std::byte> data);
  void Close(LocalSharedPtr<Connection> connection, absl::Status reason);
  void Close(LocalSharedPtr<Listener> listener, absl::Status reason);
  void Progress(LocalSharedPtr<Connection> connection);
  void Progress(LocalSharedPtr<Listener> listener);
  void OnPoll(Poll& poll, int result);
  void DrainCm();
  void DrainCq(Device& device);
  void DrainAsync(Device& device);
  void FailDevice(Device& device, const absl::Status& error);
  void RetireFailedDevice(Device& device);
  void BeginShutdown();
  void AfterRingShutdown();
  bool OnOwner() const;
  Worker* wake_worker() const { return stopping ? nullptr : worker; }

  Worker* worker;
  bool stopping = false;
  absl::Status error;
  rdma_event_channel* cm = nullptr;
  Poll cm_poll;
  absl::flat_hash_map<rdma_cm_id*, LocalSharedPtr<Connection>> connections;
  absl::flat_hash_map<rdma_cm_id*, LocalSharedPtr<Listener>> listeners;
  absl::flat_hash_map<std::string, LocalSharedPtr<Device>> devices;

 private:
  absl::Status StartTimer(const LocalSharedPtr<Connection>& connection);
  absl::Status CreateQp(const LocalSharedPtr<Connection>& connection);
  absl::Status Transition(Connection& connection, ibv_qp_state state);
  absl::StatusOr<LocalSharedPtr<Device>> GetDevice(Connection& connection);
  void HandleCm(rdma_cm_id* id, rdma_cm_id* listen_id, rdma_cm_event_type event,
                int status, std::vector<std::byte> data, rdma_conn_param peer);
  void Established(const LocalSharedPtr<Connection>& connection);
  void FailAll(const absl::Status& error);
  void DrainNotifications(Device& device, bool rearm);
};

absl::Status ErrnoStatus(const char* operation);
void CheckDestroy(int result, const char* operation);
}  // namespace bycorf::rdma::detail
