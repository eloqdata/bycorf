// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <time.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <utility>

#include "absl/strings/str_cat.h"
#include "bycorf/io/rdma_reactor.h"
#include "bycorf/runtime/worker.h"
#include "rc_internal.h"
#include "spdlog/spdlog.h"

namespace bycorf::rdma::detail {
namespace {
absl::Status Nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    return ErrnoStatus("RDMA channel nonblocking setup");
  return absl::OkStatus();
}
bool Again() { return errno == EAGAIN || errno == EWOULDBLOCK; }

template <typename Map>
auto Snapshot(const Map& map) {
  std::vector<typename Map::mapped_type> result;
  result.reserve(map.size());
  for (const auto& [key, value] : map) result.push_back(value);
  return result;
}

absl::Status CmError(rdma_cm_event_type event, int status) {
  // Rejection status can be a transport reason, not errno. Preserve both the
  // event and raw status rather than accidentally interpreting it as errno.
  return absl::UnavailableError(
      absl::StrCat(rdma_event_str(event), " (status ", status, ")"));
}
rdma_conn_param Parameters(const Connection& c) {
  rdma_conn_param p{};
  p.private_data = c.local_data.empty() ? nullptr : c.local_data.data();
  p.private_data_len = static_cast<std::uint8_t>(c.local_data.size());
  p.responder_resources = c.responder_resources;
  p.initiator_depth = c.initiator_depth;
  p.retry_count = 7;
  p.rnr_retry_count = 7;
  p.qp_num = c.qp->qp_num;
  return p;
}
}  // namespace

absl::Status ErrnoStatus(const char* operation) {
  int error = errno;
  return absl::ErrnoToStatus(error ? error : EIO,
                             absl::StrCat(operation, " (errno ", error, ")"));
}
void CheckDestroy(int result, const char* operation) {
  if (result != 0) {
    spdlog::critical("{} failed while retiring RDMA resources: {}", operation,
                     result);
    std::terminate();
  }
}
void Signal::Publish(Worker* worker) {
  if (ready) return;
  ready = true;
  if (worker && waiter) worker->Enqueue(waiter);
}
Wait::~Wait() {
  if (signal.waiter == handle) signal.waiter = {};
}
bool Wait::await_suspend(std::coroutine_handle<> awaiting) {
  if (signal.waiter) {
    conflict = true;
    return false;
  }
  handle = awaiting;
  signal.waiter = awaiting;
  return true;
}
absl::Status Wait::await_resume() {
  return conflict ? absl::FailedPreconditionError("concurrent RDMA wait")
                  : absl::OkStatus();
}

absl::Status Poll::Arm() {
  if (armed || stopping) return absl::OkStatus();
  auto status = engine->worker->SubmitRdmaEventPoll(fd, POLLIN, this);
  if (status.ok()) armed = true;
  return status;
}
void Poll::Stop() {
  if (stopping) return;
  stopping = true;
  if (armed) {
    // Cancellation admission is not retirement. Keep the tag, descriptor and
    // resource owner until Complete, or the explicit post-ring-shutdown phase.
    const auto status = engine->worker->CancelRdmaEventPoll(this);
    if (!status.ok()) engine->worker->RequestStop();
  }
}
void Poll::Complete(Worker&, int result, unsigned) {
  // Failure fanout may remove the device cache entry. This event reference
  // keeps the containing poll alive until the callback has fully returned.
  auto retained = device ? device->shared_from_this() : nullptr;
  armed = false;
  if (!stopping) engine->OnPoll(*this, result);
  // A cancellation CQE is the retirement boundary; cancellation admission
  // alone never permits release of the descriptor or containing Device.
  if (device) engine->RetireFailedDevice(*device);
}
void Deadline::Complete(Worker&, int result, unsigned) {
  auto it = engine->connections.find(connection->id);
  if (it == engine->connections.end()) std::terminate();
  auto retained = it->second;
  armed = false;
  if (!retained->handshake_done) {
    engine->Close(retained, result == -ETIME
                                ? absl::DeadlineExceededError(
                                      "RDMA connection deadline exceeded")
                                : absl::UnknownError(absl::StrCat(
                                      "RDMA deadline completion: ", result)));
  }
  engine->Progress(retained);
}

Device::~Device() {
  if (cq) CheckDestroy(ibv_destroy_cq(cq), "ibv_destroy_cq");
  if (channel)
    CheckDestroy(ibv_destroy_comp_channel(channel), "ibv_destroy_comp_channel");
}
Engine::~Engine() {
  // Normal destruction follows AfterRingShutdown. Failed Init has no accepted
  // poll, and can release its channel directly.
  if (cm) rdma_destroy_event_channel(cm);
}
bool Engine::OnOwner() const { return worker && ThisWorker().self_ == worker; }
absl::Status Engine::Init() {
  cm = rdma_create_event_channel();
  if (!cm) return ErrnoStatus("rdma_create_event_channel");
  auto status = Nonblocking(cm->fd);
  if (!status.ok()) return status;
  cm_poll.engine = this;
  cm_poll.fd = cm->fd;
  return cm_poll.Arm();
}
absl::StatusOr<std::shared_ptr<Engine>> CreateEngine(Worker& worker) {
  if (ThisWorker().self_ != &worker)
    return absl::FailedPreconditionError("RDMA must run on its owner worker");
  auto engine = std::make_shared<Engine>(worker);
  auto status = engine->Init();
  if (!status.ok()) return status;
  return engine;
}
void BeginShutdown(const std::shared_ptr<Engine>& engine) {
  if (engine) engine->BeginShutdown();
}
void AfterRingShutdown(const std::shared_ptr<Engine>& engine) {
  if (engine) engine->AfterRingShutdown();
}

absl::StatusOr<std::shared_ptr<Listener>> Engine::Listen(
    const sockaddr_storage& address, RcOptions options,
    std::vector<std::byte> data, int backlog) {
  if (stopping) return absl::CancelledError("RDMA worker is shutting down");
  if (!OnOwner())
    return absl::FailedPreconditionError("RDMA call on a different worker");
  if (!error.ok()) return error;
  auto listener = std::make_shared<Listener>();
  listener->engine = shared_from_this();
  listener->options = options;
  listener->private_data = std::move(data);
  listener->backlog = backlog;
  if (rdma_create_id(cm, &listener->id, nullptr, RDMA_PS_TCP))
    return ErrnoStatus("rdma_create_id(listener)");
  auto addr = address;
  if (rdma_bind_addr(listener->id, reinterpret_cast<sockaddr*>(&addr)) ||
      rdma_listen(listener->id, backlog)) {
    auto status = ErrnoStatus("RDMA bind/listen");
    CheckDestroy(rdma_destroy_id(listener->id), "rdma_destroy_id(listener)");
    listener->id = nullptr;
    return status;
  }
  listener->port = ntohs(rdma_get_src_port(listener->id));
  listeners.emplace(listener->id, listener);
  return listener;
}
absl::Status Engine::StartTimer(const std::shared_ptr<Connection>& c) {
  c->expires = std::chrono::steady_clock::now() + c->options.connect_timeout;
  c->timer.engine = this;
  c->timer.connection = c.get();
  const auto ms = c->options.connect_timeout.count();
  timespec now{};
  if (clock_gettime(CLOCK_MONOTONIC, &now)) return ErrnoStatus("clock_gettime");
  const auto nanos = now.tv_nsec + (ms % 1000) * 1000000;
  c->timer.expires = {.tv_sec = now.tv_sec + ms / 1000 + nanos / 1000000000,
                      .tv_nsec = nanos % 1000000000};
  auto status = worker->SubmitRdmaDeadline(c->timer.expires, &c->timer);
  if (status.ok()) c->timer.armed = true;
  return status;
}
absl::StatusOr<std::shared_ptr<Connection>> Engine::Connect(
    const sockaddr_storage& address, RcOptions options,
    std::vector<std::byte> data) {
  if (stopping) return absl::CancelledError("RDMA worker is shutting down");
  if (!OnOwner())
    return absl::FailedPreconditionError("RDMA call on a different worker");
  if (!error.ok()) return error;
  auto c = std::make_shared<Connection>();
  c->engine = shared_from_this();
  c->options = options;
  c->local_data = std::move(data);
  if (rdma_create_id(cm, &c->id, nullptr, RDMA_PS_TCP))
    return ErrnoStatus("rdma_create_id(connect)");
  connections.emplace(c->id, c);
  auto status = StartTimer(c);
  auto addr = address;
  if (status.ok() &&
      rdma_resolve_addr(c->id, nullptr, reinterpret_cast<sockaddr*>(&addr),
                        static_cast<int>(options.connect_timeout.count())))
    status = ErrnoStatus("rdma_resolve_addr");
  if (!status.ok()) Close(c, status);
  return c;
}

absl::StatusOr<std::shared_ptr<Device>> Engine::GetDevice(Connection& c) {
  auto* selected = c.id->verbs;
  if (!selected || !selected->device)
    return absl::FailedPreconditionError("CM did not select a verbs device");
  const std::string name = ibv_get_device_name(selected->device);
  if (auto it = devices.find(name); it != devices.end()) {
    if (!it->second->error.ok()) return it->second->error;
    if (it->second->requested_cqe != c.options.cq_entries)
      return absl::InvalidArgumentError(
          "shared RDMA CQ capacity is already fixed");
    return it->second;
  }
  auto device = std::make_shared<Device>();
  auto* opened = ibv_open_device(selected->device);
  if (!opened) return ErrnoStatus("ibv_open_device(RC)");
  device->context =
      std::shared_ptr<ibv_context>(opened, [](ibv_context* context) {
        CheckDestroy(ibv_close_device(context), "ibv_close_device(RC)");
      });
  auto domain = worker->AcquireRdmaDomain(device->context);
  if (!domain.ok()) return domain.status();
  device->domain = *domain;
  ibv_device_attr attrs{};
  int result = ibv_query_device(opened, &attrs);
  if (result) return absl::ErrnoToStatus(result, "ibv_query_device");
  if (c.options.cq_entries > static_cast<unsigned>(attrs.max_cqe))
    return absl::ResourceExhaustedError("requested CQ exceeds device max_cqe");
  device->channel = ibv_create_comp_channel(opened);
  if (!device->channel) return ErrnoStatus("ibv_create_comp_channel");
  auto status = Nonblocking(device->channel->fd);
  if (!status.ok()) return status;
  status = Nonblocking(opened->async_fd);
  if (!status.ok()) return status;
  device->cq = ibv_create_cq(opened, static_cast<int>(c.options.cq_entries),
                             device.get(), device->channel, 0);
  if (!device->cq) return ErrnoStatus("ibv_create_cq");
  device->requested_cqe = c.options.cq_entries;
  device->completion_poll.engine = device->async_poll.engine = this;
  device->completion_poll.device = device->async_poll.device = device.get();
  device->completion_poll.kind = Poll::Kind::kCq;
  device->completion_poll.fd = device->channel->fd;
  device->async_poll.kind = Poll::Kind::kAsync;
  device->async_poll.fd = opened->async_fd;
  // Retain the device BEFORE admitting any fd poll. A later failure must not
  // destroy a tag or fd that the ring may still reference.
  devices.emplace(name, device);
  result = ibv_req_notify_cq(device->cq, 0);
  if (result) status = absl::ErrnoToStatus(result, "ibv_req_notify_cq");
  if (status.ok()) DrainCq(*device);  // arm then recheck, including startup
  if (status.ok()) status = device->error;
  if (status.ok()) status = device->completion_poll.Arm();
  if (status.ok()) status = device->async_poll.Arm();
  if (!status.ok()) {
    FailDevice(*device, status);
    RetireFailedDevice(*device);
    return status;
  }
  return device;
}
absl::Status Engine::Transition(Connection& c, ibv_qp_state state) {
  ibv_qp_attr attr{};
  attr.qp_state = state;
  int mask = 0;
  if (rdma_init_qp_attr(c.id, &attr, &mask))
    return ErrnoStatus("rdma_init_qp_attr");
  if (state == IBV_QPS_RTR) attr.max_dest_rd_atomic = c.responder_resources;
  if (state == IBV_QPS_RTS) attr.max_rd_atomic = c.initiator_depth;
  const int result = ibv_modify_qp(c.qp, &attr, mask);
  return result ? absl::ErrnoToStatus(result, "ibv_modify_qp(RC)")
                : absl::OkStatus();
}
absl::Status Engine::CreateQp(const std::shared_ptr<Connection>& c) {
  auto found = GetDevice(*c);
  if (!found.ok()) return found.status();
  auto device = *found;
  const std::uint64_t requested =
      std::uint64_t(c->options.send_wr) + c->options.recv_wr;
  if (requested >
      static_cast<std::uint64_t>(device->cq->cqe) - device->reserved)
    return absl::ResourceExhaustedError("shared RDMA CQ budget exhausted");
  ibv_qp_init_attr attr{};
  attr.qp_type = IBV_QPT_RC;
  attr.send_cq = attr.recv_cq = device->cq;
  attr.cap.max_send_wr = c->options.send_wr;
  attr.cap.max_recv_wr = c->options.recv_wr;
  attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
  attr.sq_sig_all = 1;
  c->qp = ibv_create_qp(device->domain.native_pd(), &attr);
  if (!c->qp) return ErrnoStatus("ibv_create_qp(RC)");
  // Providers may round capacities up. Until the actual budget is reserved,
  // this QP remains unconnected with no work posted, so it cannot fill the CQ.
  const std::uint64_t actual =
      std::uint64_t(attr.cap.max_send_wr) + attr.cap.max_recv_wr;
  if (actual > static_cast<std::uint64_t>(device->cq->cqe) - device->reserved) {
    CheckDestroy(ibv_destroy_qp(c->qp), "ibv_destroy_qp(over budget)");
    c->qp = nullptr;
    return absl::ResourceExhaustedError(
        "provider QP capacities exceed shared CQ budget");
  }
  c->device = device;
  c->budget = actual;
  device->reserved += actual;
  // External-QP CM mode: never assign id->qp. CM routes/negotiates, while the
  // worker's own context owns QP/PD/MR/CQ and the sole async-event reader.
  return Transition(*c, IBV_QPS_INIT);
}

void Engine::Established(const std::shared_ptr<Connection>& c) {
  if (std::chrono::steady_clock::now() >= c->expires) {
    Close(c, absl::DeadlineExceededError("RDMA connection deadline exceeded"));
    return;
  }
  c->established = true;
  c->handshake_done = true;
  if (c->timer.armed) {
    auto status = worker->CancelRdmaEventPoll(&c->timer);
    if (!status.ok()) {
      Close(c, status);
      return;
    }
  }
  Progress(c);
}
void Engine::HandleCm(rdma_cm_id* id, rdma_cm_id* listen_id,
                      rdma_cm_event_type event, int status,
                      std::vector<std::byte> data, rdma_conn_param peer) {
  if (event == RDMA_CM_EVENT_DEVICE_REMOVAL) {
    // Retain endpoints before fanout: failing the device can erase their map
    // entries and destroy the CM ID. Nonzero status still means device scope.
    auto ci = connections.find(id);
    auto li = listeners.find(id);
    auto connection = ci == connections.end() ? nullptr : ci->second;
    auto listener = li == listeners.end() ? nullptr : li->second;
    const auto error = CmError(event, status);
    if (id->verbs && id->verbs->device) {
      auto found = devices.find(ibv_get_device_name(id->verbs->device));
      if (found != devices.end()) FailDevice(*found->second, error);
    }
    if (connection) Close(connection, error);
    if (listener) Close(listener, error);
    return;
  }
  if (event == RDMA_CM_EVENT_CONNECT_REQUEST) {
    auto it = listeners.find(listen_id);
    if (stopping || it == listeners.end() || it->second->closing ||
        it->second->children.size() >=
            static_cast<std::size_t>(it->second->backlog)) {
      (void)rdma_reject(id, nullptr, 0);
      CheckDestroy(rdma_destroy_id(id), "rdma_destroy_id(rejected request)");
      return;
    }
    auto listener = it->second;
    auto c = std::make_shared<Connection>();
    c->engine = shared_from_this();
    c->listener = listener;
    c->id = id;
    c->options = listener->options;
    c->local_data = listener->private_data;
    c->peer_data = std::move(data);
    c->initiator_depth = std::min<std::uint8_t>(1, peer.responder_resources);
    c->responder_resources = std::min<std::uint8_t>(1, peer.initiator_depth);
    connections.emplace(id, c);
    listener->children.emplace(c.get(), c);
    auto result = StartTimer(c);
    if (result.ok()) result = CreateQp(c);
    if (result.ok()) result = Transition(*c, IBV_QPS_RTR);
    if (result.ok()) result = Transition(*c, IBV_QPS_RTS);
    if (result.ok()) {
      auto param = Parameters(*c);
      if (rdma_accept(c->id, &param))
        result = ErrnoStatus("rdma_accept");
      else
        c->cm_started = true;
    }
    if (!result.ok()) {
      (void)rdma_reject(id, nullptr, 0);
      Close(c, result);
    }
    return;
  }
  auto it = connections.find(id);
  if (it == connections.end()) {
    if (auto listener = listeners.find(id); listener != listeners.end())
      Close(listener->second, CmError(event, status));
    return;
  }
  auto c = it->second;
  if (c->closing) return;
  if (status || event == RDMA_CM_EVENT_REJECTED ||
      event == RDMA_CM_EVENT_CONNECT_ERROR ||
      event == RDMA_CM_EVENT_ADDR_ERROR || event == RDMA_CM_EVENT_ROUTE_ERROR ||
      event == RDMA_CM_EVENT_UNREACHABLE ||
      event == RDMA_CM_EVENT_DISCONNECTED) {
    if (!data.empty()) c->peer_data = std::move(data);
    Close(c, CmError(event, status));
    return;
  }
  absl::Status result;
  switch (event) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      if (rdma_resolve_route(
              id, static_cast<int>(c->options.connect_timeout.count())))
        result = ErrnoStatus("rdma_resolve_route");
      break;
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
      result = CreateQp(c);
      if (result.ok()) {
        auto param = Parameters(*c);
        if (rdma_connect(id, &param))
          result = ErrnoStatus("rdma_connect");
        else
          c->cm_started = true;
      }
      break;
    case RDMA_CM_EVENT_CONNECT_RESPONSE:
      c->peer_data = std::move(data);
      c->initiator_depth =
          std::min(c->initiator_depth, peer.responder_resources);
      c->responder_resources =
          std::min(c->responder_resources, peer.initiator_depth);
      result = Transition(*c, IBV_QPS_INIT);
      if (result.ok()) result = Transition(*c, IBV_QPS_RTR);
      if (result.ok()) result = Transition(*c, IBV_QPS_RTS);
      if (result.ok() && rdma_establish(id))
        result = ErrnoStatus("rdma_establish");
      if (result.ok()) Established(c);
      break;
    case RDMA_CM_EVENT_ESTABLISHED:
      Established(c);
      break;
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
      if (c->device)
        FailDevice(*c->device, CmError(event, status));
      else
        Close(c, CmError(event, status));
      break;
    case RDMA_CM_EVENT_TIMEWAIT_EXIT:
      break;
    default:
      result = CmError(event, status);
      break;
  }
  if (!result.ok()) Close(c, result);
}
void Engine::DrainCm() {
  for (;;) {
    rdma_cm_event* event = nullptr;
    if (rdma_get_cm_event(cm, &event)) {
      if (errno == EINTR) continue;
      if (!Again()) FailAll(ErrnoStatus("rdma_get_cm_event"));
      return;
    }
    auto* id = event->id;
    auto* listener = event->listen_id;
    const auto kind = event->event;
    const int status = event->status;
    std::vector<std::byte> data;
    rdma_conn_param peer{};
    if (kind == RDMA_CM_EVENT_CONNECT_REQUEST ||
        kind == RDMA_CM_EVENT_CONNECT_RESPONSE ||
        kind == RDMA_CM_EVENT_ESTABLISHED || kind == RDMA_CM_EVENT_REJECTED) {
      peer = event->param.conn;
      peer.private_data = nullptr;
      const auto* first =
          static_cast<const std::byte*>(event->param.conn.private_data);
      if (first && event->param.conn.private_data_len)
        data.assign(first, first + event->param.conn.private_data_len);
    }
    // Copy before ack, then dispatch. No handler may destroy an ID while an
    // unacknowledged CM event still refers to it (rdma_destroy_id would wait).
    CheckDestroy(rdma_ack_cm_event(event), "rdma_ack_cm_event");
    HandleCm(id, listener, kind, status, std::move(data), peer);
  }
}

void Engine::DrainCq(Device& device) {
  ibv_wc completions[32];
  for (;;) {
    const int count = ibv_poll_cq(device.cq, 32, completions);
    if (count < 0) {
      FailDevice(device, absl::UnavailableError("ibv_poll_cq failed"));
      return;
    }
    if (!count) return;
    for (int i = 0; i < count; ++i) {
      const auto& wc = completions[i];
      // No public data-posting API exists yet. Error completions still
      // terminate their connection; success entries are consumed so
      // notifications rearm.
      if (wc.status == IBV_WC_SUCCESS) continue;
      for (auto& c : Snapshot(connections)) {
        if (c->device.get() == &device && c->qp && c->qp->qp_num == wc.qp_num) {
          Close(c, absl::UnavailableError(absl::StrCat(
                       "RDMA completion: ", ibv_wc_status_str(wc.status),
                       " (vendor ", wc.vendor_err, ")")));
        }
      }
    }
  }
}
void Engine::DrainNotifications(Device& device, bool rearm) {
  for (;;) {
    ibv_cq* cq = nullptr;
    void* context = nullptr;
    if (ibv_get_cq_event(device.channel, &cq, &context)) {
      if (errno == EINTR) continue;
      if (!Again() && rearm)
        FailDevice(device, ErrnoStatus("ibv_get_cq_event"));
      return;
    }
    ibv_ack_cq_events(cq, 1);
    if (cq != device.cq) {
      FailDevice(device, absl::InternalError(
                             "unexpected CQ on worker completion channel"));
      continue;
    }
    if (rearm && device.error.ok()) {
      const int result = ibv_req_notify_cq(cq, 0);
      if (result) {
        FailDevice(device, absl::ErrnoToStatus(result, "ibv_req_notify_cq"));
        continue;
      }
      // Notification is one-shot. Rearm BEFORE draining: a completion racing
      // this drain is either consumed here or leaves a channel event to wake
      // us.
      DrainCq(device);
    }
  }
}
void Engine::DrainAsync(Device& device) {
  for (;;) {
    ibv_async_event event{};
    if (ibv_get_async_event(device.context.get(), &event)) {
      if (errno == EINTR) continue;
      if (!Again()) FailDevice(device, ErrnoStatus("ibv_get_async_event"));
      return;
    }
    const auto type = event.event_type;
    const auto element = event.element;
    ibv_ack_async_event(&event);
    auto error = absl::UnavailableError(
        absl::StrCat("RDMA device event: ", ibv_event_type_str(type)));
    if (type == IBV_EVENT_DEVICE_FATAL || type == IBV_EVENT_CQ_ERR) {
      FailDevice(device, error);
    } else if (type == IBV_EVENT_QP_FATAL || type == IBV_EVENT_QP_REQ_ERR ||
               type == IBV_EVENT_QP_ACCESS_ERR || type == IBV_EVENT_PORT_ERR) {
      for (auto& c : Snapshot(connections)) {
        if (c->device.get() == &device &&
            ((type == IBV_EVENT_PORT_ERR &&
              c->id->port_num == element.port_num) ||
             (type != IBV_EVENT_PORT_ERR && c->qp == element.qp)))
          Close(c, error);
      }
    }
  }
}
void Engine::OnPoll(Poll& poll, int result) {
  if (result < 0 || (result & (POLLERR | POLLHUP | POLLNVAL))) {
    auto error =
        absl::UnavailableError(absl::StrCat("RDMA fd poll failed: ", result));
    if (poll.device)
      FailDevice(*poll.device, error);
    else
      FailAll(error);
    return;
  }
  switch (poll.kind) {
    case Poll::Kind::kCm:
      DrainCm();
      break;
    case Poll::Kind::kCq:
      DrainNotifications(*poll.device, true);
      break;
    case Poll::Kind::kAsync:
      DrainAsync(*poll.device);
      break;
  }
  auto status = poll.Arm();
  if (!status.ok()) {
    if (poll.device)
      FailDevice(*poll.device, status);
    else
      FailAll(status);
  }
}
void Engine::FailDevice(Device& device, const absl::Status& error) {
  if (!device.error.ok()) return;
  device.error = error;
  device.completion_poll.Stop();
  device.async_poll.Stop();
  for (auto& c : Snapshot(connections))
    if (c->device.get() == &device) Close(c, error);
}
void Engine::RetireFailedDevice(Device& device) {
  if (stopping || device.error.ok() || device.reserved != 0 ||
      device.completion_poll.armed || device.async_poll.armed)
    return;
  DrainNotifications(device, false);
  DrainAsync(device);
  for (auto it = devices.begin(); it != devices.end(); ++it) {
    if (it->second.get() == &device) {
      devices.erase(it);
      return;
    }
  }
}
void Engine::FailAll(const absl::Status& error) {
  if (this->error.ok()) this->error = error;
  cm_poll.Stop();
  for (auto& listener : Snapshot(listeners)) Close(listener, error);
  for (auto& c : Snapshot(connections)) Close(c, error);
}

void Engine::Close(std::shared_ptr<Connection> c, absl::Status reason) {
  if (c->closing || c->closed) return;
  c->closing = true;
  c->handshake_done = true;
  if (c->error.ok()) c->error = std::move(reason);
  if (c->cm_started) (void)rdma_disconnect(c->id);
  // External QPs are not transitioned by rdma_disconnect. Posting stops before
  // ERR, and CQ capacity is not returned until the QP is destroyed and drained.
  if (c->qp) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_ERR;
    (void)ibv_modify_qp(c->qp, &attr, IBV_QP_STATE);
  }
  if (c->timer.armed) {
    auto status = worker->CancelRdmaEventPoll(&c->timer);
    if (!status.ok()) worker->RequestStop();
  }
  Progress(c);
}
void Engine::Progress(std::shared_ptr<Connection> c) {
  if (c->timer.armed || c->closed) return;
  if (c->closing) {
    // Erase before any CQ drain can fail another connection and reenter this
    // path. The local shared reference retains the object through callbacks.
    connections.erase(c->id);
    if (c->qp) {
      auto* qp = std::exchange(c->qp, nullptr);
      CheckDestroy(ibv_destroy_qp(qp), "ibv_destroy_qp(RC)");
      DrainCq(*c->device);
      c->device->reserved -= c->budget;
      c->budget = 0;
      RetireFailedDevice(*c->device);
    }
    CheckDestroy(rdma_destroy_id(c->id), "rdma_destroy_id(RC)");
    c->id = nullptr;
    c->device.reset();
    c->closed = true;
    c->retired.Publish(wake_worker());
  }
  if (c->handshake_done) {
    c->connected.Publish(wake_worker());
    if (auto listener = c->listener.lock()) {
      if (!c->queued && !listener->closing) {
        c->queued = true;
        listener->ready.push_back(c);
        listener->accepted.Publish(wake_worker());
      }
      Progress(listener);
    }
  }
}
void Engine::Close(std::shared_ptr<Listener> listener, absl::Status reason) {
  if (listener->closing || listener->closed) return;
  listener->closing = true;
  listener->error = reason;
  // No per-listener fd tag exists: the shared CM poll references only Engine.
  // DrainCm has acknowledged its event before this ID can be destroyed.
  CheckDestroy(rdma_destroy_id(listener->id), "rdma_destroy_id(listener)");
  listener->id = nullptr;
  for (auto& c : Snapshot(listener->children)) Close(c, reason);
  listener->accepted.Publish(wake_worker());
  Progress(listener);
}
void Engine::Progress(std::shared_ptr<Listener> listener) {
  if (!listener->closing || listener->closed) return;
  for (const auto& [key, c] : listener->children)
    if (!c->closed) return;
  listener->ready.clear();
  listener->children.clear();
  for (auto it = listeners.begin(); it != listeners.end(); ++it) {
    if (it->second == listener) {
      listeners.erase(it);
      break;
    }
  }
  listener->closed = true;
  listener->retired.Publish(wake_worker());
}
void Engine::BeginShutdown() {
  if (stopping) return;
  stopping = true;
  cm_poll.Stop();
  for (auto& [name, device] : devices) {
    device->completion_poll.Stop();
    device->async_poll.Stop();
  }
  const auto error = absl::CancelledError("RDMA worker shutdown");
  for (auto& listener : Snapshot(listeners)) Close(listener, error);
  for (auto& c : Snapshot(connections)) Close(c, error);
}
void Engine::AfterRingShutdown() {
  // io_uring_queue_exit has joined kernel access, including queued/unsubmitted
  // tags which never got callbacks. Only now may retained fd owners disappear.
  cm_poll.armed = false;
  for (auto& [name, device] : devices) {
    device->completion_poll.armed = device->async_poll.armed = false;
    DrainAsync(*device);
  }
  if (cm) DrainCm();
  for (auto& c : Snapshot(connections)) {
    c->timer.armed = false;
    Progress(c);
  }
  for (auto& listener : Snapshot(listeners)) Progress(listener);
  for (auto& [name, device] : devices) {
    DrainNotifications(*device, false);
    DrainAsync(*device);
    // Public connection states can survive shutdown; release device references
    // there too, but exported PD/MR handles independently retain their context.
  }
  devices.clear();
  if (cm) {
    rdma_destroy_event_channel(cm);
    cm = nullptr;
  }
  worker = nullptr;
}
}  // namespace bycorf::rdma::detail
