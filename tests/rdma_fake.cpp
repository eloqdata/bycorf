// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "rdma_fake.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <deque>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {
struct CmEvent {
  rdma_cm_event event{};
  std::vector<std::byte> data;
};
struct Id {
  bool listening = false;
  rdma_cm_id* peer = nullptr;
};
struct Cq {
  bool armed = false;
  unsigned inject_qp = 0;
  int polls = 0, notifications = 0;
  std::deque<ibv_wc> completions;
};
struct Fake {
  std::unordered_map<rdma_event_channel*, std::deque<std::unique_ptr<CmEvent>>>
      channels;
  std::unordered_map<rdma_cm_event*, std::unique_ptr<CmEvent>> delivered;
  std::unordered_map<rdma_cm_id*, Id> ids;
  std::unordered_map<ibv_context*, std::deque<ibv_async_event>> contexts;
  std::unordered_map<ibv_comp_channel*, std::deque<ibv_cq*>>
      completion_channels;
  std::unordered_map<ibv_cq*, Cq> cqs;
  std::unordered_set<ibv_pd*> pds;
  std::unordered_set<ibv_qp*> qps;
  bool suppress_establish = false, suppress_connect = false, fail_qp = false,
       fail_notify = false;
  unsigned qp_extra = 0, next_qp = 1;
  std::uint16_t next_port = 10000;
  int cq_acks = 0, async_acks = 0;
} f;
ibv_device device{};
ibv_context cm_context{};

int EventFd() { return eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC | EFD_SEMAPHORE); }
void Notify(int fd) {
  std::uint64_t one = 1;
  EXPECT_EQ(write(fd, &one, sizeof(one)), sizeof(one));
}
void Consume(int fd) {
  std::uint64_t one;
  EXPECT_EQ(read(fd, &one, sizeof(one)), sizeof(one));
}
int Empty() {
  errno = EAGAIN;
  return -1;
}
void Event(rdma_cm_id* id, rdma_cm_event_type type,
           rdma_cm_id* listener = nullptr, const void* data = nullptr,
           std::size_t length = 0, int status = 0) {
  auto e = std::make_unique<CmEvent>();
  e->event.id = id;
  e->event.listen_id = listener;
  e->event.event = type;
  e->event.status = status;
  if (data && length) {
    auto* first = static_cast<const std::byte*>(data);
    e->data.assign(first, first + length);
    e->event.param.conn.private_data = e->data.data();
    e->event.param.conn.private_data_len = static_cast<std::uint8_t>(length);
  }
  e->event.param.conn.initiator_depth =
      e->event.param.conn.responder_resources = 1;
  f.channels.at(id->channel).push_back(std::move(e));
  Notify(id->channel->fd);
}
int PollCq(ibv_cq* cq, int capacity, ibv_wc* output) {
  auto& state = f.cqs.at(cq);
  ++state.polls;
  int count = 0;
  while (count < capacity && !state.completions.empty()) {
    output[count++] = state.completions.front();
    state.completions.pop_front();
  }
  return count;
}
int ArmCq(ibv_cq* cq, int solicited) {
  EXPECT_EQ(solicited, 0);
  if (std::exchange(f.fail_notify, false)) return EIO;
  auto& state = f.cqs.at(cq);
  state.armed = true;
  if (auto qp = std::exchange(state.inject_qp, 0U))
    rdma_fake::Completion(cq, qp);
  return 0;
}
}  // namespace

namespace rdma_fake {
void Reset() {
  f = {};
  cm_context.device = &device;
}
void CheckEmpty() {
  EXPECT_TRUE(f.channels.empty());
  EXPECT_TRUE(f.delivered.empty());
  EXPECT_TRUE(f.ids.empty());
  EXPECT_TRUE(f.contexts.empty());
  EXPECT_TRUE(f.completion_channels.empty());
  EXPECT_TRUE(f.cqs.empty());
  EXPECT_TRUE(f.pds.empty());
  EXPECT_TRUE(f.qps.empty());
}
void Cm(rdma_cm_id* id, rdma_cm_event_type event, int status) {
  Event(id, event, nullptr, nullptr, 0, status);
}
void SuppressEstablish(bool value) { f.suppress_establish = value; }
void SuppressConnect(bool value) { f.suppress_connect = value; }
void RoundQp(unsigned extra) { f.qp_extra = extra; }
void FailNextQp() { f.fail_qp = true; }
void FailNextNotify() { f.fail_notify = true; }
void Completion(ibv_cq* cq, std::uint32_t qp, ibv_wc_status status) {
  auto& state = f.cqs.at(cq);
  ibv_wc wc{};
  wc.qp_num = qp;
  wc.status = status;
  state.completions.push_back(wc);
  if (std::exchange(state.armed, false)) {
    f.completion_channels.at(cq->channel).push_back(cq);
    ++state.notifications;
    Notify(cq->channel->fd);
  }
}
void Async(ibv_context* context, ibv_event_type type, ibv_qp* qp) {
  ibv_async_event e{};
  e.event_type = type;
  if (type == IBV_EVENT_CQ_ERR)
    e.element.cq = qp->send_cq;
  else if (type != IBV_EVENT_DEVICE_FATAL)
    e.element.qp = qp;
  f.contexts.at(context).push_back(e);
  Notify(context->async_fd);
}
void CompleteOnArm(ibv_cq* cq, std::uint32_t qp) {
  f.cqs.at(cq).inject_qp = qp;
}
int CqPolls(ibv_cq* cq) { return f.cqs.at(cq).polls; }
int Notifications(ibv_cq* cq) { return f.cqs.at(cq).notifications; }
int AckedCqEvents() { return f.cq_acks; }
int AckedAsyncEvents() { return f.async_acks; }
int LiveQps() { return static_cast<int>(f.qps.size()); }
int LiveIds() { return static_cast<int>(f.ids.size()); }
}  // namespace rdma_fake

extern "C" {
rdma_event_channel* __wrap_rdma_create_event_channel() {
  auto* c = new rdma_event_channel{};
  c->fd = EventFd();
  f.channels.emplace(c, std::deque<std::unique_ptr<CmEvent>>{});
  return c;
}
void __wrap_rdma_destroy_event_channel(rdma_event_channel* c) {
  EXPECT_TRUE(f.channels.at(c).empty());
  f.channels.erase(c);
  close(c->fd);
  delete c;
}
int __wrap_rdma_create_id(rdma_event_channel* channel, rdma_cm_id** output,
                          void* context, rdma_port_space ps) {
  EXPECT_EQ(ps, RDMA_PS_TCP);
  auto* id = new rdma_cm_id{};
  id->channel = channel;
  id->context = context;
  id->verbs = &cm_context;
  id->port_num = 1;
  id->qp_type = IBV_QPT_RC;
  f.ids.emplace(id, Id{});
  *output = id;
  return 0;
}
int __wrap_rdma_destroy_id(rdma_cm_id* id) {
  for (const auto& [key, event] : f.delivered) {
    EXPECT_NE(event->event.id, id);
    EXPECT_NE(event->event.listen_id, id);
  }
  auto& queue = f.channels.at(id->channel);
  for (auto it = queue.begin(); it != queue.end();) {
    if ((*it)->event.id == id) {
      Consume(id->channel->fd);
      it = queue.erase(it);
    } else
      ++it;
  }
  for (auto& [key, value] : f.ids)
    if (value.peer == id) value.peer = nullptr;
  EXPECT_EQ(f.ids.erase(id), 1U);
  delete id;
  return 0;
}
int __wrap_rdma_bind_addr(rdma_cm_id* id, sockaddr* address) {
  auto* local = reinterpret_cast<sockaddr_in*>(&id->route.addr.src_addr);
  *local = *reinterpret_cast<sockaddr_in*>(address);
  if (!local->sin_port) local->sin_port = htons(f.next_port++);
  return 0;
}
int __wrap_rdma_listen(rdma_cm_id* id, int) {
  f.ids.at(id).listening = true;
  return 0;
}
int __wrap_rdma_resolve_addr(rdma_cm_id* id, sockaddr*, sockaddr* destination,
                             int) {
  *reinterpret_cast<sockaddr_in*>(&id->route.addr.dst_addr) =
      *reinterpret_cast<sockaddr_in*>(destination);
  Event(id, RDMA_CM_EVENT_ADDR_RESOLVED);
  return 0;
}
int __wrap_rdma_resolve_route(rdma_cm_id* id, int) {
  Event(id, RDMA_CM_EVENT_ROUTE_RESOLVED);
  return 0;
}
int __wrap_rdma_connect(rdma_cm_id* id, rdma_conn_param* param) {
  EXPECT_EQ(id->qp, nullptr);
  EXPECT_NE(param->qp_num, 0U);
  if (f.suppress_connect) return 0;
  rdma_cm_id* listener = nullptr;
  for (const auto& [key, value] : f.ids)
    if (value.listening && rdma_get_src_port(key) == rdma_get_dst_port(id)) {
      listener = key;
      break;
    }
  if (!listener) {
    Event(id, RDMA_CM_EVENT_REJECTED, nullptr, nullptr, 0, ECONNREFUSED);
    return 0;
  }
  rdma_cm_id* accepted;
  __wrap_rdma_create_id(listener->channel, &accepted, nullptr, RDMA_PS_TCP);
  f.ids.at(accepted).peer = id;
  f.ids.at(id).peer = accepted;
  Event(accepted, RDMA_CM_EVENT_CONNECT_REQUEST, listener, param->private_data,
        param->private_data_len);
  return 0;
}
int __wrap_rdma_accept(rdma_cm_id* id, rdma_conn_param* param) {
  EXPECT_EQ(id->qp, nullptr);
  EXPECT_NE(param->qp_num, 0U);
  Event(f.ids.at(id).peer, RDMA_CM_EVENT_CONNECT_RESPONSE, nullptr,
        param->private_data, param->private_data_len);
  return 0;
}
int __wrap_rdma_establish(rdma_cm_id* id) {
  if (f.suppress_establish) return 0;
  Event(f.ids.at(id).peer, RDMA_CM_EVENT_ESTABLISHED);
  return 0;
}
int __wrap_rdma_disconnect(rdma_cm_id* id) {
  if (auto* peer = f.ids.at(id).peer) Event(peer, RDMA_CM_EVENT_DISCONNECTED);
  return 0;
}
int __wrap_rdma_reject(rdma_cm_id* id, const void*, std::uint8_t) {
  if (auto* peer = f.ids.at(id).peer)
    Event(peer, RDMA_CM_EVENT_REJECTED, nullptr, nullptr, 0, ECONNREFUSED);
  return 0;
}
int __wrap_rdma_get_cm_event(rdma_event_channel* channel,
                             rdma_cm_event** output) {
  auto& queue = f.channels.at(channel);
  if (queue.empty()) return Empty();
  Consume(channel->fd);
  auto event = std::move(queue.front());
  queue.pop_front();
  *output = &event->event;
  f.delivered.emplace(*output, std::move(event));
  return 0;
}
int __wrap_rdma_ack_cm_event(rdma_cm_event* event) {
  EXPECT_EQ(f.delivered.erase(event), 1U);
  return 0;
}
int __wrap_rdma_init_qp_attr(rdma_cm_id*, ibv_qp_attr*, int* mask) {
  *mask = IBV_QP_STATE;
  return 0;
}
const char* __wrap_ibv_get_device_name(ibv_device*) { return "fake0"; }
ibv_context* __wrap_ibv_open_device(ibv_device* dev) {
  auto* context = new ibv_context{};
  context->device = dev;
  context->async_fd = EventFd();
  context->ops.poll_cq = PollCq;
  context->ops.req_notify_cq = ArmCq;
  f.contexts.emplace(context, std::deque<ibv_async_event>{});
  return context;
}
int __wrap_ibv_close_device(ibv_context* context) {
  EXPECT_TRUE(f.contexts.at(context).empty());
  f.contexts.erase(context);
  close(context->async_fd);
  delete context;
  return 0;
}
int __wrap_ibv_query_device(ibv_context*, ibv_device_attr* attr) {
  *attr = {};
  attr->max_cqe = 65536;
  attr->max_qp_wr = 8192;
  return 0;
}
ibv_pd* __wrap_ibv_alloc_pd(ibv_context* context) {
  auto* pd = new ibv_pd{};
  pd->context = context;
  f.pds.insert(pd);
  return pd;
}
int __wrap_ibv_dealloc_pd(ibv_pd* pd) {
  EXPECT_EQ(f.pds.erase(pd), 1U);
  delete pd;
  return 0;
}
ibv_comp_channel* __wrap_ibv_create_comp_channel(ibv_context* context) {
  auto* channel = new ibv_comp_channel{};
  channel->context = context;
  channel->fd = EventFd();
  f.completion_channels.emplace(channel, std::deque<ibv_cq*>{});
  return channel;
}
int __wrap_ibv_destroy_comp_channel(ibv_comp_channel* channel) {
  EXPECT_TRUE(f.completion_channels.at(channel).empty());
  f.completion_channels.erase(channel);
  close(channel->fd);
  delete channel;
  return 0;
}
ibv_cq* __wrap_ibv_create_cq(ibv_context* context, int cqe, void* opaque,
                             ibv_comp_channel* channel, int) {
  auto* cq = new ibv_cq{};
  cq->context = context;
  cq->cq_context = opaque;
  cq->cqe = cqe;
  cq->channel = channel;
  f.cqs.emplace(cq, Cq{});
  return cq;
}
int __wrap_ibv_destroy_cq(ibv_cq* cq) {
  EXPECT_EQ(f.cqs.erase(cq), 1U);
  delete cq;
  return 0;
}
ibv_qp* __wrap_ibv_create_qp(ibv_pd* pd, ibv_qp_init_attr* attr) {
  if (std::exchange(f.fail_qp, false)) {
    errno = ENOMEM;
    return nullptr;
  }
  EXPECT_EQ(attr->qp_type, IBV_QPT_RC);
  EXPECT_EQ(pd->context, attr->send_cq->context);
  EXPECT_EQ(pd->context, attr->recv_cq->context);
  attr->cap.max_send_wr += f.qp_extra;
  auto* qp = new ibv_qp{};
  qp->pd = pd;
  qp->context = pd->context;
  qp->qp_num = f.next_qp++;
  qp->qp_type = attr->qp_type;
  qp->send_cq = attr->send_cq;
  qp->recv_cq = attr->recv_cq;
  f.qps.insert(qp);
  return qp;
}
int __wrap_ibv_modify_qp(ibv_qp* qp, ibv_qp_attr* attr, int) {
  qp->state = attr->qp_state;
  return 0;
}
int __wrap_ibv_destroy_qp(ibv_qp* qp) {
  EXPECT_EQ(f.qps.erase(qp), 1U);
  delete qp;
  return 0;
}
int __wrap_ibv_get_cq_event(ibv_comp_channel* channel, ibv_cq** cq,
                            void** opaque) {
  auto& queue = f.completion_channels.at(channel);
  if (queue.empty()) return Empty();
  Consume(channel->fd);
  *cq = queue.front();
  *opaque = (*cq)->cq_context;
  queue.pop_front();
  return 0;
}
void __wrap_ibv_ack_cq_events(ibv_cq*, unsigned count) { f.cq_acks += count; }
int __wrap_ibv_get_async_event(ibv_context* context, ibv_async_event* event) {
  auto& queue = f.contexts.at(context);
  if (queue.empty()) return Empty();
  Consume(context->async_fd);
  *event = queue.front();
  queue.pop_front();
  return 0;
}
void __wrap_ibv_ack_async_event(ibv_async_event*) { ++f.async_acks; }
}  // extern "C"
