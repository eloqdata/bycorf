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

#include "internal.h"
#include <sys/epoch.h>
#include <sys/mbuf.h>
#include <sys/taskqueue.h>
#include <net/netisr.h>
#include <net/vnet.h>

static const struct netisr_handler* protocols[16];
void netisr_register(const struct netisr_handler* h) {
  if (h->nh_proto >= nitems(protocols)) panic("unsupported netisr protocol");
  protocols[h->nh_proto] = h;
}
void netisr_register_vnet(const struct netisr_handler* h) { (void)h; }
void netisr_unregister(const struct netisr_handler* h) {
  protocols[h->nh_proto] = NULL;
}
void netisr_unregister_vnet(const struct netisr_handler* h) { (void)h; }
int netisr_dispatch(u_int protocol, struct mbuf* m) {
  if (protocol >= nitems(protocols) || !protocols[protocol]) {
    m_freem(m);
    return EPROTONOSUPPORT;
  }
  // Flow steering has already selected this worker. A kernel netisr thread
  // would violate socket affinity, so protocol input executes synchronously.
  protocols[protocol]->nh_handler(m);
  return 0;
}
int netisr_queue(u_int protocol, struct mbuf* m) {
  return netisr_dispatch(protocol, m);
}

struct taskqueue {
  const char* name;
};
static struct taskqueue default_queue = {"celer worker"};
struct taskqueue* taskqueue_swi = &default_queue;
struct taskqueue* taskqueue_thread = &default_queue;
struct queued_task {
  struct queued_task* next;
  struct task* task;
  struct vnet* vnet;
  struct taskqueue* queue;
};
static _Thread_local struct queued_task *head, **tail;
bool celer_bsd_tasks_pending(void) { return head != NULL; }

struct taskqueue* taskqueue_create(const char* name, int flags,
                                   taskqueue_enqueue_fn notify, void* context) {
  (void)notify;
  (void)context;
  struct taskqueue* q = malloc(sizeof(*q), M_TEMP, flags);
  if (q) q->name = name;
  return q;
}
int taskqueue_start_threads(struct taskqueue** q, int count, int priority,
                            const char* name, ...) {
  // This queue is serviced by celer_bsd_poll on its owning worker. No kernel
  // task threads are created and no callback may suspend the native thread.
  (void)q;
  (void)count;
  (void)priority;
  (void)name;
  return 0;
}
void taskqueue_thread_enqueue(void* context) { (void)context; }
int taskqueue_enqueue(struct taskqueue* q, struct task* task) {
  if (task->ta_pending) {
    if (task->ta_pending != UINT16_MAX) ++task->ta_pending;
    return 0;
  }
  struct queued_task* entry = malloc(sizeof(*entry), M_TEMP, M_NOWAIT);
  if (!entry) return ENOMEM;
  entry->next = NULL;
  entry->task = task;
  entry->vnet = curvnet;
  entry->queue = q;
  task->ta_pending = 1;
  if (!tail) tail = &head;
  *tail = entry;
  tail = &entry->next;
  return 0;
}
static void execute_task(struct queued_task* entry) {
  struct task* task = entry->task;
  int count = task->ta_pending;
  task->ta_pending = 0;
  const bool network = TASK_IS_NET(task);
  struct epoch_tracker tracker;
  CURVNET_SET_QUIET(entry->vnet);
  if (network) NET_EPOCH_ENTER(tracker);
  task->ta_func(task->ta_context, count);
  if (network) NET_EPOCH_EXIT(tracker);
  CURVNET_RESTORE();
  free(entry, M_TEMP);
}
void celer_bsd_tasks_poll(void) {
  for (unsigned n = 0; head && n < 64; ++n) {
    struct queued_task* entry = head;
    head = entry->next;
    if (!head) tail = &head;
    execute_task(entry);
  }
}
int taskqueue_cancel(struct taskqueue* q, struct task* task, u_int* count) {
  if (count) *count = task->ta_pending;
  struct queued_task** link = &head;
  while (*link) {
    struct queued_task* entry = *link;
    if (entry->queue == q && entry->task == task) {
      *link = entry->next;
      if (tail == &entry->next) tail = link;
      task->ta_pending = 0;
      free(entry, M_TEMP);
      break;
    }
    link = &entry->next;
  }
  return 0;
}
void taskqueue_drain(struct taskqueue* q, struct task* task) {
  (void)q;
  while (task->ta_pending) celer_bsd_tasks_poll();
}
static void timeout_task_ready(void* arg) {
  struct timeout_task* t = arg;
  taskqueue_enqueue(t->q, &t->t);
}
void _timeout_task_init(struct taskqueue* q, struct timeout_task* t,
                        int priority, task_fn_t fn, void* context) {
  memset(t, 0, sizeof(*t));
  t->q = q;
  TASK_INIT(&t->t, priority, fn, context);
  callout_init(&t->c, 1);
}
int taskqueue_enqueue_timeout_sbt(struct taskqueue* q, struct timeout_task* t,
                                  sbintime_t sbt, sbintime_t precision,
                                  int flags) {
  t->q = q;
  return callout_reset_sbt(&t->c, sbt < 0 ? -sbt : sbt, precision,
                           timeout_task_ready, t, flags);
}
int taskqueue_cancel_timeout(struct taskqueue* q, struct timeout_task* t,
                             u_int* count) {
  callout_stop(&t->c);
  return taskqueue_cancel(q, &t->t, count);
}
void taskqueue_drain_timeout(struct taskqueue* q, struct timeout_task* t) {
  callout_drain(&t->c);
  taskqueue_drain(q, &t->t);
}
