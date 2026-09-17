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
#include <sys/callout.h>
#include <sys/lock.h>

int hz = 1000;
int tick = 1000;
sbintime_t tick_sbt = SBT_1MS;
sbintime_t tc_tick_sbt = SBT_1MS;
_Thread_local volatile int ticks;
_Thread_local volatile time_t time_second;
_Thread_local volatile time_t time_uptime;
static uint64_t boot_ns;
static _Thread_local struct callout_tailq pending;

static sbintime_t uptime_sbt(void) {
  uint64_t ns = bycorf_bsd_host.monotonic_ns() - boot_ns + 1000000000;
  return (ns / 1000000000) * SBT_1S + (ns % 1000000000) * SBT_1S / 1000000000;
}

void bycorf_bsd_clock_update(void) {
  if (!boot_ns) boot_ns = bycorf_bsd_host.monotonic_ns();
  // Each VNET uses its owner's snapshot of the same process clock. Worker 0
  // may be asleep while another worker processes a retransmission timer.
  uint64_t ns = bycorf_bsd_host.monotonic_ns() - boot_ns + 1000000000;
  ticks = ns / 1000000;
  time_uptime = ns / 1000000000;
  time_second = bycorf_bsd_host.realtime_ns() / 1000000000;
}

void getbinuptime(struct bintime* bt) {
  uint64_t ns = bycorf_bsd_host.monotonic_ns() - boot_ns + 1000000000;
  bt->sec = ns / 1000000000;
  bt->frac = (uint64_t)(((__uint128_t)(ns % 1000000000) << 64) / 1000000000);
}
void binuptime(struct bintime* bt) { getbinuptime(bt); }
void bintime(struct bintime* bt) {
  uint64_t ns = bycorf_bsd_host.realtime_ns();
  bt->sec = ns / 1000000000;
  bt->frac = (uint64_t)(((__uint128_t)(ns % 1000000000) << 64) / 1000000000);
}
void getboottimebin(struct bintime* bt) {
  uint64_t ns = bycorf_bsd_host.realtime_ns() -
                (bycorf_bsd_host.monotonic_ns() - boot_ns);
  bt->sec = ns / 1000000000;
  bt->frac = (uint64_t)(((__uint128_t)(ns % 1000000000) << 64) / 1000000000);
}
void getmicrouptime(struct timeval* tv) {
  struct bintime bt;
  getbinuptime(&bt);
  bintime2timeval(&bt, tv);
}
void microuptime(struct timeval* tv) { getmicrouptime(tv); }
void getmicrotime(struct timeval* tv) {
  struct bintime bt;
  bintime(&bt);
  bintime2timeval(&bt, tv);
}
void microtime(struct timeval* tv) { getmicrotime(tv); }
void nanouptime(struct timespec* ts) {
  struct bintime bt;
  getbinuptime(&bt);
  bintime2timespec(&bt, ts);
}
void nanotime(struct timespec* ts) {
  struct bintime bt;
  bintime(&bt);
  bintime2timespec(&bt, ts);
}

void callout_init(struct callout* c, int mpsafe) {
  (void)mpsafe;
  memset(c, 0, sizeof(*c));
  c->c_cpu = PCPU_GET(cpuid);
}
void _callout_init_lock(struct callout* c, struct lock_object* lock,
                        int flags) {
  callout_init(c, 1);
  c->c_lock = lock;
  c->c_flags = flags;
}
int _callout_stop_safe(struct callout* c, int flags) {
  (void)flags;
  if (c->c_cpu != PCPU_GET(cpuid))
    panic("cross-worker FreeBSD callout cancellation");
  if (!(c->c_iflags & CALLOUT_PENDING)) return 0;
  TAILQ_REMOVE(&pending, c, c_links.tqe);
  c->c_iflags &= ~CALLOUT_PENDING;
  c->c_flags &= ~CALLOUT_ACTIVE;
  return 1;
}
void callout_when(sbintime_t sbt, sbintime_t precision, int flags,
                  sbintime_t* result, sbintime_t* prec) {
  *result = (flags & C_ABSOLUTE) ? sbt : uptime_sbt() + sbt;
  *prec = precision;
}
int callout_reset_sbt_on(struct callout* c, sbintime_t sbt,
                         sbintime_t precision, void (*function)(void*),
                         void* arg, int cpu, int flags) {
  if (!pending.tqh_last) TAILQ_INIT(&pending);
  // FreeBSD's non-RSS TCP default requests kernel CPU 0. In this port queue
  // steering already established the socket owner, and its timers must stay
  // with that VNET. Treat the kernel CPU argument as a placement hint; the
  // callout's actual owner is still checked by _callout_stop_safe below.
  (void)cpu;
  int replaced = _callout_stop_safe(c, 0);
  callout_when(sbt, precision, flags, &c->c_time, &c->c_precision);
  c->c_func = function;
  c->c_arg = arg;
  c->c_flags |= CALLOUT_ACTIVE;
  c->c_iflags |= CALLOUT_PENDING;
  struct callout* at;
  TAILQ_FOREACH(at, &pending, c_links.tqe) {
    if (c->c_time < at->c_time) {
      TAILQ_INSERT_BEFORE(at, c, c_links.tqe);
      return replaced;
    }
  }
  TAILQ_INSERT_TAIL(&pending, c, c_links.tqe);
  return replaced;
}
int callout_schedule(struct callout* c, int n) {
  return callout_reset_sbt_on(c, tick_sbt * n, 0, c->c_func, c->c_arg, -1,
                              C_HARDCLOCK);
}
void bycorf_bsd_callout_poll(void) {
  if (!pending.tqh_last) return;
  const sbintime_t now = uptime_sbt();
  // Bound timer work so a burst of expirations cannot monopolize the worker.
  for (unsigned n = 0; n < 256; ++n) {
    struct callout* c = TAILQ_FIRST(&pending);
    if (!c || c->c_time > now) break;
    TAILQ_REMOVE(&pending, c, c_links.tqe);
    c->c_iflags &= ~CALLOUT_PENDING;
    struct lock_object* lock = c->c_lock;
    const bool unlock = !(c->c_flags & CALLOUT_RETURNUNLOCKED);
    void (*fn)(void*) = c->c_func;
    void* arg = c->c_arg;
    bycorf_bsd_lock(lock);
    fn(arg);
    // The callback may have freed its enclosing object; do not read c again.
    if (unlock) bycorf_bsd_unlock(lock);
  }
}
uint64_t bycorf_bsd_deadline_ns(void) {
  if (bycorf_bsd_tasks_pending()) return bycorf_bsd_host.monotonic_ns();
  struct callout* c = TAILQ_FIRST(&pending);
  if (!c) return UINT64_MAX;
  sbintime_t delta = c->c_time - uptime_sbt();
  if (delta <= 0) return bycorf_bsd_host.monotonic_ns();
  return bycorf_bsd_host.monotonic_ns() +
         (uint64_t)(((__uint128_t)delta * 1000000000 + SBT_1S - 1) / SBT_1S);
}
