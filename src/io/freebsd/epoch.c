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

struct epoch {
  ck_epoch_t state;
  ck_epoch_record_t records[MAXCPU];
  struct epoch* next;
};
static struct epoch* epochs;

epoch_t epoch_alloc(const char* name, int flags) {
  (void)name;
  (void)flags;
  struct epoch* e = malloc(sizeof(*e), M_TEMP, M_WAITOK | M_ZERO);
  ck_epoch_init(&e->state);
  for (unsigned i = 0; i < bycorf_bsd_worker_count; ++i)
    ck_epoch_register(&e->state, &e->records[i], NULL);
  // Epoch domains are created during serialized stack initialization. Each
  // worker subsequently owns its record and runs its own deferred callbacks.
  e->next = epochs;
  epochs = e;
  return e;
}
void _epoch_enter_preempt(epoch_t e, epoch_tracker_t tracker) {
  ck_epoch_begin(&e->records[PCPU_GET(cpuid)], &tracker->et_section);
}
void _epoch_exit_preempt(epoch_t e, epoch_tracker_t tracker) {
  ck_epoch_end(&e->records[PCPU_GET(cpuid)], &tracker->et_section);
}
void epoch_enter(epoch_t e) {
  ck_epoch_begin(&e->records[PCPU_GET(cpuid)], NULL);
}
void epoch_exit(epoch_t e) { ck_epoch_end(&e->records[PCPU_GET(cpuid)], NULL); }
int in_epoch(epoch_t e) { return e->records[PCPU_GET(cpuid)].active != 0; }
void epoch_wait_preempt(epoch_t e) {
  if (in_epoch(e)) panic("epoch wait inside a read section");
  ck_epoch_synchronize(&e->records[PCPU_GET(cpuid)]);
}
void epoch_wait(epoch_t e) { epoch_wait_preempt(e); }
void epoch_drain_callbacks(epoch_t e) {
  if (in_epoch(e)) panic("epoch drain inside a read section");
  ck_epoch_barrier(&e->records[PCPU_GET(cpuid)]);
}
void epoch_call(epoch_t e, epoch_callback_t callback, epoch_context_t context) {
  _Static_assert(sizeof(struct epoch_context) == sizeof(ck_epoch_entry_t),
                 "epoch ABI changed");
  ck_epoch_call(&e->records[PCPU_GET(cpuid)], (ck_epoch_entry_t*)context,
                (ck_epoch_cb_t*)callback);
}
void bycorf_bsd_epoch_poll(void) {
  for (struct epoch* e = epochs; e; e = e->next)
    ck_epoch_poll(&e->records[PCPU_GET(cpuid)]);
}
