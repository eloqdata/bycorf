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
#include <sys/bus.h>
#include <sys/conf.h>
#include <sys/event.h>
#include <sys/file.h>
#include <sys/filio.h>
#include <sys/hhook.h>
#include <sys/jail.h>
#include <sys/khelp.h>
#include <sys/kthread.h>
#include <sys/mutex.h>
#include <sys/osd.h>
#include <sys/priv.h>
#include <sys/refcount.h>
#include <sys/resourcevar.h>
#include <sys/sched.h>
#include <sys/selinfo.h>
#include <sys/signalvar.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/sockopt.h>
#include <sys/sysctl.h>
#include <sys/uio.h>
#include <sys/uuid.h>
#include <net/netisr.h>
#include <net/vnet.h>
#include <vm/vm.h>
#include <sys/stdarg.h>

// Kernel/user copies cross no protection boundary here. The public bridge
// constructs UIO_SYSSPACE requests from validated host buffers exclusively.
int copyin(const void* src, void* dst, size_t n) {
  memcpy(dst, src, n);
  return 0;
}
int copyout(const void* src, void* dst, size_t n) {
  memcpy(dst, src, n);
  return 0;
}
int copyinstr(const void* src, void* dst, size_t n, size_t* done) {
  size_t len = strnlen(src, n);
  if (len == n) return ENAMETOOLONG;
  memcpy(dst, src, len + 1);
  if (done) *done = len + 1;
  return 0;
}
int uiomove(void* buffer, int n, struct uio* uio) {
  if (uio->uio_segflg != UIO_SYSSPACE) return EFAULT;
  char* bytes = buffer;
  while (n > 0 && uio->uio_resid > 0) {
    if (uio->uio_iovcnt <= 0) return EINVAL;
    struct iovec* iov = uio->uio_iov;
    if (!iov->iov_len) {
      ++uio->uio_iov;
      --uio->uio_iovcnt;
      continue;
    }
    size_t len = MIN((size_t)n, MIN(iov->iov_len, (size_t)uio->uio_resid));
    if (uio->uio_rw == UIO_READ)
      memcpy(iov->iov_base, bytes, len);
    else
      memcpy(bytes, iov->iov_base, len);
    iov->iov_base = (char*)iov->iov_base + len;
    iov->iov_len -= len;
    uio->uio_resid -= len;
    uio->uio_offset += len;
    bytes += len;
    n -= len;
  }
  return 0;
}
void arc4rand(void* p, u_int n, int reseed) {
  celer_bsd_host.random_bytes(p, n);
}
uint32_t arc4random(void) {
  uint32_t value;
  arc4rand(&value, sizeof(value), 0);
  return value;
}
u_long random(void) { return arc4random() & 0x7fffffff; }
const char hex2ascii_data[] = "0123456789abcdefghijklmnopqrstuvwxyz";

int vasprintf(char** out, struct malloc_type* type, const char* format,
              va_list ap) {
  va_list copy;
  va_copy(copy, ap);
  int n = vsnprintf(NULL, 0, format, copy);
  va_end(copy);
  if (n < 0) return n;
  *out = malloc((size_t)n + 1, type, M_NOWAIT);
  if (!*out) return -1;
  return vsnprintf(*out, (size_t)n + 1, format, ap);
}
int asprintf(char** out, struct malloc_type* type, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  int n = vasprintf(out, type, format, ap);
  va_end(ap);
  return n;
}
int getenv_int(const char* name, int* value) {
  // Native kernels have a direct map; this user process does not. Enabling
  // external-page mbufs would dereference kernel VM structures during send.
  if (!strcmp(name, "kern.ipc.mb_use_ext_pgs")) {
    *value = 0;
    return 1;
  }
  return 0;
}
int getenv_quad(const char* name, quad_t* value) { return 0; }

int ratecheck(struct timeval* last, const struct timeval* minimum) {
  struct timeval now, delta;
  getmicrouptime(&now);
  delta.tv_sec = now.tv_sec - last->tv_sec;
  delta.tv_usec = now.tv_usec - last->tv_usec;
  if (delta.tv_usec < 0) {
    --delta.tv_sec;
    delta.tv_usec += 1000000;
  }
  if ((!last->tv_sec && !last->tv_usec) || timevalcmp(&delta, minimum, >=)) {
    *last = now;
    return 1;
  }
  return 0;
}
int eventratecheck(struct timeval* last, int* count, int maximum) {
  struct timeval interval = {.tv_sec = 1};
  if (ratecheck(last, &interval)) *count = 0;
  if (maximum < 0 || *count < maximum) {
    if (*count < INT_MAX) ++*count;
    return 1;
  }
  return 0;
}

// Credentials and VNET prisons are process-lifetime objects, with one baseline
// reference held by their worker. No arbitrary host user or jail is exposed.
struct ucred* crhold(struct ucred* cr) {
  __atomic_add_fetch(&cr->cr_ref, 1, __ATOMIC_RELAXED);
  return cr;
}
void crfree(struct ucred* cr) {
  if (__atomic_sub_fetch(&cr->cr_ref, 1, __ATOMIC_RELEASE) < 1)
    panic("worker credential lost baseline reference");
}
int priv_check_cred(struct ucred* cr, int privilege) {
  return cr && cr->cr_uid == 0 ? 0 : EPERM;
}
int priv_check(struct thread* td, int privilege) {
  return priv_check_cred(td->td_ucred, privilege);
}
int prison_check(struct ucred* a, struct ucred* b) {
  return a->cr_prison == b->cr_prison ? 0 : ESRCH;
}
int cr_cansee(struct ucred* a, struct ucred* b) { return prison_check(a, b); }
int cr_bsd_visible(struct ucred* a, struct ucred* b) {
  return prison_check(a, b);
}
bool cr_xids_subset(struct ucred* a, struct ucred* b) { return a == b; }
void cru2x(struct ucred* cr, struct xucred* out) {
  memset(out, 0, sizeof(*out));
  out->cr_version = XUCRED_VERSION;
  out->cr_uid = cr->cr_uid;
  out->cr_ngroups = 1;
  out->cr_groups[0] = cr->cr_gid;
}
bool jailed_without_vnet(struct ucred* cr) { return false; }
bool prison_flag(struct ucred* cr, unsigned flag) {
  return (cr->cr_prison->pr_flags & flag) != 0;
}
int prison_check_af(struct ucred* cr, int af) {
  return af == AF_INET || af == AF_UNSPEC || af == AF_ROUTE ? 0 : EAFNOSUPPORT;
}
int prison_if(struct ucred* cr, const struct sockaddr* sa) { return 0; }
int prison_ip_check(const struct prison* pr, pr_family_t af,
                    const void* address) {
  return 0;
}
u_int prison_ip_cnt(const struct prison* pr, pr_family_t af) { return 0; }
const void* prison_ip_get0(const struct prison* pr, pr_family_t af) {
  panic("restricted jail addresses are not configured");
}
struct prison* prison_find_child(struct prison* pr, int id) { return NULL; }
void prison_hold_locked(struct prison* pr) { refcount_acquire(&pr->pr_ref); }
void prison_free(struct prison* pr) {
  if (refcount_release(&pr->pr_ref))
    panic("worker prison lost baseline reference");
}
void getcredhostuuid(struct ucred* cr, char* out, size_t n) {
  strlcpy(out, "00000000-0000-0000-0000-000000000000", n);
}
void getjailname(struct ucred* cr, char* out, size_t n) {
  strlcpy(out, "celer", n);
}
int chgsbsize(struct uidinfo* uid, u_int* hiwat, u_int to, rlim_t limit) {
  if (to > limit) return 0;
  *hiwat = to;
  return 1;
}

// BSD sleep queues are deliberately absent: every exposed socket operation is
// nonblocking. An unexpected blocking path must fail visibly, never stall the
// worker that must process the packets or timers needed to wake it.
int _sleep(const void* channel, struct lock_object* lock, int priority,
           const char* why, sbintime_t timeout, sbintime_t precision,
           int flags) {
  panic("unexpected blocking FreeBSD operation: %s", why);
}
void wakeup(const void* channel) {}
void wakeup_one(const void* channel) {}
void critical_exit_preempt(void) {
  panic("FreeBSD scheduler preemption requested");
}
void _thread_lock(struct thread* td) { mtx_lock(td->td_lock); }
void sched_prio(struct thread* td, u_char priority) {
  td->td_priority = priority;
}
void spinlock_exit(void) { critical_exit(); }
int kproc_kthread_add(void (*func)(void*), void* arg, struct proc** p,
                      struct thread** td, int flags, int pages,
                      const char* process, const char* format, ...) {
  return EOPNOTSUPP;
}
volatile uint32_t hpts_that_need_softclock;
#undef tcp_hpts_softclock
void (*tcp_hpts_softclock)(void);

// Readiness is polled by the Celer backend. No kqueue/select registrations or
// SIGIO owners are admitted through the bridge, so these lists stay empty.
void knlist_init(struct knlist* knl, void* lock, void (*enter)(void*),
                 void (*leave)(void*), void (*assert)(void*, int)) {
  memset(knl, 0, sizeof(*knl));
  SLIST_INIT(&knl->kl_list);
  knl->kl_lock = enter;
  knl->kl_unlock = leave;
  knl->kl_assert_lock = assert;
  knl->kl_lockarg = lock;
}
int knlist_empty(struct knlist* knl) { return SLIST_EMPTY(&knl->kl_list); }
void knlist_destroy(struct knlist* knl) {
  if (!knlist_empty(knl)) panic("destroying registered kqueue list");
}
void knote(struct knlist* knl, long hint, int flags) {
  if (!knlist_empty(knl)) panic("unexpected kqueue registration");
}
void knlist_add(struct knlist* knl, struct knote* kn, int locked) {
  panic("kqueue is not exposed");
}
void knlist_remove(struct knlist* knl, struct knote* kn, int locked) {
  panic("kqueue is not exposed");
}
void selrecord(struct thread* td, struct selinfo* info) {
  panic("BSD select is not exposed");
}
void selwakeuppri(struct selinfo* info, int pri) {}
void seldrain(struct selinfo* info) {}
void sowakeup_aio(struct socket* so, sb_which which) {
  panic("BSD AIO is not exposed");
}
void soaio_rcv(void* arg, int pending) { panic("BSD AIO is not exposed"); }
void soaio_snd(void* arg, int pending) { panic("BSD AIO is not exposed"); }
int soaio_queue_generic(struct socket* so, struct kaiocb* job) {
  return EOPNOTSUPP;
}
int sendfile_wait_generic(struct socket* so, off_t need, int* space) {
  return EOPNOTSUPP;
}
void funsetown(struct sigio** owner) {
  if (*owner) panic("unexpected BSD SIGIO owner");
}
void pgsigio(struct sigio** owner, int signal, int check) {
  if (*owner) panic("unexpected BSD SIGIO owner");
}
void tdsignal(struct thread* td, int signal) {
  if (signal != SIGPIPE) panic("unexpected BSD signal: %d", signal);
}
void kern_psignal(struct proc* p, int signal) {
  if (signal != SIGPIPE) panic("unexpected BSD signal: %d", signal);
}
int accept_filt_getopt(struct socket* so, struct sockopt* option) {
  return EOPNOTSUPP;
}
int accept_filt_setopt(struct socket* so, struct sockopt* option) {
  return option ? EOPNOTSUPP : 0;
}
const cap_rights_t cap_recv_rights, cap_send_rights;
int getsock(struct thread* td, int fd, const cap_rights_t* rights,
            struct file** file) {
  return EBADF;
}
int _fdrop(struct file* file, struct thread* td) {
  panic("BSD file descriptors are not exposed");
}

// The prototype loads no helper modules. Empty hook heads preserve the native
// TCP fast-path contract (nonnull head, zero hooks); OSD carries no extensions.
int hhook_head_register(int32_t type, int32_t id, struct hhook_head** out,
                        uint32_t flags) {
  *out = malloc(sizeof(**out), M_TEMP, M_WAITOK | M_ZERO);
  (*out)->hhh_type = type;
  (*out)->hhh_id = id;
  return 0;
}
int hhook_head_deregister(struct hhook_head* head) {
  free(head, M_TEMP);
  return 0;
}
int khelp_init_osd(uint32_t classes, struct osd* osd) {
  memset(osd, 0, sizeof(*osd));
  return 0;
}
int khelp_destroy_osd(struct osd* osd) {
  if (osd->osd_nslots) panic("unregistered kernel helper");
  return 0;
}
int osd_register(u_int type, osd_destructor_t destructor,
                 const osd_method_t* methods) {
  return 1;
}

// Device/control APIs have no host endpoint. The in-process Ethernet interface
// is configured directly; these errors cannot be mistaken for a working ioctl.
int bus_get_domain(device_t dev, int* domain) {
  *domain = 0;
  return 0;
}
void make_dev_args_init_impl(struct make_dev_args* args, size_t size) {
  memset(args, 0, size);
  args->mda_size = size;
}
int make_dev_s(struct make_dev_args* args, struct cdev** device,
               const char* format, ...) {
  if (device) *device = NULL;
  return EOPNOTSUPP;
}
void devctl_notify(const char* system, const char* subsystem, const char* type,
                   const char* data) {}
int uuid_ether_add(const uint8_t* mac) { return 0; }
int uuid_ether_del(const uint8_t* mac) { return 0; }
void _gone_in(int major, const char* message, ...) {
  printf("FreeBSD deprecated API: %s\n", message);
}
void netisr_clearqdrops(const struct netisr_handler* handler) {}
void netisr_getqdrops(const struct netisr_handler* handler, uint64_t* drops) {
  *drops = 0;
}
void netisr_getqlimit(const struct netisr_handler* handler, u_int* limit) {
  *limit = 0;
}
int netisr_setqlimit(const struct netisr_handler* handler, u_int limit) {
  return EOPNOTSUPP;
}

// No sysctl request bridge is exported yet. Native sysctl descriptors remain
// linked for VNET layout/initialization, but requests cannot mutate the stack.
struct sysctl_oid_list sysctl__children =
    SLIST_HEAD_INITIALIZER(sysctl__children);
SYSCTL_ROOT_NODE(CTL_DEBUG, debug, CTLFLAG_RD, 0, "Debug");
#define UNEXPOSED_SYSCTL(name) \
  int name(SYSCTL_HANDLER_ARGS) { return EOPNOTSUPP; }
UNEXPOSED_SYSCTL(sysctl_handle_32)
UNEXPOSED_SYSCTL(sysctl_handle_64)
UNEXPOSED_SYSCTL(sysctl_handle_bool)
UNEXPOSED_SYSCTL(sysctl_handle_int)
UNEXPOSED_SYSCTL(sysctl_handle_long)
UNEXPOSED_SYSCTL(sysctl_handle_string)
UNEXPOSED_SYSCTL(sysctl_handle_uma_zone_cur)
UNEXPOSED_SYSCTL(sysctl_msec_to_ticks)
UNEXPOSED_SYSCTL(sysctl_sec_to_timeval)
int sysctl_wire_old_buffer(struct sysctl_req* req, size_t size) {
  return EOPNOTSUPP;
}
struct sbuf* sbuf_new_for_sysctl(struct sbuf* s, char* buffer, int n,
                                 struct sysctl_req* req) {
  return NULL;
}

// These functions exist only to reject paths requiring the real kernel's VM.
// External-page mbufs, sendfile and TLS are disabled above and at the bridge.
_Noreturn uintptr_t celer_bsd_no_direct_map(uintptr_t address) {
  panic("kernel direct map is unavailable");
}
vm_page_t PHYS_TO_VM_PAGE(vm_paddr_t address) {
  panic("kernel direct map is unavailable");
}
vm_page_t vm_page_alloc_noobj(int flags) { panic("kernel VM is unavailable"); }
void vm_page_free(vm_page_t page) { panic("kernel VM is unavailable"); }
bool vm_page_unwire_noq(vm_page_t page) { panic("kernel VM is unavailable"); }
void vm_wait(void) { panic("kernel VM is unavailable"); }
int uiomove_fromphys(struct vm_page* pages[], vm_offset_t offset, int n,
                     struct uio* uio) {
  return EOPNOTSUPP;
}
uint32_t calculate_crc32c(uint32_t crc, const unsigned char* bytes,
                          unsigned int n) {
  // No SCTP endpoint is exposed, but checksum dispatch can reference its
  // helper.
  while (n--) {
    crc ^= *bytes++;
    for (unsigned bit = 0; bit < 8; ++bit)
      crc = (crc >> 1) ^ (0x82f63b78u & (0u - (crc & 1u)));
  }
  return crc;
}
