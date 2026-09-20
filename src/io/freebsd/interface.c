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
#include <sys/mbuf.h>
#include <sys/sockio.h>
#include <sys/socket.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <net/if_var.h>
#include <net/if_types.h>
#include <net/if_dl.h>
#include <net/route.h>
#include <net/route/route_ctl.h>
#include <net/vnet.h>
#include <netinet/in.h>
#include <netinet/in_var.h>

static _Thread_local if_t interface;
static _Thread_local struct bycorf_bsd_interface config;

static int transmit(if_t ifp, struct mbuf* m) {
  struct bycorf_bsd_interface* cfg = if_getsoftc(ifp);
  // The first version deliberately uses copied, MTU-sized frames and software
  // checksums. No BSD mbuf or stack-local pointer crosses to a queue owner.
  unsigned char frame[ETHER_MAX_LEN];
  size_t n = m->m_pkthdr.len;
  int error = EMSGSIZE;
  if (n <= sizeof(frame)) {
    m_copydata(m, 0, n, frame);
    error = cfg->transmit(cfg->context, frame, n);
  }
  if_inc_counter(ifp, error ? IFCOUNTER_OERRORS : IFCOUNTER_OPACKETS, 1);
  m_freem(m);
  return error;
}
static int local_output(if_t ifp, struct mbuf* m,
                        const struct sockaddr* destination,
                        struct route* route) {
  // The destination socket may belong to another worker's VNET. Use the same
  // copied-frame boundary as physical output; the host queues local frames to
  // their socket owner rather than recursively entering TCP under its lock.
  if (destination->sa_family != AF_INET ||
      ((const struct sockaddr_in*)destination)->sin_addr.s_addr !=
          config.address) {
    m_freem(m);
    return EAFNOSUPPORT;
  }
  M_PREPEND(m, ETHER_HDR_LEN, M_NOWAIT);
  if (!m) return ENOBUFS;
  struct ether_header* header = mtod(m, struct ether_header*);
  memcpy(header->ether_dhost, config.mac, ETHER_ADDR_LEN);
  memcpy(header->ether_shost, config.mac, ETHER_ADDR_LEN);
  header->ether_type = htons(ETHERTYPE_IP);
  return transmit(interface, m);
}
static int control(if_t ifp, u_long command, caddr_t data) {
  if (command == SIOCSIFFLAGS) {
    if_setdrvflags(ifp, IFF_DRV_RUNNING);
    return 0;
  }
  return ether_ioctl(ifp, command, data);
}
static void initialize(void* arg) {
  if_setdrvflags(interface, IFF_DRV_RUNNING);
}
static void flush(if_t ifp) {}

int bycorf_bsd_attach_interface(const struct bycorf_bsd_interface* value) {
  if (interface) return EALREADY;
  if (!value || !value->transmit || value->mtu < 576 || value->mtu > ETHERMTU)
    return EINVAL;
  config = *value;
  // Address installation creates a host route through lo0. FreeBSD creates
  // lo0 down; without bringing it up, IP output rejects that route with
  // EHOSTUNREACH. Local packets cross the host's frame queues, so compute real
  // checksums and retain the physical MTU instead of loopback offload flags.
  if (!V_loif) return ENXIO;
  if_setflags(V_loif, if_getflags(V_loif) | IFF_UP);
  if_setdrvflags(V_loif, IFF_DRV_RUNNING);
  if_setmtu(V_loif, config.mtu);
  if_sethwassist(V_loif, 0);
  if_setoutputfn(V_loif, local_output);
  if_link_state_change(V_loif, LINK_STATE_UP);
  interface = if_alloc(IFT_ETHER);
  if (!interface) return ENOMEM;
  if_initname(interface, "bycorf", 0);
  if_setsoftc(interface, &config);
  if_setflags(interface, IFF_UP | IFF_BROADCAST | IFF_SIMPLEX | IFF_MULTICAST);
  if_setdrvflags(interface, IFF_DRV_RUNNING);
  if_setmtu(interface, config.mtu);
  if_settransmitfn(interface, transmit);
  if_setioctlfn(interface, control);
  if_setinitfn(interface, initialize);
  if_setqflushfn(interface, flush);
  ether_ifattach(interface, config.mac);
  if_link_state_change(interface, LINK_STATE_UP);
  struct in_aliasreq request = {0};
  strlcpy(request.ifra_name, if_name(interface), sizeof(request.ifra_name));
  request.ifra_addr =
      (struct sockaddr_in){.sin_len = sizeof(struct sockaddr_in),
                           .sin_family = AF_INET,
                           .sin_addr.s_addr = config.address};
  request.ifra_mask = request.ifra_addr;
  request.ifra_mask.sin_addr.s_addr = config.netmask;
  request.ifra_broadaddr = request.ifra_addr;
  request.ifra_broadaddr.sin_addr.s_addr = config.address | ~config.netmask;
  int error =
      in_control_ioctl(SIOCAIFADDR, &request, interface, curthread->td_ucred);
  if (!error && config.gateway) {
    struct sockaddr_in dst = {.sin_len = sizeof(dst), .sin_family = AF_INET};
    struct sockaddr_in mask = dst, gw = dst;
    gw.sin_addr.s_addr = config.gateway;
    struct rt_addrinfo info = {.rti_flags = RTF_UP | RTF_GATEWAY};
    info.rti_info[RTAX_DST] = (struct sockaddr*)&dst;
    info.rti_info[RTAX_GATEWAY] = (struct sockaddr*)&gw;
    info.rti_info[RTAX_NETMASK] = (struct sockaddr*)&mask;
    struct rib_cmd_info result;
    struct epoch_tracker epoch;
    NET_EPOCH_ENTER(epoch);
    error = rib_action(0, RTM_ADD, &info, &result);
    NET_EPOCH_EXIT(epoch);
  }
  if (error) {
    ether_ifdetach(interface);
    if_free(interface);
    interface = NULL;
  }
  return error;
}
static void input(const void* bytes, size_t length, bool local) {
  if (!interface || length < ETHER_HDR_LEN || length > ETHER_MAX_LEN) return;
  if (local) {
    const unsigned char* frame = bytes;
    if (length < ETHER_HDR_LEN + 20 || frame[12] != 8 || frame[13] != 0 ||
        memcmp(frame + 26, &config.address, 4) != 0 ||
        memcmp(frame + 30, &config.address, 4) != 0)
      return;
  }
  struct mbuf* m = m_getcl(M_NOWAIT, MT_DATA, M_PKTHDR);
  if (!m) {
    if_inc_counter(interface, IFCOUNTER_IQDROPS, 1);
    return;
  }
  m->m_len = m->m_pkthdr.len = length;
  m->m_pkthdr.rcvif = interface;
  memcpy(mtod(m, void*), bytes, length);
  struct epoch_tracker epoch;
  NET_EPOCH_ENTER(epoch);
  // Preserve the stack's anti-spoofing check for physical input. Only the
  // host's trusted local queue may present a locally sourced packet via lo0.
  if (local)
    if_simloop(V_loif, m, AF_INET, ETHER_HDR_LEN);
  else
    if_input(interface, m);
  NET_EPOCH_EXIT(epoch);
}
void bycorf_bsd_input(const void* bytes, size_t length) {
  input(bytes, length, false);
}
void bycorf_bsd_local_input(const void* bytes, size_t length) {
  input(bytes, length, true);
}
