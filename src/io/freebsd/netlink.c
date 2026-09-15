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
#include <sys/socket.h>
#include <net/route.h>
#include <net/route/route_ctl.h>
#include <netlink/netlink.h>
#include <netlink/netlink_ctl.h>
#include <netlink/netlink_var.h>
#include <netlink/route/route_var.h>

// Route changes have no routing-socket/netlink subscribers in this process.
// Native route code always dereferences both callback tables, even when the
// corresponding protocol is disabled, so the empty tables must be nonnull.
static void route_event(uint32_t fib, const struct rib_cmd_info* info) {}
static void interface_event(struct ifnet* interface, int flags) {}
static struct rtbridge ignored_events = {.route_f = route_event,
                                         .ifmsg_f = interface_event};
struct rtbridge* rtsock_callback_p = &ignored_events;
struct rtbridge* netlink_callback_p = &ignored_events;

// Interface configuration uses the native IPv4 control API. There is no public
// netlink writer or message endpoint, and attempts cannot report success.
int nl_modify_ifp_generic(struct ifnet* ifp, struct nl_parsed_link* attrs,
                          const struct nlattr_bmask* mask,
                          struct nl_pstate* state) {
  return EOPNOTSUPP;
}
bool nlattr_add(struct nl_writer* writer, uint16_t type, uint16_t size,
                const void* data) {
  return false;
}
uint32_t nlattr_save_offset(const struct nl_writer* writer) { return 0; }
void* nlmsg_reserve_data_raw(struct nl_writer* writer, size_t size) {
  return NULL;
}
