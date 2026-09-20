// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef BYCORF_NET_CONNECTION_STATS_H_
#define BYCORF_NET_CONNECTION_STATS_H_

#include <cstdint>

namespace bycorf {

// Counts active registered streams across every worker and Runtime in this
// process, including outgoing connections. Listening sockets and pending
// connects are not registered streams. Closing removes a stream immediately,
// before its outstanding I/O and storage are retired.
//
// Safe to sample from any thread without waking or waiting for workers. The
// counter changes only when a connection opens/closes, never per request.
std::uint64_t ProcessActiveConnectionCount() noexcept;

}  // namespace bycorf

#endif  // BYCORF_NET_CONNECTION_STATS_H_
