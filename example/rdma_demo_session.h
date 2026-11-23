// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "bycorf/rdma/rc.h"

namespace bycorf::example {
// One finite, checked exchange of every supported RC data opcode. Both peers
// use the same payload size. Owns registrations/backing memory until Close has
// stopped DMA; the caller must likewise quiesce the worker before destroying a
// suspended task. This is an example protocol, not a runtime wire format.
Task<absl::Status> RunRdmaDemo(Worker& worker, rdma::RcConnection& connection,
                               bool server, std::size_t bytes);
}  // namespace bycorf::example
