// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <cstdint>

namespace rdma_fake {
void Reset();
void CheckEmpty();
void SuppressConnect(bool suppress);
void SuppressEstablish(bool suppress);
void Cm(rdma_cm_id* id, rdma_cm_event_type event, int status = 0);
void RoundQp(unsigned extra);
void FailNextQp();
void FailNextNotify();
void Completion(ibv_cq* cq, std::uint32_t qp,
                ibv_wc_status status = IBV_WC_SUCCESS);
void Async(ibv_context* context, ibv_event_type type, ibv_qp* qp = nullptr);
// Inject a CQE exactly when notification is requested, before the CQ recheck.
void CompleteOnArm(ibv_cq* cq, std::uint32_t qp);
int CqPolls(ibv_cq* cq);
int Notifications(ibv_cq* cq);
int AckedCqEvents();
int AckedAsyncEvents();
int LiveQps();
int LiveIds();
}  // namespace rdma_fake
