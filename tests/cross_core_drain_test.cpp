// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <thread>
#include <vector>

#include "bycorf/runtime/worker.h"

namespace bycorf {

// Drive the real mailbox consumer without I/O initialization or executing the
// queued request/reply coroutines. This makes the admission order
// deterministic.
class WorkerCrossCoreTestPeer {
 public:
  static bool Drain(Worker& worker) { return worker.DrainCrossCore(); }

  static std::vector<RemoteWork*> TakeRequests(Worker& worker) {
    std::vector<RemoteWork*> result(worker.foreground_remote_work_.begin(),
                                    worker.foreground_remote_work_.end());
    worker.foreground_remote_work_.clear();
    return result;
  }

  static std::vector<std::coroutine_handle<>> TakeReplies(Worker& worker) {
    std::vector<std::coroutine_handle<>> result;
    for (auto ready : worker.ready_) result.push_back(ready.handle_);
    worker.ready_.clear();
    return result;
  }
};

namespace {

enum class Kind { kRequest, kReply, kNotification };

Task<absl::Status> SuspendedReply() { co_return absl::OkStatus(); }

class Mailboxes {
 public:
  explicit Mailboxes(unsigned count = 8)
      : cross_core(count), receiver(count - 1), work(count) {
    worker.BindCrossCore(receiver, &cross_core);
    for (unsigned sender = 0; sender < count; ++sender) {
      work[sender].origin_ = sender;
      work[sender].waiter_ = SuspendedReply().ReleaseHandle();
    }
  }

  ~Mailboxes() {
    for (const auto& item : work) item.waiter_.destroy();
  }

  RemoteNotification Notification(unsigned sender) {
    return {.context_ = &notifications,
            .value_ = sender,
            .run_fn_ = +[](void* context, std::uint64_t value) noexcept {
              static_cast<std::vector<unsigned>*>(context)->push_back(value);
            }};
  }

  void Post(Kind kind, unsigned sender, unsigned count) {
    auto& lane = cross_core.lane(receiver, sender);
    for (unsigned i = 0; i < count; ++i) {
      bool queued = false;
      switch (kind) {
        case Kind::kRequest:
          queued = lane.requests_.try_enqueue(&work[sender]);
          break;
        case Kind::kReply:
          queued = lane.replies_.try_enqueue(&work[sender]);
          break;
        case Kind::kNotification:
          queued = lane.notifications_.try_enqueue(Notification(sender));
          break;
      }
      ASSERT_TRUE(queued);
    }
    ActivateCrossCoreLane(&cross_core, receiver, sender, lane);
  }

  void PostOverflow(Kind kind, unsigned sender, unsigned count,
                    moodycamel::ProducerToken& token) {
    auto& mb = cross_core.mailbox(receiver);
    for (unsigned i = 0; i < count; ++i) {
      switch (kind) {
        case Kind::kRequest:
          ASSERT_TRUE(mb.requests_.enqueue(token, &work[sender]));
          break;
        case Kind::kReply:
          ASSERT_TRUE(mb.replies_.enqueue(token, &work[sender]));
          break;
        case Kind::kNotification:
          ASSERT_TRUE(mb.notifications_.enqueue(token, Notification(sender)));
          break;
      }
    }
    mb.overflow_pending_.store(true, std::memory_order_release);
  }

  moodycamel::ProducerToken Producer(Kind kind) {
    auto& mb = cross_core.mailbox(receiver);
    switch (kind) {
      case Kind::kRequest:
        return moodycamel::ProducerToken(mb.requests_);
      case Kind::kReply:
        return moodycamel::ProducerToken(mb.replies_);
      case Kind::kNotification:
        return moodycamel::ProducerToken(mb.notifications_);
    }
    std::terminate();
  }

  std::array<std::vector<unsigned>, 3> Drain() {
    WorkerCrossCoreTestPeer::Drain(worker);
    std::array<std::vector<unsigned>, 3> result;
    for (const auto* item : WorkerCrossCoreTestPeer::TakeRequests(worker)) {
      result[0].push_back(item->origin_);
    }
    for (auto handle : WorkerCrossCoreTestPeer::TakeReplies(worker)) {
      const auto it =
          std::find_if(work.begin(), work.end(),
                       [&](const auto& w) { return w.waiter_ == handle; });
      EXPECT_NE(it, work.end());
      if (it != work.end()) result[1].push_back(it->origin_);
    }
    result[2].swap(notifications);
    for (const auto& messages : result) EXPECT_LE(messages.size(), 64U);
    return result;
  }

  std::vector<unsigned> Drain(Kind kind) {
    auto result = Drain();
    return std::move(result[static_cast<unsigned>(kind)]);
  }

  CrossCore cross_core;
  Worker worker;
  const unsigned receiver;
  std::vector<RemoteWork> work;
  std::vector<unsigned> notifications;
};

class CrossCoreDrain : public testing::TestWithParam<Kind> {};

TEST_P(CrossCoreDrain, RetainsSenderForFourBatchesThenServesWaitingPeer) {
  Mailboxes fixture;
  const Kind kind = GetParam();
  fixture.Post(kind, 5, 1);
  for (unsigned round = 0; round < 4; ++round) {
    fixture.Post(kind, 0, 64);
    EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>(64, 0));
  }
  // Sender 0 is still busy when its quota expires: sender 5 must go first.
  fixture.Post(kind, 0, 64);
  auto messages = fixture.Drain(kind);
  ASSERT_EQ(messages.size(), 64U);
  EXPECT_EQ(messages.front(), 5U);
  EXPECT_EQ(std::count(messages.begin(), messages.end(), 0U), 63);
}

TEST_P(CrossCoreDrain, EmptySenderImmediatelyYieldsRemainingBatch) {
  Mailboxes fixture;
  const Kind kind = GetParam();
  fixture.Post(kind, 0, 7);
  fixture.Post(kind, 5, 100);
  auto expected = std::vector<unsigned>(7, 0);
  expected.insert(expected.end(), 57, 5);
  EXPECT_EQ(fixture.Drain(kind), expected);
  EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>(43, 5));
  EXPECT_TRUE(fixture.Drain(kind).empty());
  fixture.Post(kind, 0, 1);
  EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>{0});
}

TEST_P(CrossCoreDrain, PartialBatchesCannotExceedSenderQuota) {
  Mailboxes fixture;
  const Kind kind = GetParam();
  fixture.Post(kind, 0, 7);
  fixture.Post(kind, 1, 255);
  fixture.Post(kind, 2, 100);
  auto expected = std::vector<unsigned>(7, 0);
  expected.insert(expected.end(), 57, 1);
  EXPECT_EQ(fixture.Drain(kind), expected);
  fixture.Post(kind, 1, 57);
  for (unsigned round = 0; round < 3; ++round) {
    EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>(64, 1));
    fixture.Post(kind, 1, 64);
  }
  // Sender 1 has consumed 249: only seven more are allowed before sender 2.
  expected.assign(7, 1);
  expected.insert(expected.end(), 57, 2);
  EXPECT_EQ(fixture.Drain(kind), expected);
}

TEST_P(CrossCoreDrain, RotatesAcrossBitmapWordsAndWraps) {
  Mailboxes fixture(66);
  const Kind kind = GetParam();
  constexpr std::array<unsigned, 3> senders{0, 63, 64};
  for (unsigned sender : senders) fixture.Post(kind, sender, 64);
  for (unsigned round = 0; round < 25; ++round) {
    const unsigned sender = senders[(round / 4) % senders.size()];
    EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>(64, sender));
    fixture.Post(kind, sender, 64);
  }
}

TEST_P(CrossCoreDrain, OverflowAndLanesBothProgressUnderSustainedLoad) {
  Mailboxes fixture;
  const Kind kind = GetParam();
  auto producer = fixture.Producer(kind);
  fixture.Post(kind, 1, 64);
  for (unsigned round = 0; round < 12; ++round) {
    fixture.PostOverflow(kind, 0, 64, producer);
    const unsigned sender = round % 2;
    EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>(64, sender));
    if (sender == 1) fixture.Post(kind, 1, 64);
  }
}

TEST_P(CrossCoreDrain, OverflowConsumerRotatesBetweenProducers) {
  Mailboxes fixture;
  const Kind kind = GetParam();
  auto first = fixture.Producer(kind);
  auto second = fixture.Producer(kind);
  fixture.PostOverflow(kind, 0, 512, first);
  fixture.PostOverflow(kind, 1, 512, second);
  auto messages = fixture.Drain(kind);
  ASSERT_EQ(messages.size(), 64U);
  const unsigned initial = messages.front();
  EXPECT_EQ(messages, std::vector<unsigned>(64, initial));
  for (unsigned round = 1; round < 4; ++round) {
    EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>(64, initial));
  }
  EXPECT_EQ(fixture.Drain(kind), std::vector<unsigned>(64, 1 - initial));
}

INSTANTIATE_TEST_SUITE_P(MessageKinds, CrossCoreDrain,
                         testing::Values(Kind::kRequest, Kind::kReply,
                                         Kind::kNotification),
                         [](const testing::TestParamInfo<Kind>& info) {
                           switch (info.param) {
                             case Kind::kRequest:
                               return "Requests";
                             case Kind::kReply:
                               return "Replies";
                             case Kind::kNotification:
                               return "Notifications";
                           }
                           return "Unknown";
                         });

TEST(CrossCoreDrainState, MessageKindsShareTheSenderQuota) {
  Mailboxes fixture;
  for (unsigned kind = 0; kind < 3; ++kind) {
    fixture.Post(static_cast<Kind>(kind), 0, 200);
    fixture.Post(static_cast<Kind>(kind), 5, 1);
  }
  auto first = fixture.Drain();
  for (const auto& messages : first) {
    EXPECT_EQ(messages, std::vector<unsigned>(64, 0));
  }
  // The first visit consumed 192 across all three kinds. Only 64 more are
  // allowed before rotating, even though all three queues are still busy.
  auto second = fixture.Drain();
  EXPECT_EQ(second[0], std::vector<unsigned>(64, 0));
  EXPECT_TRUE(second[1].empty());
  EXPECT_TRUE(second[2].empty());
  auto third = fixture.Drain();
  for (const auto& messages : third) {
    auto expected = std::vector<unsigned>{5};
    expected.insert(expected.end(), 63, 0);
    EXPECT_EQ(messages, expected);
  }
}

TEST(CrossCoreDrainState, DifferentMessageKindsCannotResetTheSenderQuota) {
  Mailboxes fixture;
  for (unsigned kind = 0; kind < 3; ++kind) {
    fixture.Post(static_cast<Kind>(kind), kind, 64);
    fixture.Post(static_cast<Kind>(kind), 5, 1);
  }
  // Requests from 0, replies from 1 and notifications from 2 stay busy. They
  // must each yield after four batches, instead of resetting one another's
  // cursor while a later sender remains unserved.
  for (unsigned round = 0; round < 12; ++round) {
    const unsigned sender = round / 4;
    auto messages = fixture.Drain();
    for (unsigned kind = 0; kind < 3; ++kind) {
      EXPECT_EQ(messages[kind], kind == sender
                                    ? std::vector<unsigned>(64, sender)
                                    : std::vector<unsigned>{});
    }
    fixture.Post(static_cast<Kind>(sender), sender, 64);
  }
  auto messages = fixture.Drain();
  for (const auto& batch : messages) {
    ASSERT_FALSE(batch.empty());
    EXPECT_EQ(batch.front(), 5U);
  }
}

TEST(CrossCoreDrainState, CallbackPublicationAfterRequestReadIsNotLost) {
  Mailboxes fixture;
  auto& lane = fixture.cross_core.lane(fixture.receiver, 0);
  ASSERT_TRUE(lane.notifications_.try_enqueue(
      {.context_ = &fixture,
       .value_ = 0,
       .run_fn_ = +[](void* context, std::uint64_t) noexcept {
         auto& f = *static_cast<Mailboxes*>(context);
         f.Post(Kind::kRequest, 0, 1);
         f.Post(Kind::kNotification, 5, 1);
       }}));
  ActivateCrossCoreLane(&fixture.cross_core, fixture.receiver, 0, lane);
  auto first = fixture.Drain();
  EXPECT_TRUE(first[0].empty());
  auto second = fixture.Drain();
  EXPECT_EQ(second[0], std::vector<unsigned>{0});
  EXPECT_EQ(second[2], std::vector<unsigned>{5});
  EXPECT_FALSE(WorkerCrossCoreTestPeer::Drain(fixture.worker));
}

TEST(CrossCoreDrainState,
     OverflowPublishedDuringFastDrainSurvivesUntilNextPoll) {
  Mailboxes fixture;
  auto& lane = fixture.cross_core.lane(fixture.receiver, 0);
  ASSERT_TRUE(lane.notifications_.try_enqueue(
      {.context_ = &fixture,
       .value_ = 0,
       .run_fn_ = +[](void* context, std::uint64_t) noexcept {
         auto& f = *static_cast<Mailboxes*>(context);
         // Publish after the entry flag check selected the SPSC-only path.
         for (unsigned kind = 0; kind < 3; ++kind) {
           const auto message_kind = static_cast<Kind>(kind);
           auto producer = f.Producer(message_kind);
           f.PostOverflow(message_kind, 5, 1, producer);
         }
       }}));
  ActivateCrossCoreLane(&fixture.cross_core, fixture.receiver, 0, lane);
  auto first = fixture.Drain();
  for (const auto& messages : first) EXPECT_TRUE(messages.empty());
  EXPECT_TRUE(fixture.cross_core.mailbox(fixture.receiver)
                  .overflow_pending_.load(std::memory_order_acquire));
  auto second = fixture.Drain();
  for (const auto& messages : second) {
    EXPECT_EQ(messages, std::vector<unsigned>{5});
  }
  EXPECT_FALSE(WorkerCrossCoreTestPeer::Drain(fixture.worker));
}

TEST(CrossCoreDrainState, ConcurrentPublishAndQuotaRotationDoNotLoseMessages) {
  static constexpr unsigned kSenders = 8;
  static constexpr unsigned kPerSender = 20'000;
  CrossCore cross_core(kSenders + 1);
  Worker worker;
  worker.BindCrossCore(kSenders, &cross_core);
  struct Received {
    std::array<unsigned, kSenders> next{};
    unsigned total = 0;
    bool ordered = true;
  } received;
  std::atomic<bool> start{false};
  std::vector<std::jthread> producers;
  for (unsigned sender = 0; sender < kSenders; ++sender) {
    producers.emplace_back([&, sender](std::stop_token stop) {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      auto& lane = cross_core.lane(kSenders, sender);
      for (unsigned sequence = 0; sequence < kPerSender; ++sequence) {
        const RemoteNotification message{
            .context_ = &received,
            .value_ = sender * kPerSender + sequence,
            .run_fn_ = +[](void* context, std::uint64_t value) noexcept {
              auto& r = *static_cast<Received*>(context);
              const unsigned source = value / kPerSender;
              r.ordered &= r.next[source]++ == value % kPerSender;
              ++r.total;
            }};
        while (!lane.notifications_.try_enqueue(message)) {
          if (stop.stop_requested()) return;
          std::this_thread::yield();
        }
        ActivateCrossCoreLane(&cross_core, kSenders, sender, lane);
      }
    });
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  start.store(true, std::memory_order_release);
  while (received.total != kSenders * kPerSender &&
         std::chrono::steady_clock::now() < deadline) {
    WorkerCrossCoreTestPeer::Drain(worker);
  }
  for (auto& producer : producers) producer.request_stop();
  producers.clear();
  EXPECT_EQ(received.total, kSenders * kPerSender);
  EXPECT_TRUE(received.ordered);
  for (unsigned next : received.next) EXPECT_EQ(next, kPerSender);
  EXPECT_FALSE(WorkerCrossCoreTestPeer::Drain(worker));
}

}  // namespace
}  // namespace bycorf
