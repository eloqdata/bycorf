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

// Exercise the production backend and Worker with link-time SPDK stubs. Only
// device discovery, environment initialization and NVMe commands are replaced;
// the pending FIFO, polling and callback ownership stay real.
#include <fcntl.h>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <limits>
#include <string_view>
#include <type_traits>
#include <vector>

#include "bycorf/io/spdk_storage.h"
#include "bycorf/runtime/worker.h"

namespace {
#define CHECK(condition)                                                \
  do {                                                                  \
    if (!(condition)) {                                                 \
      std::cerr << "FAIL line " << __LINE__ << ": " #condition << '\n'; \
      std::exit(1);                                                     \
    }                                                                   \
  } while (false)

enum class Operation { kRead, kWrite, kFlush };
struct Namespace {
  unsigned controller;
};
struct Controller {
  std::array<Namespace, 2> namespaces;
};
std::array<Controller, 2> controllers = [] {
  std::array<Controller, 2> result{};
  for (unsigned i = 0; i < result.size(); ++i) {
    for (auto& ns : result[i].namespaces) ns.controller = i;
  }
  return result;
}();
constexpr std::array<const char*, 3> paths{
    "spdk://0000:01:00.0/1", "spdk://0000:01:00.0/2", "spdk://0000:02:00.0/1"};

struct Command {
  Operation operation;
  spdk_nvme_ns* ns;
  void* buffer;
  uint64_t lba;
  uint32_t count;
  spdk_nvme_cmd_cb callback;
  void* context;
};
struct Qpair {
  Controller* controller;
  std::deque<Command> inflight;
  std::vector<Command> accepted;
  unsigned capacity = 1;
  // Completions require explicit tokens, so tests can keep one qpair stalled
  // while another makes progress, independently of Worker polling frequency.
  unsigned completion_tokens = 0;
  unsigned attempts = 0;
  int submit_error = 0;
  bool completion_error = false;
};
std::vector<Qpair*> qpairs;
unsigned environment_calls = 0;
bool volatile_write_cache = true;

Qpair& Queue(spdk_nvme_qpair* qpair) {
  auto* queue = reinterpret_cast<Qpair*>(qpair);
  CHECK(std::find(qpairs.begin(), qpairs.end(), queue) != qpairs.end());
  return *queue;
}

int Submit(spdk_nvme_qpair* qpair, Command command) {
  auto& queue = Queue(qpair);
  CHECK(reinterpret_cast<Namespace*>(command.ns)->controller ==
        static_cast<unsigned>(queue.controller - controllers.data()));
  ++queue.attempts;
  if (queue.submit_error != 0) return queue.submit_error;
  if (queue.inflight.size() >= queue.capacity) return -ENOMEM;
  queue.inflight.push_back(command);
  queue.accepted.push_back(command);
  // SPDK does not retain callbacks or payloads when submission is rejected.
  return 0;
}
}  // namespace

extern "C" {
int __wrap_spdk_env_init(const spdk_env_opts*) {
  ++environment_calls;
  return 0;
}
int __wrap_spdk_nvme_probe(const spdk_nvme_transport_id* trid, void* context,
                           spdk_nvme_probe_cb probe, spdk_nvme_attach_cb attach,
                           spdk_nvme_remove_cb) {
  unsigned index;
  if (std::string_view(trid->traddr) == "0000:01:00.0")
    index = 0;
  else if (std::string_view(trid->traddr) == "0000:02:00.0")
    index = 1;
  else
    return -ENODEV;
  spdk_nvme_ctrlr_opts options{};
  CHECK(probe(context, trid, &options));
  attach(context, trid, reinterpret_cast<spdk_nvme_ctrlr*>(&controllers[index]),
         &options);
  return 0;
}
spdk_nvme_ns* __wrap_spdk_nvme_ctrlr_get_ns(spdk_nvme_ctrlr* ctrlr,
                                            uint32_t id) {
  if (id < 1 || id > 2) return nullptr;
  return reinterpret_cast<spdk_nvme_ns*>(
      &reinterpret_cast<Controller*>(ctrlr)->namespaces[id - 1]);
}
bool __wrap_spdk_nvme_ns_is_active(spdk_nvme_ns*) { return true; }
uint32_t __wrap_spdk_nvme_ns_get_sector_size(spdk_nvme_ns*) { return 512; }
uint64_t __wrap_spdk_nvme_ns_get_size(spdk_nvme_ns*) { return 1ULL << 34; }
const spdk_nvme_ctrlr_data* __wrap_spdk_nvme_ctrlr_get_data(spdk_nvme_ctrlr*) {
  static const auto data = [] {
    spdk_nvme_ctrlr_data value{};
    value.vwc.present = 1;
    return value;
  }();
  static const spdk_nvme_ctrlr_data no_cache{};
  return volatile_write_cache ? &data : &no_cache;
}
spdk_nvme_qpair* __wrap_spdk_nvme_ctrlr_alloc_io_qpair(
    spdk_nvme_ctrlr* ctrlr, const spdk_nvme_io_qpair_opts*, size_t) {
  auto* queue = new Qpair{.controller = reinterpret_cast<Controller*>(ctrlr)};
  qpairs.push_back(queue);
  return reinterpret_cast<spdk_nvme_qpair*>(queue);
}
int __wrap_spdk_nvme_ctrlr_free_io_qpair(spdk_nvme_qpair* qpair) {
  auto* queue = &Queue(qpair);
  CHECK(queue->inflight.empty());
  std::erase(qpairs, queue);
  delete queue;
  return 0;
}
uint32_t __wrap_spdk_nvme_qpair_get_num_outstanding_reqs(
    spdk_nvme_qpair* qpair) {
  return Queue(qpair).inflight.size();
}
int32_t __wrap_spdk_nvme_qpair_process_completions(spdk_nvme_qpair* qpair,
                                                   uint32_t limit) {
  auto& queue = Queue(qpair);
  unsigned completed = 0;
  while (!queue.inflight.empty() && queue.completion_tokens != 0 &&
         (limit == 0 || completed < limit)) {
    auto command = queue.inflight.front();
    queue.inflight.pop_front();
    --queue.completion_tokens;
    ++completed;
    spdk_nvme_cpl completion{};
    if (queue.completion_error)
      completion.status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
    command.callback(command.context, &completion);
  }
  return completed;
}
int __wrap_spdk_nvme_ns_cmd_read(spdk_nvme_ns* ns, spdk_nvme_qpair* qpair,
                                 void* buffer, uint64_t lba, uint32_t count,
                                 spdk_nvme_cmd_cb callback, void* context,
                                 uint32_t flags) {
  CHECK(flags == 0);
  return Submit(qpair,
                {Operation::kRead, ns, buffer, lba, count, callback, context});
}
int __wrap_spdk_nvme_ns_cmd_write(spdk_nvme_ns* ns, spdk_nvme_qpair* qpair,
                                  void* buffer, uint64_t lba, uint32_t count,
                                  spdk_nvme_cmd_cb callback, void* context,
                                  uint32_t flags) {
  CHECK(flags == 0);
  return Submit(qpair,
                {Operation::kWrite, ns, buffer, lba, count, callback, context});
}
int __wrap_spdk_nvme_ns_cmd_flush(spdk_nvme_ns* ns, spdk_nvme_qpair* qpair,
                                  spdk_nvme_cmd_cb callback, void* context) {
  return Submit(qpair,
                {Operation::kFlush, ns, nullptr, 0, 0, callback, context});
}
}  // extern "C"

// Linker wrapping does not type-check against the SPDK declarations by itself.
#define CHECK_SIGNATURE(name) \
  static_assert(std::is_same_v<decltype(&name), decltype(&__wrap_##name)>)
CHECK_SIGNATURE(spdk_env_init);
CHECK_SIGNATURE(spdk_nvme_probe);
CHECK_SIGNATURE(spdk_nvme_ctrlr_get_ns);
CHECK_SIGNATURE(spdk_nvme_ns_is_active);
CHECK_SIGNATURE(spdk_nvme_ns_get_sector_size);
CHECK_SIGNATURE(spdk_nvme_ns_get_size);
CHECK_SIGNATURE(spdk_nvme_ctrlr_get_data);
CHECK_SIGNATURE(spdk_nvme_ctrlr_alloc_io_qpair);
CHECK_SIGNATURE(spdk_nvme_ctrlr_free_io_qpair);
CHECK_SIGNATURE(spdk_nvme_qpair_get_num_outstanding_reqs);
CHECK_SIGNATURE(spdk_nvme_qpair_process_completions);
CHECK_SIGNATURE(spdk_nvme_ns_cmd_read);
CHECK_SIGNATURE(spdk_nvme_ns_cmd_write);
CHECK_SIGNATURE(spdk_nvme_ns_cmd_flush);
#undef CHECK_SIGNATURE

namespace {
struct Completion : bycorf::IoCompletion {
  unsigned calls = 0;
  int result = -1;
  void Complete(bycorf::Worker& worker, int value, unsigned flags) override {
    CHECK(&worker == bycorf::ThisWorker().self_);
    CHECK(flags == 0);
    CHECK(++calls == 1);
    result = value;
  }
};

template <typename Done>
void Drain(bycorf::Worker& worker, Done done) {
  for (unsigned i = 0; i < 20000 && !done(); ++i) worker.RunOnce(false);
  CHECK(done());
}

void CheckQueues(bycorf::Worker& worker, std::span<std::byte> buffer) {
  auto& first = *qpairs[0];
  auto& second = *qpairs[1];
  Completion read, write, flush, other, other_deferred;
  CHECK(worker.SubmitRead({0}, buffer, 512, &read).ok());
  CHECK(worker.SubmitWrite({1}, buffer, 1024, &write).ok());
  CHECK(worker.SubmitFdatasync({0}, &flush).ok());
  CHECK(worker.SubmitRead({2}, buffer, 1536, &other).ok());
  CHECK(worker.SubmitRead({2}, buffer, 2048, &other_deferred).ok());
  CHECK(read.calls + write.calls + flush.calls + other.calls +
            other_deferred.calls ==
        0);
  CHECK(first.accepted.size() == 1 && second.accepted.size() == 1);
  CHECK(first.accepted[0].context == &read);
  CHECK(second.accepted[0].context == &other);
  unsigned attempts = first.attempts;
  for (unsigned i = 0; i < 3; ++i) worker.RunOnce(false);
  CHECK(first.attempts > attempts);
  CHECK(first.accepted.size() == 1);
  CHECK(read.calls + write.calls + flush.calls + other.calls +
            other_deferred.calls ==
        0);

  second.completion_tokens = 2;
  Drain(worker, [&] { return other_deferred.calls == 1; });
  CHECK(other.calls == 1 &&
        other_deferred.result == static_cast<int>(buffer.size()));
  CHECK(other.result == static_cast<int>(buffer.size()));
  CHECK(read.calls + write.calls + flush.calls == 0);
  CHECK(first.accepted.size() == 1 && second.accepted.size() == 2);
  CHECK(second.accepted[1].operation == Operation::kRead);
  CHECK(second.accepted[1].context == &other_deferred);
  CHECK(second.accepted[1].ns ==
        reinterpret_cast<spdk_nvme_ns*>(&controllers[1].namespaces[0]));
  CHECK(second.accepted[1].buffer == buffer.data() &&
        second.accepted[1].lba == 4 && second.accepted[1].count == 8);

  first.completion_tokens = 1;
  Drain(worker, [&] { return first.accepted.size() == 2; });
  CHECK(read.calls == 1 && read.result == static_cast<int>(buffer.size()));
  CHECK(write.calls == 0 && flush.calls == 0);
  const auto& retried = first.accepted[1];
  CHECK(retried.operation == Operation::kWrite);
  CHECK(retried.context == &write);
  CHECK(retried.ns ==
        reinterpret_cast<spdk_nvme_ns*>(&controllers[0].namespaces[1]));
  CHECK(retried.buffer == buffer.data() && retried.lba == 2 &&
        retried.count == 8);
  first.completion_tokens = 1;
  Drain(worker, [&] { return first.accepted.size() == 3; });
  CHECK(write.calls == 1 && write.result == static_cast<int>(buffer.size()));
  CHECK(flush.calls == 0);
  CHECK(first.accepted[2].operation == Operation::kFlush);
  CHECK(first.accepted[2].context == &flush);
  CHECK(first.accepted[2].ns ==
        reinterpret_cast<spdk_nvme_ns*>(&controllers[0].namespaces[0]));
  first.completion_tokens = 1;
  Drain(worker, [&] { return flush.calls == 1; });
  CHECK(flush.result == 0);
  std::cout
      << "PASS independent qpairs, FIFO retries, read/write/flush metadata\n";
}

void CheckErrors(bycorf::Worker& worker, std::span<std::byte> buffer) {
  auto& queue = *qpairs[0];
  Completion oversized;
  const auto attempts = queue.attempts;
  // The stub namespace is larger than this sector-aligned request, but its
  // successful byte count cannot fit in IoCompletion's int result. Stub I/O
  // never touches the buffer; validation must reject it before SPDK is called.
  CHECK(worker
            .SubmitReadFixed({0},
                             {.data_ = buffer.data(),
                              .size_ = static_cast<std::size_t>(
                                           std::numeric_limits<int>::max()) +
                                       1},
                             0, &oversized)
            .code() == absl::StatusCode::kInvalidArgument);
  CHECK(oversized.calls == 0 && queue.attempts == attempts);
  for (int error : {-EINVAL, -ENOMEM}) {
    queue.submit_error = error;
    Completion rejected;
    // An empty qpair cannot make progress through a completion, even for
    // ENOMEM. Rejection must not retain the tag or poison later submissions.
    for (unsigned i = 0; i < 3; ++i) {
      CHECK(worker.SubmitRead({0}, buffer, 0, &rejected).code() ==
            absl::StatusCode::kUnavailable);
    }
    CHECK(rejected.calls == 0);
    queue.submit_error = 0;
  }
  for (int error : {-EINVAL, -ENOMEM}) {
    Completion accepted, deferred;
    CHECK(worker.SubmitRead({0}, buffer, 0, &accepted).ok());
    CHECK(worker.SubmitRead({0}, buffer, 0, &deferred).ok());
    CHECK(accepted.calls + deferred.calls == 0);
    queue.submit_error = error;
    if (error == -ENOMEM) queue.completion_tokens = 1;
    // EINVAL fails the retry even with I/O in flight. ENOMEM fails it once
    // the last in-flight completion is gone: neither may strand the request.
    Drain(worker, [&] { return deferred.calls == 1; });
    CHECK(deferred.result == error);
    CHECK(accepted.calls == (error == -ENOMEM ? 1U : 0U));
    queue.submit_error = 0;
    if (accepted.calls == 0) queue.completion_tokens = 1;
    Drain(worker, [&] { return accepted.calls == 1; });
    CHECK(accepted.result == static_cast<int>(buffer.size()));
  }
  Completion failed;
  queue.completion_error = true;
  CHECK(worker.SubmitRead({0}, buffer, 0, &failed).ok());
  queue.completion_tokens = 1;
  Drain(worker, [&] { return failed.calls == 1; });
  CHECK(failed.result == -EIO);
  queue.completion_error = false;
  std::cout << "PASS immediate errors, retry errors, nonrecoverable ENOMEM, "
               "completion errors\n";
}

void CheckLargeBursts(bycorf::Worker& worker, std::span<std::byte> buffer) {
  auto& first = *qpairs[0];
  auto& second = *qpairs[1];
  constexpr unsigned kCount = 8193;
  // Exercise both a large software backlog and many hardware-accepted I/Os.
  // Neither may impose the former worker-wide 4096-request limit on a peer.
  for (unsigned capacity : {1U, kCount}) {
    first.capacity = capacity;
    const auto first_begin = first.accepted.size();
    const auto second_begin = second.accepted.size();
    std::vector<Completion> tags(kCount);
    for (std::size_t i = 0; i < tags.size(); ++i) {
      const auto bytes = (i % 8 + 1) * 512;
      CHECK(
          worker.SubmitRead({0}, buffer.first(bytes), i * 512, &tags[i]).ok());
    }
    for (const auto& tag : tags) CHECK(tag.calls == 0);
    CHECK(first.accepted.size() == first_begin + capacity);
    Completion other, closed;
    CHECK(worker.SubmitRead({2}, buffer.first(512), 0, &other).ok());
    CHECK(other.calls == 0);
    CHECK(second.accepted.size() == second_begin + 1);
    CHECK(second.accepted.back().context == &other);
    CHECK(!worker.SubmitCloseDirect({0}, &closed).ok());
    CHECK(!worker.RegisterFixedFiles(4).ok());

    second.completion_tokens = 1;
    Drain(worker, [&] { return other.calls == 1; });
    CHECK(other.result == 512);
    for (const auto& tag : tags) CHECK(tag.calls == 0);
    first.completion_tokens = tags.size();
    Drain(worker, [&] { return tags.back().calls == 1; });
    CHECK(first.accepted.size() == first_begin + tags.size());
    for (std::size_t i = 0; i < tags.size(); ++i) {
      CHECK(tags[i].calls == 1 &&
            tags[i].result == static_cast<int>((i % 8 + 1) * 512));
      const auto& command = first.accepted[first_begin + i];
      CHECK(command.context == &tags[i]);
      CHECK(command.lba == i && command.count == i % 8 + 1);
    }
    CHECK(closed.calls == 0);
    CHECK(first.inflight.empty() && second.inflight.empty());
    CHECK(first.completion_tokens == 0 && second.completion_tokens == 0);
  }
  first.capacity = 1;
  std::cout
      << "PASS large queued/issued bursts and independent qpair progress\n";
}

void CheckLateSubmissionFifo(bycorf::Worker& worker,
                             std::span<std::byte> buffer) {
  auto& queue = *qpairs[0];
  const auto begin = queue.accepted.size();
  std::array<Completion, 4> tags;
  for (unsigned i = 0; i < 3; ++i)
    CHECK(worker.SubmitRead({0}, buffer, i * 512, &tags[i]).ok());
  CHECK(queue.accepted.size() == begin + 1);
  queue.capacity = 4;
  const auto attempts = queue.attempts;
  CHECK(worker.SubmitRead({0}, buffer, 1536, &tags.back()).ok());
  CHECK(queue.attempts == attempts);
  for (const auto& tag : tags) CHECK(tag.calls == 0);
  // Capacity is available again, but the late producer cannot skip the two
  // older requests already accepted into this qpair's software FIFO.
  queue.completion_tokens = 1;
  Drain(worker, [&] { return queue.accepted.size() == begin + tags.size(); });
  CHECK(tags.front().calls == 1);
  for (std::size_t i = 0; i < tags.size(); ++i) {
    CHECK(queue.accepted[begin + i].lba == i);
    CHECK(queue.accepted[begin + i].context == &tags[i]);
    if (i != 0) CHECK(tags[i].calls == 0);
  }
  queue.completion_tokens = tags.size() - 1;
  Drain(worker, [&] { return tags.back().calls == 1; });
  queue.capacity = 1;
  std::cout << "PASS late submissions preserve FIFO when capacity returns\n";
}

void CheckCompletionOwnership(bycorf::Worker& worker,
                              std::span<std::byte> buffer) {
  struct ReusedCompletion final : bycorf::IoCompletion {
    explicit ReusedCompletion(std::span<std::byte> bytes) : buffer(bytes) {}
    void Complete(bycorf::Worker& owner, int value, unsigned flags) override {
      CHECK(&owner == bycorf::ThisWorker().self_ && flags == 0);
      CHECK(calls < results.size());
      results[calls++] = value;
      if (calls == 1)
        CHECK(owner.SubmitWrite({1}, buffer.first(1024), 512, this).ok());
      else if (calls == 2)
        CHECK(owner.SubmitFdatasync({0}, this).ok());
    }
    std::span<std::byte> buffer;
    std::array<int, 3> results{};
    unsigned calls = 0;
  } reused(buffer);
  auto& queue = *qpairs[0];
  CHECK(worker.SubmitRead({0}, buffer.first(512), 0, &reused).ok());
  CHECK(reused.calls == 0 && queue.accepted.back().context == &reused);
  for (unsigned expected_calls = 1; expected_calls <= 3; ++expected_calls) {
    queue.completion_tokens = 1;
    Drain(worker, [&] { return reused.calls == expected_calls; });
    CHECK(queue.accepted.back().context == &reused);
  }
  CHECK((reused.results == std::array<int, 3>{512, 1024, 0}));

  struct SelfDeletingCompletion final : bycorf::IoCompletion {
    explicit SelfDeletingCompletion(bool& done) : destroyed(done) {}
    ~SelfDeletingCompletion() override { destroyed = true; }
    void Complete(bycorf::Worker& owner, int result, unsigned flags) override {
      CHECK(&owner == bycorf::ThisWorker().self_);
      CHECK(result == 512 && flags == 0);
      delete this;
    }
    bool& destroyed;
  };
  bool destroyed = false;
  auto* self_deleting = new SelfDeletingCompletion(destroyed);
  CHECK(worker.SubmitRead({0}, buffer.first(512), 0, self_deleting).ok());
  CHECK(!destroyed && queue.accepted.back().context == self_deleting);
  queue.completion_tokens = 1;
  Drain(worker, [&] { return destroyed; });
  std::cout
      << "PASS callback tag reuse, byte/flush results and self-destruction\n";
}

void CheckRetryErrorReentry(bycorf::Worker& worker,
                            std::span<std::byte> buffer) {
  struct ReusedCompletion final : bycorf::IoCompletion {
    explicit ReusedCompletion(std::span<std::byte> bytes) : buffer(bytes) {}
    void Complete(bycorf::Worker& owner, int result, unsigned flags) override {
      CHECK(&owner == bycorf::ThisWorker().self_ && flags == 0);
      CHECK(++calls <= 2);
      if (calls == 1) {
        CHECK(result == -EINVAL);
        qpairs[0]->submit_error = 0;
        CHECK(owner.SubmitRead({0}, buffer.first(1024), 1024, this).ok());
      } else {
        CHECK(result == 1024);
      }
    }
    std::span<std::byte> buffer;
    unsigned calls = 0;
  } reused(buffer);
  auto& queue = *qpairs[0];
  const auto begin = queue.accepted.size();
  Completion active, older;
  CHECK(worker.SubmitRead({0}, buffer, 0, &active).ok());
  CHECK(worker.SubmitRead({0}, buffer.first(2048), 512, &reused).ok());
  CHECK(worker.SubmitRead({0}, buffer, 1536, &older).ok());
  CHECK(active.calls + reused.calls + older.calls == 0);
  queue.submit_error = -EINVAL;
  Drain(worker, [&] { return reused.calls == 1; });
  CHECK(active.calls == 0 && older.calls == 0);
  CHECK(queue.accepted.size() == begin + 1);
  queue.completion_tokens = 1;
  Drain(worker, [&] { return queue.accepted.size() == begin + 2; });
  CHECK(active.calls == 1 && older.calls == 0 && reused.calls == 1);
  CHECK(queue.accepted.back().context == &older);
  queue.completion_tokens = 1;
  Drain(worker, [&] { return queue.accepted.size() == begin + 3; });
  CHECK(older.calls == 1 && reused.calls == 1);
  CHECK(queue.accepted.back().context == &reused);
  CHECK(queue.accepted.back().count == 2 && queue.accepted.back().lba == 2);
  queue.completion_tokens = 1;
  Drain(worker, [&] { return reused.calls == 2; });
  std::cout << "PASS retry-error callback reentry preserves older queued I/O\n";
}

void CheckChannelReopen(bycorf::Worker& worker, std::span<std::byte> buffer) {
  std::array<Completion, 2> closed;
  CHECK(worker.SubmitCloseDirect({0}, &closed[0]).ok());
  CHECK(worker.SubmitCloseDirect({1}, &closed[1]).ok());
  CHECK(closed[0].calls + closed[1].calls == 0);
  CHECK(qpairs.size() == 1);
  Drain(worker, [&] { return closed[1].calls == 1; });
  CHECK(closed[0].calls == 1);

  struct OpeningCompletion final : Completion {
    void Complete(bycorf::Worker& owner, int result, unsigned flags) override {
      Completion::Complete(owner, result, flags);
      for (unsigned i = 0; i < opened.size(); ++i)
        CHECK(
            owner.SubmitOpenDirect(paths[i], O_RDWR, 0, {i}, &opened[i]).ok());
    }
    std::array<Completion, 2> opened;
  } opening;
  // Closing the first controller shifts the remaining file's channel index.
  // Opening a controller from its completion then changes the channel table
  // during polling, while the qpair being polled remains valid.
  CHECK(worker.SubmitRead({2}, buffer.first(512), 0, &opening).ok());
  CHECK(qpairs[0]->accepted.back().context == &opening);
  qpairs[0]->completion_tokens = 1;
  Drain(worker, [&] { return opening.opened.back().calls == 1; });
  CHECK(opening.calls == 1 && opening.result == 512);
  CHECK(opening.opened[0].calls == 1 && qpairs.size() == 2);

  Completion remaining, reopened;
  CHECK(worker.SubmitRead({2}, buffer.first(512), 0, &remaining).ok());
  CHECK(worker.SubmitRead({0}, buffer.first(1024), 0, &reopened).ok());
  CHECK(qpairs[0]->accepted.back().context == &remaining);
  CHECK(qpairs[1]->accepted.back().context == &reopened);
  qpairs[0]->completion_tokens = 1;
  qpairs[1]->completion_tokens = 1;
  Drain(worker, [&] { return remaining.calls + reopened.calls == 2; });
  CHECK(remaining.result == 512 && reopened.result == 1024);

  Completion closed_other, shifted, opened_other;
  CHECK(worker.SubmitCloseDirect({2}, &closed_other).ok());
  CHECK(worker.SubmitRead({1}, buffer.first(2048), 0, &shifted).ok());
  CHECK(qpairs.size() == 1 && qpairs[0]->accepted.back().context == &shifted);
  qpairs[0]->completion_tokens = 1;
  Drain(worker, [&] { return shifted.calls == 1; });
  CHECK(closed_other.calls == 1 && shifted.result == 2048);
  CHECK(worker.SubmitOpenDirect(paths[2], O_RDWR, 0, {2}, &opened_other).ok());
  Drain(worker, [&] { return opened_other.calls == 1; });
  CHECK(qpairs.size() == 2);
  std::cout << "PASS channel removal, callback open and shifted file indices\n";
}

void CheckSyntheticShutdown(bycorf::Worker& worker) {
  for (unsigned operation = 0; operation < 3; ++operation) {
    CHECK(worker.Init({.recv_buffer_count_ = 0}).ok());
    CHECK(worker.RegisterFixedFiles(1).ok());
    Completion opened, completed;
    CHECK(worker.SubmitOpenDirect(paths[0], O_RDWR, 0, {0}, &opened).ok());
    CHECK(opened.calls == 0);
    if (operation != 0) {
      Drain(worker, [&] { return opened.calls == 1; });
      if (operation == 1) {
        CHECK(worker.SubmitCloseDirect({0}, &completed).ok());
      } else {
        volatile_write_cache = false;
        CHECK(worker.SubmitFdatasync({0}, &completed).ok());
      }
      CHECK(completed.calls == 0);
    }
    // No NVMe command is in flight. Shutdown still owes the accepted open,
    // close or no-VWC flush its deferred completion on the owning worker.
    for (const auto* queue : qpairs) CHECK(queue->inflight.empty());
    worker.Shutdown();
    CHECK(opened.calls == 1 && opened.result == 0);
    if (operation != 0) CHECK(completed.calls == 1 && completed.result == 0);
    CHECK(qpairs.empty());
    volatile_write_cache = true;
  }
  std::cout << "PASS shutdown delivers synthetic open/close/no-VWC flush\n";
}
}  // namespace

int main() {
  CHECK(bycorf::ConfigureIoBackends({.spdk_storage = true}).ok());
  for (auto path : paths) CHECK(bycorf::ProbeSpdkStorage(path).ok());
  CHECK(environment_calls == 1);
  bycorf::ReleaseSpdkStorageMetadataQpairs();
  CHECK(qpairs.empty());
  bycorf::CrossCore cross_core(1);
  const int wake_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK(wake_fd >= 0);
  cross_core.mailbox(0).wake_fd_ = wake_fd;
  bycorf::Worker worker;
  worker.BindCrossCore(0, &cross_core);
  bycorf::SetThisWorker(0, &cross_core, &worker);
  CHECK(worker.Init({.spdk_max_completions_per_poll_ = 1}).ok());
  CHECK(worker.RegisterFixedFiles(paths.size()).ok());
  std::array<Completion, 3> opened;
  for (unsigned i = 0; i < paths.size(); ++i) {
    CHECK(worker.SubmitOpenDirect(paths[i], O_RDWR, 0, {i}, &opened[i]).ok());
    CHECK(opened[i].calls == 0);
  }
  Drain(worker, [&] { return opened.back().calls == 1; });
  for (auto& tag : opened) CHECK(tag.calls == 1 && tag.result == 0);
  CHECK(qpairs.size() == 2);
  // Stub commands do not DMA: ordinary memory suffices, with no hugepages,
  // device binding, root privileges or PCI discovery required.
  std::array<std::byte, 4096> buffer{};
  CheckQueues(worker, buffer);
  CheckErrors(worker, buffer);
  CheckLargeBursts(worker, buffer);
  CheckLateSubmissionFifo(worker, buffer);
  CheckCompletionOwnership(worker, buffer);
  CheckRetryErrorReentry(worker, buffer);
  CheckChannelReopen(worker, buffer);

  std::vector<Completion> shutdown(5000);
  for (std::size_t i = 0; i < shutdown.size(); ++i) {
    CHECK((i % 2 == 0 ? worker.SubmitRead({0}, buffer, 0, &shutdown[i])
                      : worker.SubmitWrite({0}, buffer, 0, &shutdown[i]))
              .ok());
    CHECK(shutdown[i].calls == 0);
  }
  Completion other;
  CHECK(worker.SubmitRead({2}, std::span(buffer).first(512), 0, &other).ok());
  CHECK(other.calls == 0);
  qpairs[0]->completion_tokens = shutdown.size();
  qpairs[1]->completion_tokens = 1;
  worker.Shutdown();
  for (const auto& tag : shutdown) CHECK(tag.calls == 1 && tag.result == 4096);
  CHECK(other.calls == 1 && other.result == 512);
  CHECK(qpairs.empty());
  std::cout << "PASS shutdown drains in-flight and deferred I/O on the owning "
               "worker\n";
  CheckSyntheticShutdown(worker);
  close(wake_fd);
}
