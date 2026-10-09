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
// the request pool, pending FIFO, polling and callback ownership stay real.
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
uint64_t __wrap_spdk_nvme_ns_get_size(spdk_nvme_ns*) { return 1ULL << 30; }
const spdk_nvme_ctrlr_data* __wrap_spdk_nvme_ctrlr_get_data(spdk_nvme_ctrlr*) {
  static const auto data = [] {
    spdk_nvme_ctrlr_data value{};
    value.vwc.present = 1;
    return value;
  }();
  return &data;
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
  CHECK(retried.ns ==
        reinterpret_cast<spdk_nvme_ns*>(&controllers[0].namespaces[1]));
  CHECK(retried.buffer == buffer.data() && retried.lba == 2 &&
        retried.count == 8);
  first.completion_tokens = 1;
  Drain(worker, [&] { return first.accepted.size() == 3; });
  CHECK(write.calls == 1 && write.result == static_cast<int>(buffer.size()));
  CHECK(flush.calls == 0);
  CHECK(first.accepted[2].operation == Operation::kFlush);
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
  for (int error : {-EINVAL, -ENOMEM}) {
    queue.submit_error = error;
    Completion rejected;
    // More than the pool capacity catches a request leaked on synchronous
    // rejection. An empty qpair must not defer even an ENOMEM failure.
    for (unsigned i = 0; i < 4097; ++i) {
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

void CheckPool(bycorf::Worker& worker, std::span<std::byte> buffer) {
  for (unsigned round = 0; round < 2; ++round) {
    std::vector<Completion> tags(4096);
    for (auto& tag : tags) CHECK(worker.SubmitRead({0}, buffer, 0, &tag).ok());
    for (auto& tag : tags) CHECK(tag.calls == 0);
    Completion rejected, closed;
    CHECK(worker.SubmitRead({0}, buffer, 0, &rejected).code() ==
          absl::StatusCode::kResourceExhausted);
    CHECK(!worker.SubmitCloseDirect({0}, &closed).ok());
    CHECK(!worker.RegisterFixedFiles(4).ok());
    qpairs[0]->completion_tokens = tags.size();
    Drain(worker, [&] { return tags.back().calls == 1; });
    for (auto& tag : tags)
      CHECK(tag.calls == 1 && tag.result == static_cast<int>(buffer.size()));
    CHECK(rejected.calls == 0 && closed.calls == 0);
    CHECK(qpairs[0]->inflight.empty());
  }
  std::cout << "PASS bounded pool, close protection and complete pool reuse\n";
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
  CheckPool(worker, buffer);

  Completion read, deferred;
  CHECK(worker.SubmitRead({0}, buffer, 0, &read).ok());
  CHECK(worker.SubmitWrite({0}, buffer, 0, &deferred).ok());
  CHECK(read.calls + deferred.calls == 0);
  qpairs[0]->completion_tokens = 2;
  worker.Shutdown();
  CHECK(read.calls == 1 && read.result == 4096);
  CHECK(deferred.calls == 1 && deferred.result == 4096);
  CHECK(qpairs.empty());
  close(wake_fd);
  std::cout << "PASS shutdown drains in-flight and deferred I/O on the owning "
               "worker\n";
}
