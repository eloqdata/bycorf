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

#include "bycorf/io/spdk_storage.h"

#if BYCORF_KERNEL_BYPASS

#include <fcntl.h>
#include <pthread.h>
#include <spdk/env.h>
#include <spdk/nvme.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <unordered_map>
#include <utility>

#include "bycorf/io/dpdk_environment.h"
#include "bycorf/runtime/worker.h"

namespace bycorf {
namespace {

constexpr std::string_view kSpdkPrefix = "spdk://";

struct ParsedPath {
  std::string traddr_;
  std::uint32_t nsid_ = 1;
  std::string canonical_;
};

struct SpdkController {
  std::string traddr_;
  spdk_nvme_ctrlr* ctrlr_ = nullptr;
  spdk_nvme_qpair* sync_qpair_ = nullptr;
  unsigned io_queue_count_ = 0;
  std::mutex sync_mutex_;
};

struct SpdkDevice {
  std::string path_;
  std::string traddr_;
  std::uint32_t nsid_ = 1;
  SpdkController* controller_ = nullptr;
  spdk_nvme_ns* ns_ = nullptr;
  std::uint32_t sector_size_ = 0;
  std::uint64_t size_bytes_ = 0;
};

struct ProbeContext {
  std::string traddr_;
  std::uint32_t nsid_ = 1;
  spdk_nvme_ctrlr* ctrlr_ = nullptr;
  unsigned io_queue_count_ = 0;
};

std::mutex g_spdk_mutex;
std::unordered_map<std::string, std::unique_ptr<SpdkDevice>> g_spdk_devices;
std::unordered_map<std::string, std::unique_ptr<SpdkController>>
    g_spdk_controllers;

absl::StatusOr<ParsedPath> ParsePath(std::string_view path) {
  if (!path.starts_with(kSpdkPrefix)) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "SPDK path must start with spdk://");
  }
  std::string_view body = path.substr(kSpdkPrefix.size());
  if (body.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "SPDK path is missing PCI address");
  }
  std::uint32_t nsid = 1;
  const std::size_t slash = body.find('/');
  std::string_view address = body.substr(0, slash);
  if (slash != std::string_view::npos) {
    std::string_view ns = body.substr(slash + 1);
    if (ns.empty()) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "SPDK namespace id is empty");
    }
    const auto parsed = std::from_chars(ns.data(), ns.data() + ns.size(), nsid);
    if (parsed.ec != std::errc{} || parsed.ptr != ns.data() + ns.size() ||
        nsid == 0) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "invalid SPDK namespace id");
    }
  }
  if (address.empty() || address.size() >= SPDK_NVMF_TRADDR_MAX_LEN) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid SPDK PCI address");
  }
  ParsedPath result;
  result.traddr_ = std::string(address);
  result.nsid_ = nsid;
  result.canonical_ = std::string(kSpdkPrefix) + result.traddr_ + "/" +
                      std::to_string(result.nsid_);
  return result;
}

bool ProbeCallback(void* context, const spdk_nvme_transport_id* trid,
                   spdk_nvme_ctrlr_opts* options) {
  auto* probe = static_cast<ProbeContext*>(context);
  if (probe->traddr_ != trid->traddr) {
    return false;
  }
  options->num_io_queues = std::max(options->num_io_queues, 16U);
  options->io_queue_size = std::max(options->io_queue_size, 256U);
  options->io_queue_requests = std::max(options->io_queue_requests, 512U);
  return true;
}

void AttachCallback(void* context, const spdk_nvme_transport_id* trid,
                    spdk_nvme_ctrlr* ctrlr,
                    const spdk_nvme_ctrlr_opts* options) {
  auto* probe = static_cast<ProbeContext*>(context);
  if (probe->traddr_ == trid->traddr) {
    probe->ctrlr_ = ctrlr;
    probe->io_queue_count_ = options->num_io_queues;
  }
}

absl::StatusOr<SpdkDevice*> GetDevice(std::string_view path) {
  FreezeIoBackends();
  if (!SpdkStorageEnabled())
    return absl::FailedPreconditionError("SPDK storage is disabled");
  auto parsed = ParsePath(path);
  if (!parsed.ok()) {
    return parsed.status();
  }
  std::lock_guard<std::mutex> lock(g_spdk_mutex);
  auto existing = g_spdk_devices.find(parsed->canonical_);
  if (existing != g_spdk_devices.end()) {
    return existing->second.get();
  }
  absl::Status initialized = EnsureDpdkEnvironment();
  if (!initialized.ok()) {
    return initialized;
  }

  SpdkController* controller = nullptr;
  auto known = g_spdk_controllers.find(parsed->traddr_);
  if (known != g_spdk_controllers.end()) {
    controller = known->second.get();
  } else {
    spdk_nvme_transport_id trid{};
    trid.trtype = SPDK_NVME_TRANSPORT_PCIE;
    std::snprintf(trid.trstring, sizeof(trid.trstring), "%s", "PCIE");
    std::snprintf(trid.traddr, sizeof(trid.traddr), "%s",
                  parsed->traddr_.c_str());
    ProbeContext context{.traddr_ = parsed->traddr_, .nsid_ = parsed->nsid_};
    if (spdk_nvme_probe(&trid, &context, ProbeCallback, AttachCallback,
                        nullptr) != 0 ||
        context.ctrlr_ == nullptr) {
      return absl::Status(
          absl::StatusCode::kNotFound,
          "SPDK could not attach NVMe controller " + parsed->traddr_);
    }
    auto attached = std::make_unique<SpdkController>();
    attached->traddr_ = parsed->traddr_;
    attached->ctrlr_ = context.ctrlr_;
    attached->io_queue_count_ = context.io_queue_count_;
    attached->sync_qpair_ =
        spdk_nvme_ctrlr_alloc_io_qpair(attached->ctrlr_, nullptr, 0);
    if (attached->sync_qpair_ == nullptr) {
      return absl::Status(absl::StatusCode::kResourceExhausted,
                          "SPDK failed to allocate metadata I/O qpair");
    }
    controller = attached.get();
    g_spdk_controllers.emplace(parsed->traddr_, std::move(attached));
  }

  spdk_nvme_ns* ns = spdk_nvme_ctrlr_get_ns(controller->ctrlr_, parsed->nsid_);
  if (ns == nullptr || !spdk_nvme_ns_is_active(ns)) {
    return absl::Status(
        absl::StatusCode::kNotFound,
        "SPDK NVMe namespace is not active: " + parsed->canonical_);
  }
  const std::uint32_t sector_size = spdk_nvme_ns_get_sector_size(ns);
  if (sector_size == 0 || (sector_size & (sector_size - 1)) != 0) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "SPDK namespace reports invalid sector size");
  }
  auto device = std::make_unique<SpdkDevice>();
  device->path_ = parsed->canonical_;
  device->traddr_ = parsed->traddr_;
  device->nsid_ = parsed->nsid_;
  device->controller_ = controller;
  device->ns_ = ns;
  device->sector_size_ = sector_size;
  device->size_bytes_ = spdk_nvme_ns_get_size(ns);
  SpdkDevice* raw = device.get();
  g_spdk_devices.emplace(parsed->canonical_, std::move(device));
  return raw;
}

struct SyncCompletion {
  bool done_ = false;
  bool error_ = false;
};

void CompleteSync(void* context, const spdk_nvme_cpl* completion) {
  auto* result = static_cast<SyncCompletion*>(context);
  result->error_ = spdk_nvme_cpl_is_error(completion);
  result->done_ = true;
}

absl::Status SubmitSync(SpdkDevice& device, void* buffer, std::size_t bytes,
                        std::uint64_t offset, bool write, bool flush) {
  if (bytes == 0) {
    return absl::OkStatus();
  }
  if (offset % device.sector_size_ != 0 || bytes % device.sector_size_ != 0 ||
      offset > device.size_bytes_ || bytes > device.size_bytes_ - offset) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "unaligned or out-of-range SPDK I/O");
  }
  const std::uint64_t lba = offset / device.sector_size_;
  const std::uint32_t lba_count =
      static_cast<std::uint32_t>(bytes / device.sector_size_);
  SpdkController& controller = *device.controller_;
  if (controller.sync_qpair_ == nullptr) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "SPDK metadata qpair has already been released");
  }
  SyncCompletion completion;
  int rc =
      write
          ? spdk_nvme_ns_cmd_write(device.ns_, controller.sync_qpair_, buffer,
                                   lba, lba_count, CompleteSync, &completion, 0)
          : spdk_nvme_ns_cmd_read(device.ns_, controller.sync_qpair_, buffer,
                                  lba, lba_count, CompleteSync, &completion, 0);
  if (rc != 0) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "SPDK metadata I/O submission failed");
  }
  while (!completion.done_) {
    if (spdk_nvme_qpair_process_completions(controller.sync_qpair_, 0) < 0) {
      return absl::Status(absl::StatusCode::kUnavailable,
                          "SPDK metadata qpair failed");
    }
  }
  if (completion.error_) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "SPDK metadata I/O completion failed");
  }
  if (!write || !flush) {
    return absl::OkStatus();
  }
  completion = {};
  rc = spdk_nvme_ns_cmd_flush(device.ns_, controller.sync_qpair_, CompleteSync,
                              &completion);
  if (rc != 0) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "SPDK metadata flush submission failed");
  }
  while (!completion.done_) {
    if (spdk_nvme_qpair_process_completions(controller.sync_qpair_, 0) < 0) {
      return absl::Status(absl::StatusCode::kUnavailable,
                          "SPDK metadata flush qpair failed");
    }
  }
  return completion.error_
             ? absl::Status(absl::StatusCode::kDataLoss,
                            "SPDK metadata flush completion failed")
             : absl::OkStatus();
}

}  // namespace

bool IsSpdkStoragePath(std::string_view path) noexcept {
  return path.starts_with(kSpdkPrefix);
}

absl::StatusOr<SpdkStorageDeviceInfo> ProbeSpdkStorage(std::string_view path) {
  auto device = GetDevice(path);
  if (!device.ok()) {
    return device.status();
  }
  return SpdkStorageDeviceInfo{
      .io_alignment_ = (*device)->sector_size_,
      .size_bytes_ = (*device)->size_bytes_,
      .controller_id_ = (*device)->controller_->traddr_,
      .io_queue_count_ = (*device)->controller_->io_queue_count_};
}

void ReleaseSpdkStorageMetadataQpairs() noexcept {
  std::lock_guard<std::mutex> lock(g_spdk_mutex);
  for (auto& [_, controller] : g_spdk_controllers) {
    std::lock_guard<std::mutex> controller_lock(controller->sync_mutex_);
    if (controller->sync_qpair_ != nullptr) {
      spdk_nvme_ctrlr_free_io_qpair(controller->sync_qpair_);
      controller->sync_qpair_ = nullptr;
    }
  }
}

absl::Status ReadSpdkStorage(std::string_view path, std::span<std::byte> output,
                             std::uint64_t offset) {
  auto device = GetDevice(path);
  if (!device.ok()) {
    return device.status();
  }
  void* dma = spdk_dma_zmalloc(output.size(), (*device)->sector_size_, nullptr);
  if (dma == nullptr) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "SPDK metadata DMA allocation failed");
  }
  std::lock_guard<std::mutex> lock((*device)->controller_->sync_mutex_);
  absl::Status status =
      SubmitSync(**device, dma, output.size(), offset, false, false);
  if (status.ok()) {
    std::memcpy(output.data(), dma, output.size());
  }
  spdk_dma_free(dma);
  return status;
}

absl::Status WriteSpdkStorage(std::string_view path,
                              std::span<const std::byte> input,
                              std::uint64_t offset, bool flush) {
  auto device = GetDevice(path);
  if (!device.ok()) {
    return device.status();
  }
  void* dma = spdk_dma_malloc(input.size(), (*device)->sector_size_, nullptr);
  if (dma == nullptr) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "SPDK metadata DMA allocation failed");
  }
  std::memcpy(dma, input.data(), input.size());
  std::lock_guard<std::mutex> lock((*device)->controller_->sync_mutex_);
  absl::Status status =
      SubmitSync(**device, dma, input.size(), offset, true, flush);
  spdk_dma_free(dma);
  return status;
}

void* AllocateStorageBuffer(std::size_t bytes, std::size_t alignment) noexcept {
  FreezeIoBackends();
  if (!SpdkStorageEnabled())
    return ::operator new[](bytes, std::align_val_t(alignment), std::nothrow);
  if (!EnsureDpdkEnvironment().ok()) return nullptr;
  return spdk_dma_zmalloc(bytes, alignment, nullptr);
}

void FreeStorageBuffer(void* buffer, std::size_t alignment) noexcept {
  if (SpdkStorageEnabled())
    spdk_dma_free(buffer);
  else
    ::operator delete[](buffer, std::align_val_t(alignment));
}

void SpdkStorageBackend::CompleteAsync(void* context,
                                       const spdk_nvme_cpl* completion) {
  auto* request = static_cast<AsyncRequest*>(context);
  const int result =
      spdk_nvme_cpl_is_error(completion) ? -EIO : request->success_result_;
  SpdkStorageBackend* backend = request->backend_;
  IoCompletion* tag = request->tag_;
  backend->CompleteRequest(tag, result);
  backend->ReleaseRequest(request);
}

SpdkStorageBackend::~SpdkStorageBackend() { Shutdown(); }

void SpdkStorageBackend::CompleteRequest(IoCompletion* tag, int result) {
  --outstanding_;
  tag->Complete(*worker_, result, 0);
}

absl::Status SpdkStorageBackend::Init(Worker* worker) {
  if (worker == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "SPDK storage backend requires a worker");
  }
  worker_ = worker;
  // This is deliberately much larger than the registered read-buffer count.
  // It covers foreground reads plus writes/flushes without a hot-path malloc.
  request_pool_.resize(4096);
  free_requests_ = nullptr;
  for (AsyncRequest& request : request_pool_) {
    request.next_ = free_requests_;
    free_requests_ = &request;
  }
  initialized_ = true;
  return absl::OkStatus();
}

void SpdkStorageBackend::Shutdown() {
  if (!initialized_) {
    return;
  }
  while (outstanding_ != 0) {
    Poll();
  }
  for (ControllerChannel& channel : channels_) {
    if (channel.qpair_ != nullptr) {
      spdk_nvme_ctrlr_free_io_qpair(channel.qpair_);
      channel.qpair_ = nullptr;
    }
  }
  files_.clear();
  channels_.clear();
  pending_completions_.clear();
  request_pool_.clear();
  free_requests_ = nullptr;
  next_poll_channel_ = 0;
  worker_ = nullptr;
  initialized_ = false;
}

absl::Status SpdkStorageBackend::RegisterFixedFiles(unsigned count) {
  if (!initialized_ || count == 0 || !files_.empty()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "invalid SPDK fixed-file table registration");
  }
  files_.resize(count);
  return absl::OkStatus();
}

absl::Status SpdkStorageBackend::RegisterBuffers(
    std::span<const iovec> buffers) {
  if (!initialized_ || buffers.empty()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "invalid SPDK DMA-buffer registration");
  }
  for (const iovec& buffer : buffers) {
    if (buffer.iov_base == nullptr || buffer.iov_len == 0 ||
        spdk_vtophys(buffer.iov_base, nullptr) == SPDK_VTOPHYS_ERROR) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "storage buffer is not SPDK DMA addressable");
    }
  }
  return absl::OkStatus();
}

SpdkStorageBackend::OpenFile* SpdkStorageBackend::Lookup(FixedFile file) {
  return file.index_ < files_.size() && files_[file.index_].qpair_ != nullptr
             ? &files_[file.index_]
             : nullptr;
}

SpdkStorageBackend::AsyncRequest*
SpdkStorageBackend::AcquireRequest() noexcept {
  AsyncRequest* request = free_requests_;
  if (request != nullptr) {
    free_requests_ = request->next_;
    request->next_ = nullptr;
  }
  return request;
}

void SpdkStorageBackend::ReleaseRequest(AsyncRequest* request) noexcept {
  request->backend_ = nullptr;
  request->tag_ = nullptr;
  request->success_result_ = 0;
  request->next_ = free_requests_;
  free_requests_ = request;
}

absl::Status SpdkStorageBackend::SubmitOpenDirect(std::string_view path,
                                                  int flags, mode_t,
                                                  FixedFile file,
                                                  IoCompletion* tag) {
  if ((flags & O_RDWR) == 0 || file.index_ >= files_.size() || tag == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid SPDK open request");
  }
  auto device = GetDevice(path);
  if (!device.ok()) {
    return device.status();
  }
  OpenFile& opened = files_[file.index_];
  if (opened.qpair_ != nullptr) {
    return absl::Status(absl::StatusCode::kAlreadyExists,
                        "SPDK fixed file is already open");
  }
  SpdkController* controller = (*device)->controller_;
  auto channel = std::find_if(channels_.begin(), channels_.end(),
                              [controller](const ControllerChannel& candidate) {
                                return candidate.controller_ == controller;
                              });
  if (channel == channels_.end()) {
    spdk_nvme_qpair* qpair =
        spdk_nvme_ctrlr_alloc_io_qpair(controller->ctrlr_, nullptr, 0);
    if (qpair == nullptr) {
      return absl::Status(absl::StatusCode::kResourceExhausted,
                          "SPDK failed to allocate worker I/O qpair");
    }
    channels_.push_back(ControllerChannel{
        .controller_ = controller, .qpair_ = qpair, .open_files_ = 0});
    channel = std::prev(channels_.end());
  }
  opened.device_ = *device;
  opened.qpair_ = channel->qpair_;
  ++channel->open_files_;
  // The coroutine awaitable stores its continuation only after this submit
  // method returns.  Defer even immediately completed operations until Poll().
  pending_completions_.emplace_back(tag, 0);
  return absl::OkStatus();
}

absl::Status SpdkStorageBackend::SubmitCloseDirect(FixedFile file,
                                                   IoCompletion* tag) {
  OpenFile* opened = Lookup(file);
  if (opened == nullptr || tag == nullptr || outstanding_ != 0) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "invalid or busy SPDK close request");
  }
  auto channel = std::find_if(channels_.begin(), channels_.end(),
                              [opened](const ControllerChannel& candidate) {
                                return candidate.qpair_ == opened->qpair_;
                              });
  assert(channel != channels_.end() && channel->open_files_ != 0);
  if (--channel->open_files_ == 0) {
    spdk_nvme_ctrlr_free_io_qpair(channel->qpair_);
    channels_.erase(channel);
    if (channels_.empty()) {
      next_poll_channel_ = 0;
    } else if (next_poll_channel_ >= channels_.size()) {
      next_poll_channel_ %= channels_.size();
    }
  }
  *opened = {};
  pending_completions_.emplace_back(tag, 0);
  return absl::OkStatus();
}

absl::Status SpdkStorageBackend::SubmitIo(FixedFile file, void* buffer,
                                          std::size_t bytes,
                                          std::uint64_t offset, bool write,
                                          IoCompletion* tag) {
  OpenFile* opened = Lookup(file);
  if (opened == nullptr || buffer == nullptr || bytes == 0 || tag == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid SPDK I/O request");
  }
  auto* device = static_cast<SpdkDevice*>(opened->device_);
  const std::uint32_t sector = device->sector_size_;
  if (offset % sector != 0 || bytes % sector != 0 ||
      bytes / sector > std::numeric_limits<std::uint32_t>::max() ||
      offset > device->size_bytes_ || bytes > device->size_bytes_ - offset) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "unaligned SPDK I/O request");
  }
  AsyncRequest* request = AcquireRequest();
  if (request == nullptr) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "SPDK request pool exhausted");
  }
  request->backend_ = this;
  request->tag_ = tag;
  request->success_result_ = static_cast<int>(bytes);
  const std::uint64_t lba = offset / sector;
  const std::uint32_t count = static_cast<std::uint32_t>(bytes / sector);
  int rc = write ? spdk_nvme_ns_cmd_write(device->ns_, opened->qpair_, buffer,
                                          lba, count, CompleteAsync, request, 0)
                 : spdk_nvme_ns_cmd_read(device->ns_, opened->qpair_, buffer,
                                         lba, count, CompleteAsync, request, 0);
  if (rc != 0) {
    ReleaseRequest(request);
    return absl::Status(absl::StatusCode::kUnavailable,
                        "SPDK I/O submission failed");
  }
  ++outstanding_;
  return absl::OkStatus();
}

absl::Status SpdkStorageBackend::SubmitReadFixed(FixedFile file,
                                                 FixedBuffer buffer,
                                                 std::uint64_t offset,
                                                 IoCompletion* tag) {
  return SubmitIo(file, buffer.data_, buffer.size_, offset, false, tag);
}

absl::Status SpdkStorageBackend::SubmitRead(FixedFile file,
                                            std::span<std::byte> buffer,
                                            std::uint64_t offset,
                                            IoCompletion* tag) {
  return SubmitIo(file, buffer.data(), buffer.size(), offset, false, tag);
}

absl::Status SpdkStorageBackend::SubmitWrite(FixedFile file,
                                             std::span<const std::byte> buffer,
                                             std::uint64_t offset,
                                             IoCompletion* tag) {
  return SubmitIo(file, const_cast<std::byte*>(buffer.data()), buffer.size(),
                  offset, true, tag);
}

absl::Status SpdkStorageBackend::SubmitWriteFixed(FixedFile file,
                                                  FixedBuffer buffer,
                                                  std::uint64_t offset,
                                                  IoCompletion* tag) {
  return SubmitIo(file, buffer.data_, buffer.size_, offset, true, tag);
}

absl::Status SpdkStorageBackend::SubmitFdatasync(FixedFile file,
                                                 IoCompletion* tag) {
  OpenFile* opened = Lookup(file);
  if (opened == nullptr || tag == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid SPDK flush request");
  }
  AsyncRequest* request = AcquireRequest();
  if (request == nullptr) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "SPDK request pool exhausted");
  }
  request->backend_ = this;
  request->tag_ = tag;
  request->success_result_ = 0;
  auto* device = static_cast<SpdkDevice*>(opened->device_);
  if (spdk_nvme_ns_cmd_flush(device->ns_, opened->qpair_, CompleteAsync,
                             request) != 0) {
    ReleaseRequest(request);
    return absl::Status(absl::StatusCode::kUnavailable,
                        "SPDK flush submission failed");
  }
  ++outstanding_;
  return absl::OkStatus();
}

SpdkPollResult SpdkStorageBackend::Poll(unsigned max_completions) {
  SpdkPollResult result;
  if (!pending_completions_.empty()) {
    std::vector<std::pair<IoCompletion*, int>> completions;
    completions.swap(pending_completions_);
    for (const auto& [tag, result] : completions) {
      tag->Complete(*worker_, result, 0);
    }
    result.did_work_ = true;
    result.completions_ = static_cast<std::uint32_t>(completions.size());
  }
  const std::size_t channel_count = channels_.size();
  for (std::size_t visited = 0; visited < channel_count; ++visited) {
    const std::size_t index = (next_poll_channel_ + visited) % channel_count;
    ControllerChannel& channel = channels_[index];
    const unsigned remaining =
        max_completions == 0 ? 0
                             : (result.completions_ >= max_completions
                                    ? 0
                                    : max_completions - result.completions_);
    if (max_completions != 0 && remaining == 0) {
      next_poll_channel_ = index;
      break;
    }
    const int32_t completed =
        spdk_nvme_qpair_process_completions(channel.qpair_, remaining);
    if (completed > 0) {
      result.did_work_ = true;
      result.completions_ += static_cast<std::uint32_t>(completed);
    }
    next_poll_channel_ = (index + 1) % channel_count;
  }
  return result;
}

}  // namespace bycorf

#endif  // BYCORF_KERNEL_BYPASS
