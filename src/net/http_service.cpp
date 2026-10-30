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

#include "bycorf/net/http_service.h"

#include <limits.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/runtime/worker.h"

namespace bycorf {
namespace {

std::span<const std::byte> Bytes(std::string_view value) {
  return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

}  // namespace

void HttpResponse::AppendBorrowedBody(std::string_view text) {
  if (text.empty()) return;
  if (body_parts_.empty() && !body_.empty()) {
    body_parts_.push_back({nullptr, 0, body_.size()});
  }
  body_parts_.push_back({text.data(), 0, text.size()});
}

void HttpResponse::AppendBody(std::string_view text) {
  if (text.empty()) return;
  const auto offset = body_.size();
  body_.append(text);
  if (body_parts_.empty()) return;
  auto& last = body_parts_.back();
  if (last.data_ == nullptr && last.offset_ + last.size_ == offset) {
    last.size_ += text.size();
  } else {
    body_parts_.push_back({nullptr, offset, text.size()});
  }
}

std::size_t HttpResponse::BodySize() const noexcept {
  if (body_parts_.empty()) return body_.size();
  std::size_t size = 0;
  for (const auto& part : body_parts_) size += part.size_;
  return size;
}

std::string_view HttpResponse::Part(const BodyPart& part) const noexcept {
  return {part.data_ == nullptr ? body_.data() + part.offset_ : part.data_,
          part.size_};
}

bool HttpService::RegisterGet(std::string path, Handler handler) {
  for (const Route& route : get_routes_) {
    if (route.path_ == path) {
      return false;
    }
  }
  get_routes_.push_back(
      Route{.path_ = std::move(path), .handler_ = std::move(handler)});
  return true;
}

void HttpService::Prepare(unsigned thread_count) {
  TcpService::Prepare(thread_count);
  response_buffers_ = std::make_unique<ResponseBufferPool[]>(thread_count);
}

Task<absl::Status> HttpService::Serve(TcpStream stream) {
  constexpr std::size_t kMaxRetainedBuffers = 4;
  constexpr std::size_t kMaxRetainedBufferBytes = 1024 * 1024;
  auto& buffer_pool = response_buffers_[ThisWorker().id_].buffers_;
  HttpResponse response;
  if (!buffer_pool.empty()) {
    response.body_ = std::move(buffer_pool.back().body_);
    response.body_parts_ = std::move(buffer_pool.back().body_parts_);
    buffer_pool.pop_back();
  }
  struct ReturnResponseBuffer {
    HttpResponse& response_;
    std::vector<HttpResponse>& pool_;
    ~ReturnResponseBuffer() {
      response_.body_.clear();
      response_.body_parts_.clear();
      // Retain scratch capacity only after all socket writes have retired.
      if (response_.body_.capacity() + response_.body_parts_.capacity() *
                                           sizeof(HttpResponse::BodyPart) <=
              kMaxRetainedBufferBytes &&
          pool_.size() < kMaxRetainedBuffers) {
        pool_.push_back(std::move(response_));
      }
    }
  } return_buffer{response, buffer_pool};

  constexpr std::size_t kMaxRequestBytes = 16 * 1024;
  std::array<std::byte, 2048> buffer{};
  std::string request_bytes;
  while (request_bytes.find("\r\n\r\n") == std::string::npos) {
    if (request_bytes.size() >= kMaxRequestBytes) {
      response.status_ = "431 Request Header Fields Too Long";
      response.body_ = "request headers are too large\n";
      co_return co_await WriteResponse(stream, response);
    }
    auto read = co_await stream.ReadSome(buffer);
    if (!read.ok()) {
      co_return read.status();
    }
    if (*read == 0) {
      co_return absl::OkStatus();
    }
    if (*read > kMaxRequestBytes - request_bytes.size()) {
      response.status_ = "431 Request Header Fields Too Long";
      response.body_ = "request headers are too large\n";
      co_return co_await WriteResponse(stream, response);
    }
    request_bytes.append(reinterpret_cast<const char*>(buffer.data()), *read);
  }

  const std::size_t line_end = request_bytes.find("\r\n");
  const std::string_view line(request_bytes.data(), line_end);
  const std::size_t first_space = line.find(' ');
  const std::size_t second_space = first_space == std::string_view::npos
                                       ? std::string_view::npos
                                       : line.find(' ', first_space + 1);
  if (first_space == std::string_view::npos ||
      second_space == std::string_view::npos ||
      !line.substr(second_space + 1).starts_with("HTTP/1.")) {
    response.status_ = "400 Bad Request";
    response.body_ = "malformed HTTP request\n";
    co_return co_await WriteResponse(stream, response);
  }

  HttpRequest request{
      .method_ = std::string(line.substr(0, first_space)),
      .target_ = std::string(
          line.substr(first_space + 1, second_space - first_space - 1)),
  };
  if (const std::size_t query = request.target_.find('?');
      query != std::string::npos) {
    request.target_.resize(query);
  }
  if (request.method_ != "GET") {
    response.status_ = "405 Method Not Allowed";
    response.body_ = "only GET is supported\n";
    co_return co_await WriteResponse(stream, response);
  }
  for (const Route& route : get_routes_) {
    if (route.path_ == request.target_) {
      const absl::Status handled = co_await route.handler_(request, &response);
      if (!handled.ok()) {
        response.status_ = "500 Internal Server Error";
        response.content_type_ = "text/plain; charset=utf-8";
        response.body_parts_.clear();
        response.body_ = "request handler failed\n";
      }
      co_return co_await WriteResponse(stream, response);
    }
  }
  response.status_ = "404 Not Found";
  response.body_ = "not found\n";
  co_return co_await WriteResponse(stream, response);
}

Task<absl::Status> HttpService::WriteResponse(TcpStream& stream,
                                              const HttpResponse& response) {
  const std::string header =
      absl::StrCat("HTTP/1.1 ", response.status_,
                   "\r\nContent-Type: ", response.content_type_,
                   "\r\nContent-Length: ", response.BodySize(),
                   "\r\nConnection: close\r\n\r\n");
  absl::Status written = co_await stream.WriteAll(Bytes(header));
  if (!written.ok()) {
    co_return written;
  }
  if (response.body_parts_.empty()) {
    co_return co_await stream.WriteAll(Bytes(response.body_));
  }
  // Bound each sendmsg even when the body contains thousands of pieces.
  // WriteAllV handles partial writes; every referenced byte stays alive in
  // response or in the handler's borrowed immutable storage through the await.
  std::array<iovec, std::min<std::size_t>(IOV_MAX, 1024)> buffers;
  for (std::size_t first = 0; first < response.body_parts_.size();) {
    const auto count =
        std::min(buffers.size(), response.body_parts_.size() - first);
    for (std::size_t i = 0; i < count; ++i) {
      const auto text = response.Part(response.body_parts_[first + i]);
      buffers[i] = {const_cast<char*>(text.data()), text.size()};
    }
    written = co_await stream.WriteAllV(std::span(buffers).first(count));
    if (!written.ok()) co_return written;
    first += count;
  }
  co_return absl::OkStatus();
}

}  // namespace bycorf
