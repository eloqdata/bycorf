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

#include "celer/net/http_service.h"

#include <array>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/worker.h"

namespace celer {
namespace {

std::span<const std::byte> Bytes(std::string_view value) {
  return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

}  // namespace

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
  std::vector<std::string>& buffer_pool =
      response_buffers_[ThisWorker().id_].buffers_;
  HttpResponse response;
  if (!buffer_pool.empty()) {
    response.body_ = std::move(buffer_pool.back());
    buffer_pool.pop_back();
    response.body_.clear();
  }
  struct ReturnResponseBuffer {
    std::string* body_;
    std::vector<std::string>* pool_;
    std::size_t max_buffers_;
    std::size_t max_buffer_bytes_;
    ~ReturnResponseBuffer() {
      if (body_->capacity() <= max_buffer_bytes_ &&
          pool_->size() < max_buffers_) {
        body_->clear();
        pool_->push_back(std::move(*body_));
      }
    }
  } return_buffer{&response.body_, &buffer_pool, kMaxRetainedBuffers,
                  kMaxRetainedBufferBytes};

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
                   "\r\nContent-Length: ", response.body_.size(),
                   "\r\nConnection: close\r\n\r\n");
  absl::Status written = co_await stream.WriteAll(Bytes(header));
  if (!written.ok()) {
    co_return written;
  }
  co_return co_await stream.WriteAll(Bytes(response.body_));
}

}  // namespace celer
