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

#ifndef BYCORF_NET_HTTP_SERVICE_H_
#define BYCORF_NET_HTTP_SERVICE_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "bycorf/net/tcp_service.h"
#include "bycorf/runtime/task.h"

namespace bycorf {

struct HttpRequest {
  std::string method_;
  std::string target_;
};

struct HttpResponse {
  struct BodyPart {
    // Null data selects an offset in body_; offsets survive string growth.
    const char* data_ = nullptr;
    std::size_t offset_ = 0;
    std::size_t size_ = 0;
  };
  std::string status_ = "200 OK";
  std::string content_type_ = "text/plain; charset=utf-8";
  std::string body_;
  std::vector<BodyPart> body_parts_;

  // Optional segmented body. Borrowed text must remain immutable and alive
  // until the service finishes sending, including after the handler returns.
  // Owned pieces are copied into body_ and addressed by offset. Once parts
  // are used, do not mutate body_ directly; use these append methods instead.
  void AppendBorrowedBody(std::string_view text);
  void AppendBody(std::string_view text);
  [[nodiscard]] std::size_t BodySize() const noexcept;
  [[nodiscard]] std::string_view Part(const BodyPart& part) const noexcept;
};

// Small pull-endpoint HTTP service. It intentionally supports one request per
// connection and bounded headers; handlers fill a provided reusable response.
class HttpService final : public TcpService {
 public:
  using Handler =
      std::function<Task<absl::Status>(const HttpRequest&, HttpResponse*)>;

  explicit HttpService(std::uint16_t port) : TcpService(port) {}

  bool RegisterGet(std::string path, Handler handler);
  void Prepare(unsigned thread_count) override;

 protected:
  Task<absl::Status> Serve(TcpStream stream) override;

 private:
  friend class HttpServiceTestPeer;
  struct Route {
    std::string path_;
    Handler handler_;
  };

  struct alignas(64) ResponseBufferPool {
    std::vector<HttpResponse> buffers_;
  };

  static Task<absl::Status> WriteResponse(TcpStream& stream,
                                          const HttpResponse& response);

  std::vector<Route> get_routes_;
  // One cache-line-aligned pool per worker. Buffers return after the socket
  // write, so periodic scrapes reuse their response capacity.
  std::unique_ptr<ResponseBufferPool[]> response_buffers_;
};

}  // namespace bycorf

#endif  // BYCORF_NET_HTTP_SERVICE_H_
