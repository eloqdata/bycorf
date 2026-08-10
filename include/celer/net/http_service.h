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

#ifndef CELER_NET_HTTP_SERVICE_H_
#define CELER_NET_HTTP_SERVICE_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "celer/net/tcp_service.h"
#include "celer/runtime/task.h"

namespace celer {

struct HttpRequest {
  std::string method_;
  std::string target_;
};

struct HttpResponse {
  std::string status_ = "200 OK";
  std::string content_type_ = "text/plain; charset=utf-8";
  std::string body_;
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
  struct Route {
    std::string path_;
    Handler handler_;
  };

  struct alignas(64) ResponseBufferPool {
    std::vector<std::string> buffers_;
  };

  static Task<absl::Status> WriteResponse(TcpStream& stream,
                                          const HttpResponse& response);

  std::vector<Route> get_routes_;
  // One cache-line-aligned pool per worker. Buffers return after the socket
  // write, so periodic scrapes reuse their response capacity.
  std::unique_ptr<ResponseBufferPool[]> response_buffers_;
};

}  // namespace celer

#endif  // CELER_NET_HTTP_SERVICE_H_
