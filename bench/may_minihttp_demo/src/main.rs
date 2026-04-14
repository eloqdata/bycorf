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

use std::io;
use std::env;

use may::config;
use may_minihttp::{HttpServer, HttpService, Request, Response};

#[derive(Clone)]
struct CoronetBench;

impl HttpService for CoronetBench {
    fn call(&mut self, req: Request, res: &mut Response) -> io::Result<()> {
        let path = req.path();
        res.header("Content-Type: text/html; charset=utf-8");
        match path {
            "/" => {
                res.body("OK\n");
            }
            "/ping" => {
                res.body("pong\n");
            }
            _ => {
                if let Some(id) = path.strip_prefix("/users/") {
                    let mut body = String::with_capacity(id.len() + 1);
                    body.push_str(id);
                    body.push('\n');
                    res.body_vec(body.into_bytes());
                } else {
                    res.status_code(404, "Not Found");
                    res.body("Not Found\n");
                }
            }
        }
        Ok(())
    }
}

fn main() {
    let workers = env::args()
        .nth(1)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1);

    config().set_workers(workers);
    eprintln!("may_minihttp_demo listening on 127.0.0.1:8082 workers={workers}");

    let server = HttpServer(CoronetBench)
        .start("127.0.0.1:8082")
        .expect("failed to start may_minihttp demo");
    server.join().expect("server join failed");
}
