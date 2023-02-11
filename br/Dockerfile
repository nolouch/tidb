# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builder image
FROM golang:1.19.4-bullseye as builder

RUN apt install -y wget make git gcc && mkdir -p /go/src/github.com/pingcap/tidb

WORKDIR /go/src/github.com/pingcap/tidb

# Cache dependencies
COPY go.mod .
COPY go.sum .
COPY parser/go.mod parser/go.mod
COPY parser/go.sum parser/go.sum

ARG GITHUB_TOKEN
RUN if [ -n "$GITHUB_TOKEN" ]; then git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; fi

ENV GOPRIVATE=github.com/tidbcloud

# Overwrite metrics.EnvRegisterMetricsAtInit when we build tidb-server and br cmd
ENV REGISTER_METRICS_INIT=false

RUN GO111MODULE=on go mod download

# Build real binaries
COPY . .
RUN make build_br

# Executable image
FROM debian:bullseye-slim
RUN apt update && apt install -y bash curl netcat procps dumb-init && rm /bin/sh && ln -s /bin/bash /bin/sh && apt-get clean
ENV BR_LOG_TO_TERM=1

COPY --from=builder /go/src/github.com/pingcap/tidb/bin/br /br

WORKDIR /

EXPOSE 4000

ENTRYPOINT ["/usr/bin/dumb-init", "/br"]
