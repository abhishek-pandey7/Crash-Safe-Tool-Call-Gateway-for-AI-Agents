# syntax=docker/dockerfile:1
#
# The comparison image. The other tools are installed here, at pinned
# versions, and only ever run inside this image. Build from the repository
# root:
#
#   docker build -f compare/Dockerfile -t crashsafe-compare .

FROM golang:1.27

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv nodejs npm redis-server git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# once-gate 0.2.1, from PyPI.
RUN python3 -m venv /opt/oncegate && /opt/oncegate/bin/pip install --no-cache-dir once-gate==0.2.1

# agentsafe-mcp, module tag mcp/v0.6.0.
RUN GOBIN=/usr/local/bin go install github.com/Ashutosh2308Bhardwaj/agentsafe/mcp/cmd/agentsafe-mcp@v0.6.0

# mcp-idempotent is not published to npm; built from its repository at a
# pinned commit.
RUN git clone https://github.com/krish-shahh/mcp-idempotent /opt/mcp-idempotent \
 && cd /opt/mcp-idempotent \
 && git checkout 17bb541c00e5a084a26166c4d505e334ee1ce7fc \
 && npm ci --no-audit --no-fund \
 && npm run build
COPY compare/mcp-idempotent-redis.mjs /opt/mcp-idempotent/redis-proxy.mjs

# This repository: the gateway, the stdio fake tool and the driver.
WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
RUN go build -o /usr/local/bin/gateway ./cmd/gateway \
 && cd compare \
 && go build -o /usr/local/bin/stdiofake ./stdiofake \
 && go build -o /usr/local/bin/compare-run ./run

WORKDIR /work
# Redis backs mcp-idempotent's Redis store and runs beside the driver.
ENTRYPOINT ["/bin/sh", "-c", "redis-server --daemonize yes --save '' --appendonly no > /dev/null && exec compare-run \"$@\"", "--"]
