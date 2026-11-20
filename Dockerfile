# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN for cmd in gateway gatewayctl faketool mcpcall; do \
      go build -trimpath -ldflags="-s -w" -o /out/$cmd ./cmd/$cmd || exit 1; \
    done

# The gateway, with the operator command and the test client alongside.
FROM gcr.io/distroless/static-debian13:nonroot AS gateway
COPY --from=build /out/gateway /out/gatewayctl /out/mcpcall /usr/local/bin/
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/gateway"]

# The fake tool server, for the demo only.
FROM gcr.io/distroless/static-debian13:nonroot AS faketool
COPY --from=build /out/faketool /usr/local/bin/faketool
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/faketool"]
