# Build the MiniKV server with the Go version declared in go.mod.
FROM golang:1.26.3-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/minikv-server ./cmd/server

# Small runtime image. The busybox nc applet is used by Compose health checks.
FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /out/minikv-server /usr/local/bin/minikv-server
COPY config/cluster.docker.json /app/config/cluster.docker.json
RUN mkdir -p /app/data && chown -R 10001:10001 /app
USER 10001:10001
EXPOSE 5000 5001 5002 2112
ENTRYPOINT ["/usr/local/bin/minikv-server"]
