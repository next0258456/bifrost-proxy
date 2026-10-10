# Build stage (using glibc due to DuckDB CGO requirement)
FROM golang:1.24-bookworm AS builder

WORKDIR /app

# Install dependencies for CGO
RUN apt-get update && apt-get install -y gcc g++ make

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build with CGO enabled
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o bifrost-proxy .

# Run stage
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/bifrost-proxy .

EXPOSE 8080

# SLA Tuning: Giới hạn memory dưới 300MB
# GOMEMLIMIT giúp Go Runtime dọn rác khẩn cấp khi đạt 250MB
ENV GOMEMLIMIT=250MiB
# GOGC=50 giúp dọn rác tích cực hơn (đánh đổi chút xíu CPU để lấy Memory)
ENV GOGC=50

CMD ["./bifrost-proxy"]
