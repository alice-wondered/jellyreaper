FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/jellyreaper ./cmd/jellyreaper

FROM alpine:3.20

RUN adduser -D -u 10001 appuser
WORKDIR /app

RUN mkdir -p /data /logs /embeds /app && chown -R appuser:appuser /data /logs /embeds /app

COPY --from=builder /out/jellyreaper /app/jellyreaper

ENV HTTP_PORT=6767 \
    HTTP_ADDR=:6767 \
    DB_PATH=/data/jellyreaper.db \
    LOG_DIR=/logs \
    EMBED_PERSIST_DIR=/embeds

EXPOSE 6767
USER appuser

ENTRYPOINT ["/app/jellyreaper"]
