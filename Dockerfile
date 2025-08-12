FROM alpine:latest

COPY build/clickhouse-timeline-linux-amd64 /usr/bin/clickhouse-timeline

ENTRYPOINT ["/usr/bin/clickhouse-timeline"]
