FROM alpine:latest

ARG TARGETARCH

COPY build/clickhouse-timeline-linux-${TARGETARCH} /usr/bin/clickhouse-timeline

ENTRYPOINT ["/usr/bin/clickhouse-timeline"]
