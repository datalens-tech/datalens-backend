# syntax=docker/dockerfile:1

FROM golang AS builder

WORKDIR /app

COPY --from=sources . .
COPY config.yaml config.yaml

RUN make rogue_mysql_server

FROM alpine:latest

WORKDIR /app

COPY --from=builder ./app/rogue_mysql_server ./rogue_mysql_server
COPY --from=builder ./app/config.yaml ./config.yaml

EXPOSE 3306

CMD ["/app/rogue_mysql_server", "-config", "config.yaml"]
