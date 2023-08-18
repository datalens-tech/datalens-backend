#!/bin/sh -x

set -Eeu

cd /data
make translations-local
# docker-related hack:
# (TODO: run as outer-uid user inside docker)
chown --reference . --recursive . || true
