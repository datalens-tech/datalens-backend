#!/bin/bash

# set -Eeuo pipefail

# Either provided by phusion-baseimage,
# or can be created in the entry point by `declare -px > /etc/container_environment.sh`
. /etc/container_environment.sh

start_ts="$(date -Ins)"
res="$(
/bin/sh /etc/cron.daily/logrotate 2>&1
)"
status="$?"
end_ts="$(date -Ins)"
pfx="PASSIVE-CHECK:logrotate_hourly.service;"
if [ $status -ne 0 ]; then
    out="${pfx}CRIT;${start_ts} .. ${end_ts} ${status} ${res}"
else
    out="${pfx}OK;${end_ts} ${res}"
fi
printf "%s\n" "$out"
printf "%s\n%s\n%s\n" "$start_ts" "$out" "$end_ts" > /tmp/_logrotate_hourly_last.log
