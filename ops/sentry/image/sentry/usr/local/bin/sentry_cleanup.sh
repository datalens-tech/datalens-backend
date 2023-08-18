#!/usr/bin/env bash
set -o errexit


# Get cleanup days from 1st argument and check if it is not integer
CLEANUP_DAYS="$1"
[ -z "$CLEANUP_DAYS" ] && echo "Usage: /usr/local/bin/sentry_cleanup <days>" && exit 0
case "${CLEANUP_DAYS}" in
    ''|*[!0-9]*) echo "Cleanup days must be integer" && exit 1 ;;
esac


# Set defaults
DEFAULT_ZKCONFIG="/etc/zkflock.json"
DEFAULT_ZKLOCK="sentrycleanup_${QLOUD_PROJECT}_${QLOUD_APPLICATION}_${QLOUD_ENVIRONMENT}"
DEFAULT_ZKAPPID="SENTRY"

# Merge with runtime opts
ZKCONFIG="${SENTRY_ZKFLOCK_CONFIG:-$DEFAULT_ZKCONFIG}"
ZKLOCK="${SENTRY_ZKFLOCK_NAME:-$DEFAULT_ZKLOCK}"
ZKAPPID="${SENTRY_ZKFLOCK_APPID:-$DEFAULT_ZKAPPID}"

CLEANUP_CMD="/usr/local/bin/sentry cleanup --days ${CLEANUP_DAYS}"
ZK_CMD="/usr/local/bin/zk-flock -c ${ZKCONFIG} ${ZKLOCK}"


function compile_zkflock_config {
    local src="/etc/zkflock.template"
    local dst="${1}"
    local servers=${2%,}
    local appid="${3}"
    servers="${servers//,/\",\"}"
    servers=$servers appid=$appid envsubst "\$servers \$appid" < ${src} > "${dst}"
}


if [ -n "${SENTRY_ZKFLOCK_SERVERS}" ]; then
    compile_zkflock_config "${ZKCONFIG}" "${SENTRY_ZKFLOCK_SERVERS}" "${ZKAPPID}"
fi

if [ -f "${ZKCONFIG}" ]; then
    ${ZK_CMD} "${CLEANUP_CMD}"
else
    echo "No zk config for cleanup (${ZKCONFIG}), skipping"
fi
