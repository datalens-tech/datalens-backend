#!/bin/sh

CFGFILE="/etc/yandex/statbox-push-client/push-client.yaml"
if [ ! -e "$CFGFILE" ]; then CFGFILE="/etc/push-client.yaml"; fi

# See also:
# https://a.yandex-team.ru/arc/trunk/arcadia/market/sre/juggler/bundles/rtc/checks/push-client/push-client-status.sh?rev=4211457#L11
res="$(
  date -Ins
  push-client \
    -c "$CFGFILE" \
    --status --json \
    --check:send-time=600 --check:commit-time=600 --check:lag-size=100000000 \
    2>&1
)"
status="$?"
if [ $status -ne 0 ]; then
    status_name='CRIT'
else
    status_name='OK'
fi

# Minimal string quoting for json
jquote(){ printf '%s' "$1" | sed -z 's/\\/\\\\/g; s/\n/\\n/g; s/"/\\"/g; s/^/"/; s/$/"/'; }

env_name="some_bi_env"
if [ -n "${YENV_TYPE:-}" ]; then
    env_name="$YENV_TYPE"
fi

component_name="some_bi_component"
if [ -n "${BI_COMPONENT:-}" ]; then
    component_name="$BI_COMPONENT"
elif [ -n "${QLOUD_COMPONENT:-}" ]; then
    component_name="$QLOUD_COMPONENT"
elif [ -n "${DEPLOY_BOX_ID:-}" ]; then
    component_name="$DEPLOY_BOX_ID"
fi

instance_path="some_bi_instance_path"
if [ -n "${BI_INSTANCE_PATH:-}" ]; then
    instance_path="$BI_INSTANCE_PATH"
elif [ -n "${QLOUD_DISCOVERY_INSTANCE:-}" ]; then
    instance_path="$QLOUD_DISCOVERY_INSTANCE"
elif [ -n "${DEPLOY_BOX_ID}" ]; then
    instance_path="${DEPLOY_PROJECT_ID:-}.${DEPLOY_STAGE_ID:-}.${DEPLOY_BOX_ID:-}"
else
    instance_path="$(hostname -f)"
fi


printf '{
    "events": [
        {
            "service": "push_client.service",
            "description": %s,
            "tags": ["datalens", %s, %s, %s],
            "status": %s
        }
    ]
}
' \
    "$(jquote "$res")" \
    "$(jquote "$env_name")" \
    "$(jquote "$component_name")" \
    "$(jquote "$instance_path")" \
    "$(jquote "$status_name")"
