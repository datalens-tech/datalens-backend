#!/bin/sh

_DEFAULT_INSTANCE_COUNT=2
_DEFAULT_INSTANCE_CPU=16
_SHELL_NAME=$(basename $SHELL)

case ${_SHELL_NAME} in
    "zsh")
        _RED="${fg[red]}"
        _GREEN="${fg[green]}"
        _BYELLOW="${fg[yellow,bold]}"
        _NC="${reset_color}"
    ;;
    *)
        _RED='\x1B[0;31m'
        _GREEN='\x1B[0;32m'
        _BYELLOW='\x1B[1;33m'
        _NC='\x1B[0m'
    ;;
esac

configure_and_start_clique() {
    if [ -z "${pool:-}" ]; then
        echo -e "${_RED}ERROR: pool is required${_NC}"
        return
    fi

    if [ -z "${alias:-}" ]; then
        echo -e "${_RED}ERROR: alias is required${_NC}"
        return
    fi

    if [ -z "${instance_count:-}" ]; then
        echo -e "${_GREEN}Instance count is not specified, using default (${_DEFAULT_INSTANCE_COUNT})${_NC}"
        _effective_instance_count=$_DEFAULT_INSTANCE_COUNT
    else
        _effective_instance_count=$instance_count
    fi

    if [ -z "${instance_cpu:-}" ]; then
        echo -e "${_GREEN}Instance CPU is not specified, using default (${_DEFAULT_INSTANCE_CPU})${_NC}"
        _effective_instance_cpu=$_DEFAULT_INSTANCE_CPU
    else
        _effective_instance_cpu=$instance_cpu
    fi

    _clique_exists=`yt clickhouse ctl exists "${alias}" "$@"`
    if [ "${_clique_exists}" != "%true" ]; then
        echo -e "${_GREEN}Clique ${alias} does not exist, going to create it with given options${_NC}"
        yt clickhouse ctl create "${alias}" "$@"
    else
        echo -e "${_GREEN}Clique ${alias} already exists, applying options${_NC}"
    fi

    yt clickhouse ctl set-option --alias "${alias}" pool ${pool} "$@"
    yt clickhouse ctl set-option --alias "${alias}" instance_count "${_effective_instance_count}" "$@"
    yt clickhouse ctl set-option --alias "${alias}" instance_cpu "${_effective_instance_cpu}" "$@"
    yt clickhouse ctl set-option --alias "${alias}" restart_on_speclet_change %true "$@"
    yt clickhouse ctl set-option --alias "${alias}" clickhouse_config '{
        engine={timezone="UTC"};
        yt={settings={composite={
            enable_conversion=1;
            default_yson_format="pretty";
        }}};
    }' "$@"

    yt clickhouse ctl set-option --alias "${alias}" active %true "$@"

    if [ "${_clique_exists}" != "%true" ]; then
        echo -e "${_GREEN}Clique ${alias} should start shortly${_NC}"
    else
        echo -e "${_GREEN}Clique ${alias} should restart shortly if specs were changed, otherwise it can be restarted manually${_NC}"
    fi

    _clique_status=$(yt clickhouse ctl status "${alias}" "$@")
    echo -e "${_BYELLOW}${alias} status:\n${_clique_status} ${_NC}"
}

set -x

alias=chyt_datalens_back \
pool=datalens-tests \
instance_count=2 \
instance_cpu=8 \
    configure_and_start_clique --proxy=hahn

alias=chyt_datalens_back_no_robot \
pool=datalens-tests \
instance_count=2 \
instance_cpu=8 \
    configure_and_start_clique --proxy=hahn


alias=chyt_datalens_analytics \
pool=datalens-analytics-chyt \
instance_count=2 \
instance_cpu=5 \
    configure_and_start_clique --proxy=hahn

set +x
