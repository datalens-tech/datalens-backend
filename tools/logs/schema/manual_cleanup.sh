#!/bin/sh

set -eu

work(){
    env="$1"
    days="$2"
    cutoff="$(date +%Y%m%d -d "${days} days ago")"
    _dl_ch_logs_manage "
        select distinct partition_id
        from system.parts
        where database = '${env}' and table = 'bi_logs' and partition_id < '${cutoff}'
    " \
    | while read -r name; do
        query="alter table ${env}.bi_logs drop partition '${name}'"
        echo "$query"
        _dl_ch_logs_manage "$query"
    done
}


work int_testing 6
work ext_testing 6
work int_prod 6
work ext_prod 12
