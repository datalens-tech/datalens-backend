#!/bin/sh
FIELDS="request_id, pid, ts, timestampns, msg, app_instance, app_name, app_version, funcName, hostname, levelname, lineno, name, MESSAGE"
echo "
    create or replace view ext_prod.all_bi_logs
    as
    select $FIELDS from ext_prod.bi_logs
    union all
    select $FIELDS from int_prod.bi_logs
    union all
    select $FIELDS from ext_testing.bi_logs
    union all
    select $FIELDS from int_testing.bi_logs
"
