SELECT
    reqs.message,
    bodies.message,
    Yson::LookupString(fields, 'request_id') as request_id,
    Yson::LookupString(fields, 'request_path') as req_path,
    rest,
    qloud_instance,
    qloud_environment,
    version,
    timestamp,
    host,
    stackTrace,
    iso_eventtime,
    level,
    fields,
    threadName,
    loggerName,
    qloud_component,
    levelStr
FROM hahn.[logs/qloud-runtime-log/1d/2018-10-02\[(datalens, dls)\]] AS reqs
LEFT JOIN (
    select
        message,
        Yson::LookupString(fields, 'request_id') as request_id
    from hahn.[logs/qloud-runtime-log/1d/2018-10-02\[(datalens, dls)\]]
    where message like 'DLS request body: %'
) AS bodies
on Yson::LookupString(reqs.fields, 'request_id') = bodies.request_id
WHERE qloud_project = 'datalens' AND qloud_application = "dls"
AND reqs.message = 'Handling request'
AND Yson::LookupString(fields, 'request_path') != "/_dls/debug/"
ORDER BY timestamp
;
