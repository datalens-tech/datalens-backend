## Requesting the APIs from development ##

Install dependencies:

    pip install -i https://pypi.yandex-team.ru/simple/ -e '.[all]'

Grab the secrets from YAV:

    make secrets-update

Try it out:

    ./_envit ./.env_int_beta ./_curl.sh 'debug/'

Reminder: generally, the relevant APIs are the same as requested from the browser,
e.g. `nodes/all/9gh...c/permissions`.

Also useful:

    ./_envit ./.env_int_beta ./djmanage dbshell

    ./_envit ./.env_int_beta ./djmanage shell_plus


## Qloud log sample searches ##

### Long requests ###

https://qloud-ext.yandex-team.ru/projects/datalens/dls/testing?tab=logs-beta&range=2018-10-02T12%3A00%3A00-2018-10-02T20%3A00%3A00&query=%23%20%40fields.request_path%3A%22%2F_dls%2Fbatch_accesses%22%20AND%20%40fields.msg%3A%22Finished%20request%22%20AND%20%20%40fields.duration%20%3A%20%5B0.4%20TO%20*%5D&filter=%3D%3Dqloud_component%2Cbackend&tz=-180d

### reqid like ###

https://qloud-ext.yandex-team.ru/projects/datalens/dls/testing?tab=logs-beta&range=2018-10-02T17%3A00%3A00-2018-10-02T20%3A00%3A00&query=%23WHERE%20(Json%3A%3AGetField(fields%2C%20%27reqid%27)%7B0%7D%20LIKE%20%27%2525f257%25%27)&filter=%3D%3Dqloud_component%2Cbackend&tz=-180

### YQL replayable request select ###

See `./_aux/replay_shooter/README.md`


## Other notes ##

### Node grant logs ###

    select n.identifier as node_identifier, n.meta as node_meta, n.ctime as node_ctime, n.realm as node_tenant, n.scope as node_scope, g.perm_kind as grant_type, g.state as grant_state, g.meta as grant_meta, g.ctime as grant_ctime, g.mtime as grant_mtime, g.realm as grant_tenant, gs.name as grant_user_name, gs.active as grant_user_active, gs.source as grant_user_source, gs.realm as grant_user_tenant, gs.mtime as grant_user_mtime, gs.meta as grant_user_meta, l.id as log_id, l.kind as log_kind, l.request_user_id as log_request_user_id, l.meta as log_meta, l.ctime as log_timestamp from dls_nodes n left join dls_node_config nc on n.id = nc.node_id left join dls_grant g on nc.id = g.node_config_id left join dls_log l on g.guid = l.grant_guid left join dls_subject gs on g.subject_id = gs.id where n.identifier = '3r06xsd6b0fo2';


## Initial configuration checklist ##

  * environment name, `envname=...`
  * `./yadls/settings.py` env-specific configuration
  * convenience: `./.env_${envname}` shell file with secrets, for sourcing
    * Add it to `.gitignore` and to secfiles.
  * database setup
  * `(cd ./yadls && ../_envit ../.env_${envname} alembic upgrade head)`
  * run hosted instance (e.g. qloud) with `DLS_ENV=...` and secrets.
