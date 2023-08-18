#!/bin/sh -x

set -eu

# ======= internal postgresql =======
# reference: https://github.com/docker-library/postgres/blob/master/Dockerfile-debian.template
# notable: explicitly add postgres uid/gid; gosu; en_us.UTF-8; disable create-main-cluster; export PYTHONDONTWRITEBYTECODE=1;
# reference: https://github.com/docker-library/postgres/blob/master/docker-entrypoint.sh
# notable: mkdir && chown && chmod;  initdb --username=... --pwfile=...; temp_server start/stop;

# Other useful stuff:
#
#     create tablespace tmpdata location '/tmp/data';
#     drop table if exists t1;
#     create temp table t1 (c1 text, c2 integer) tablespace tmpdata;
#     insert into t1 (c1, c2) values ('abc', 123), ('zxc', 456);
#     insert into t1 (c2) select generate_series(1, 5000000) as c2;
#     select count(1) from t1;
#
#     import psycopg2
#     conn = psycopg2.connect('user=postgres dbname=postgres')
#     cur = conn.cursor()
#     cur.execute('select 1 - 2')
#     print(cur.fetchall())


for i in $(seq 1 10)
do
        apt-get update -y && break || echo "." && sleep 1
done

for y in $(seq 1 10)
do
        apt-get install -y --no-install-recommends postgresql-common postgresql-12 postgresql-contrib-12 -y && break || echo "." && sleep 1
done

apt-get clean && rm -rf /var/lib/apt/lists/*

cp /etc/postgresql/12/main/postgresql.conf /etc/postgresql/12/main/postgresql.conf.orig
cat > /etc/postgresql/12/main/postgresql.conf <<\_EOF
data_directory = '/var/lib/postgresql/12/main'
hba_file = '/etc/postgresql/12/main/pg_hba.conf'
ident_file = '/etc/postgresql/12/main/pg_ident.conf'
external_pid_file = '/var/run/postgresql/12-main.pid'
port = 5432
max_connections = 100
unix_socket_directories = '/var/run/postgresql'
ssl = off
ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'
shared_buffers = 128MB
dynamic_shared_memory_type = posix
max_wal_size = 1GB
min_wal_size = 80MB
log_line_prefix = '%m [%p] %q%u@%d '
log_timezone = 'Etc/UTC'
cluster_name = '12/main'
stats_temp_directory = '/var/run/postgresql/12-main.pg_stat_tmp'
datestyle = 'iso, mdy'
timezone = 'Etc/UTC'
lc_messages = 'C.UTF-8'
lc_monetary = 'C.UTF-8'
lc_numeric = 'C.UTF-8'
lc_time = 'C.UTF-8'
default_text_search_config = 'pg_catalog.english'
include_dir = 'conf.d'
_EOF

cp /etc/postgresql/12/main/pg_hba.conf /etc/postgresql/12/main/pg_hba.conf.orig
cat > /etc/postgresql/12/main/pg_hba.conf <<\_EOF
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
host    replication     all             127.0.0.1/32            md5
host    replication     all             ::1/128                 md5
local   all             postgres                                peer    map=anyauth
_EOF

cp /etc/postgresql/12/main/pg_ident.conf /etc/postgresql/12/main/pg_ident.conf.orig
cat > /etc/postgresql/12/main/pg_ident.conf <<\_EOF
anyauth    root    postgres
anyauth    www-data    postgres
_EOF

mkdir -p /tmp/pgdata
chown postgres:postgres /tmp/pgdata
