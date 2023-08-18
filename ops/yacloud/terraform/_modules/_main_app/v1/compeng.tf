resource "kubernetes_config_map" "compeng_config" {
  metadata {
    name      = "compeng-config"
    namespace = var.k8s_namespace
  }
  data = {
    "postgresql.conf" = <<EOF
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
EOF
    "pg_hba.conf" = <<EOF
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
local   all             postgres                                peer    map=anyauth
EOF
    "pg_ident.conf" = <<EOF
anyauth    root    postgres
anyauth    www-data    postgres
anyauth    postgres    postgres
EOF
  }
}

resource "kubernetes_config_map" "compeng_init" {
  metadata {
    name      = "compeng-init"
    namespace = var.k8s_namespace
  }
  data = {
    "compeng_init.sh" = <<EOF
#!/bin/bash
mkdir -p /var/lib/postgresql/12/main
mkdir -p /var/run/postgresql/12-main.pg_stat_tmp
chmod 00700 /var/lib/postgresql/12/main

initdb -D /var/lib/postgresql/12/main

exec \
    postgres \
    --config-file=/etc/postgresql/12/main/postgresql.conf
EOF
  }
}
