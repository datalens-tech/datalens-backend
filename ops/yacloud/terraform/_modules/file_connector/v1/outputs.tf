output "persistent_bucket_name" {
  value = yandex_storage_bucket.persistent.bucket
}

output "tmp_bucket_name" {
  value = yandex_storage_bucket.tmp.bucket
}

output "ch_hosts" {
  value = yandex_mdb_clickhouse_cluster.this[*].host[0].fqdn
}

output "ch_user" {
  value = "dl_file_conn" # do we really need to change it?
}

output "sa_key_secret_id" {
  value = yandex_lockbox_secret.file_conn_sa_key.id
}

output "ch_user_secret_id" {
  value = yandex_lockbox_secret.ch_user.id
}
