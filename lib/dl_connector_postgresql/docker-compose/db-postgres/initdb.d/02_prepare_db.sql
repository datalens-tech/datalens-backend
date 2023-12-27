CREATE SCHEMA IF NOT EXISTS test_data_partitions;
DROP TABLE IF EXISTS test_data_partitions.sample_partition;
create table test_data_partitions.sample_partition (
    id int not NULL,
    md5 TEXT not NULL
) PARTITION BY Range (id);

CREATE TABLE test_data_partitions.sample_partition_pt_1 PARTITION OF test_data_partitions.sample_partition
    FOR VALUES FROM (0) TO (50);

CREATE TABLE test_data_partitions.sample_partition_pt_2 PARTITION OF test_data_partitions.sample_partition
    FOR VALUES FROM (50) TO (101);


insert into test_data_partitions.sample_partition (
    select id, md5(random()::text) from generate_Series(1, 100) as id
);
