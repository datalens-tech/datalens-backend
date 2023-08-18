cat /common-data/sample-lite.csv | clickhouse-client --query="INSERT INTO test_data.SampleLite FORMAT CSV"
cat /common-data/sample.csv | clickhouse-client --query="INSERT INTO test_data.sample_superstore FORMAT CSV"
cat /common-data/sample-hashed-name.csv | clickhouse-client --query="INSERT INTO test_data.sample_superstore_hashed_customer_name FORMAT CSV"
