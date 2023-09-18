cat /common-data/sample.csv | clickhouse-client --query="INSERT INTO test_data.sample FORMAT CSV"
