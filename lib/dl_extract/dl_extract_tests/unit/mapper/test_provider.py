import dl_extract


def test_primitive_mapper_returns_expected_config() -> None:
    settings = dl_extract.PrimitiveClusterExtractClickhouseProviderSettings(
        HOSTS=["host-1", "host-2"],
        PORT=9440,
        DATABASE="extracts",
        USERNAME="user",
        PASSWORD="pass",
    )
    mapper = dl_extract.StaticExtractClickhouseProvider.from_settings(settings=settings)

    config = mapper.get_clickhouse_config(dl_extract.ExtractClickhouseProviderArguments(dataset_id="dataset-x"))

    assert config.hosts == ["host-1", "host-2"]
    assert config.port == 9440
    assert config.database == "extracts"
    assert config.table == "extract_dataset-x"
    assert config.username == "user"
    assert config.password == "pass"
