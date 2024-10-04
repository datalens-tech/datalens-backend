import pytest

from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig


def test_object_like_config():
    data = {
        "some_key": 1,
        "another_key": ["value1", "value2", "value3"],
        "complex_key": {
            "key1": "key1_value",
            "key2": "key2_value",
        },
    }
    config = ObjectLikeConfig.from_dict(data)
    assert config.get("some_key") == 1
    assert config["some_key"] == 1
    assert config.some_key == 1
    assert hasattr(config, "some_key")
    assert config["another_key"] == ["value1", "value2", "value3"]
    assert config["complex_key"]["key1"] == "key1_value"
    assert config.complex_key.key2 == "key2_value"

    with pytest.raises(AttributeError) as exc_info:
        assert config.complex_key.key3
    assert str(exc_info.value) == 'There is no record in config by path: "complex_key.key3"'
