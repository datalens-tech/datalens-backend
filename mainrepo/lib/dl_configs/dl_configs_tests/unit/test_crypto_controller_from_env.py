from __future__ import annotations

import json

from cryptography import fernet

from dl_configs.crypto_keys import get_crypto_keys_config_from_env


def test_minimal():
    env = dict(FERNET_KEY=fernet.Fernet.generate_key().decode("ascii"))
    cc = get_crypto_keys_config_from_env(env)
    assert cc.map_id_key == {"0": env["FERNET_KEY"]}
    assert cc.actual_key_id == "0"


def test_single_zero_id_key():
    env = dict(DL_CRY_KEY_VAL_ID_0=fernet.Fernet.generate_key().decode("ascii"))
    cc = get_crypto_keys_config_from_env(env)
    assert cc.map_id_key == {"0": env["DL_CRY_KEY_VAL_ID_0"]}
    assert cc.actual_key_id == "0"


def test_multiple_keys():
    env = dict(
        DL_CRY_KEY_VAL_ID_0=fernet.Fernet.generate_key().decode("ascii"),
        DL_CRY_KEY_VAL_ID_1=fernet.Fernet.generate_key().decode("ascii"),
        DL_CRY_ACTUAL_KEY_ID="1",
    )
    cc = get_crypto_keys_config_from_env(env)
    assert cc.map_id_key == {
        "0": env["DL_CRY_KEY_VAL_ID_0"],
        "1": env["DL_CRY_KEY_VAL_ID_1"],
    }
    assert cc.actual_key_id == "1"


def test_multiple_keys_complex_names():
    env = dict(
        DL_CRY_KEY_VAL_ID_0=fernet.Fernet.generate_key().decode("ascii"),
        DL_CRY_KEY_VAL_ID_CL_PRD_1=fernet.Fernet.generate_key().decode("ascii"),
        DL_CRY_ACTUAL_KEY_ID="CL_PRD_1",
    )
    cc = get_crypto_keys_config_from_env(env)
    assert cc.map_id_key == {
        "0": env["DL_CRY_KEY_VAL_ID_0"],
        "CL_PRD_1": env["DL_CRY_KEY_VAL_ID_CL_PRD_1"],
    }
    assert cc.actual_key_id == "CL_PRD_1"


def test_json():
    actual_key_id = "some_k_id_0"
    cry_dict = dict(
        keys={
            actual_key_id: fernet.Fernet.generate_key().decode("ascii"),
            "outdated_0": fernet.Fernet.generate_key().decode("ascii"),
        },
        actual_key_id=actual_key_id,
    )
    env = dict(
        DL_CRY_JSON_VALUE=json.dumps(cry_dict),
    )
    cc = get_crypto_keys_config_from_env(env)
    assert cc.map_id_key == cry_dict["keys"]
    assert cc.actual_key_id == actual_key_id
