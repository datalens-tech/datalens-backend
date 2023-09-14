"""
Here we test old autonomous loaders of sub-configs (which actually use new declarative framework).
In future all loaders which are tested here must be removed.
"""
from bi_configs.rqe import (
    RQEConfig,
    rqe_config_from_env,
)


def test_rqe_config():
    hmac_key = "sfgasdfdamjowerre"

    actual_config = rqe_config_from_env(
        {
            # Passing legacy env key to also check env remap
            "EXT_QUERY_EXECUTER_SECRET_KEY": hmac_key
        }
    )

    assert actual_config == RQEConfig.get_default().clone(hmac_key=hmac_key.encode())
