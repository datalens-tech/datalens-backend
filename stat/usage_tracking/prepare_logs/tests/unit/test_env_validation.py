from argparse import Namespace

from datalens.backend.stat.usage_tracking.prepare_logs.__main__ import validate_args
import pytest


@pytest.mark.parametrize("time_scale", ["1d, 30min, stream/5min"])
@pytest.mark.parametrize("app_type", ["int", "ext"])
@pytest.mark.parametrize("env", ["production", "testing"])
def test_validation_ok(time_scale, app_type, env):
    args = Namespace(
        src=[f"//logs/datalens-back-{app_type}-{env}-fast/{time_scale}/2021-12-27T12:30:00"],
        dst=["..."],
        time_scale=time_scale,
        app_type=app_type,
        env=env,
    )
    validate_args(args)


@pytest.mark.parametrize(
    "case_name, args",
    [
        (
            "FAIL_structure_1",
            Namespace(
                src=["//logs/datalens-back-int-production-fast/2021-12-27T12:30:00"],
                dst=["..."],
                time_scale="30min",
                app_type="int",
                env="production",
            ),
        ),
        (
            "FAIL_structure_2",
            Namespace(
                src=["//datalens-back-int-production-fast/30min/2021-12-27T12:30:00"],
                dst=["..."],
                time_scale="30min",
                app_type="int",
                env="production",
            ),
        ),
        (
            "FAIL_tags_time",
            Namespace(
                src=["//logs/datalens-back-int-production-fast/30min/2021-12-27T12:30:00"],
                dst=["..."],
                time_scale="1d",
                app_type="int",
                env="production",
            ),
        ),
        (
            "FAIL_tags_type",
            Namespace(
                src=["//logs/datalens-back-ext-production-fast/30min/2021-12-27T12:30:00"],
                dst=["..."],
                time_scale="30min",
                app_type="int",
                env="production",
            ),
        ),
        (
            "FAIL_tags_env",
            Namespace(
                src=["//logs/datalens-back-int-production-fast/30min/2021-12-27T12:30:00"],
                dst=["..."],
                time_scale="30min",
                app_type="int",
                env="testing",
            ),
        ),
        (
            "FAIL_tags_fast",
            Namespace(
                src=["//logs/datalens-back-int-production-debug/30min/2021-12-27T12:30:00"],
                dst=["..."],
                time_scale="30min",
                app_type="int",
                env="testing",
            ),
        ),
        (
            "FAIL_src_dist_len",
            Namespace(
                src=["//logs/datalens-back-int-production-fast/30min/2021-12-27T12:30:00"],
                dst=["path_1", "path_2"],
                time_scale="30min",
                app_type="int",
                env="production",
            ),
        ),
    ],
)
def test_validation_assertion_error(case_name, args):
    with pytest.raises(ValueError):
        validate_args(args)
