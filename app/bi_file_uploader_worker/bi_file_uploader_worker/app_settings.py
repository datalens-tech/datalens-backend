import attr

from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.utils import app_type_env_var_converter
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings


@attr.s(frozen=True)
class DefaultFileUploaderWorkerSettings(FileUploaderWorkerSettings):
    APP_TYPE: AppType = s_attrib(  # type: ignore
        "YENV_TYPE",
        is_app_cfg_type=True,
        env_var_converter=app_type_env_var_converter,
    )
