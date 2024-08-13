import logging
import os
import time

from aiohttp import web
import boto3

from dl_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from dl_core.logging_config import configure_logging
from dl_file_uploader_api import app_version
from dl_file_uploader_api.app_factory import StandaloneFileUploaderApiAppFactory
from dl_file_uploader_api.app_settings import FileUploaderAPISettingsOS


LOGGER = logging.getLogger(__name__)


async def create_gunicorn_app() -> web.Application:
    settings = load_settings_from_env_with_fallback(FileUploaderAPISettingsOS)
    configure_logging(app_name="dl_file_uploader_api")
    try:
        LOGGER.info("Creating application instance...")
        app_factory = StandaloneFileUploaderApiAppFactory(settings=settings)
        app = app_factory.create_app(app_version)
        ensure_buckets(settings)
        LOGGER.info("Application instance was created")
        return app
    except Exception:
        LOGGER.exception("Exception during app creation")
        raise


def ensure_buckets(settings: FileUploaderAPISettingsOS):
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url=settings.S3.ENDPOINT_URL,
        aws_access_key_id=settings.S3.ACCESS_KEY_ID,
        aws_secret_access_key=settings.S3.SECRET_ACCESS_KEY,
    )
    for bucket_name in [settings.S3_TMP_BUCKET_NAME, settings.S3_PERSISTENT_BUCKET_NAME]:
        bucket = s3.Bucket(bucket_name)

        if bucket.creation_date is None:
            try:
                s3.create_bucket(Bucket=bucket_name)
                LOGGER.info(f"Bucket {bucket_name} was created")
            except Exception:
                LOGGER.info(f"Error while creating bucket {bucket_name}")


def main() -> None:
    current_app = create_gunicorn_app()
    web.run_app(
        current_app,
        host=os.environ["APP_HOST"],
        port=int(os.environ["APP_PORT"]),
    )


if __name__ == "__main__":
    main()
