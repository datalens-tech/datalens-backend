#!/usr/bin/env python3

from __future__ import annotations

import asyncio
import logging

import sh

from bi_cloud_integration.model import IAMResource
from bi_cloud_integration.yc_as_client import DLASClient


REQUEST_ID = "testscript_fake_request"
FOLDER_ID = "aoevv1b69su5144mlro3"  # datalens-dev back


async def amain(iam_token, request_id=REQUEST_ID, folder_id=FOLDER_ID):
    as_cli = DLASClient.create(
        endpoint="as.private-api.cloud-preprod.yandex.net:4286",
    )
    try:
        user = await as_cli.authenticate(
            iam_token=iam_token,
            request_id=request_id,
        )
    except Exception as exc:
        logging.error("authentication error: %s", repr(exc).replace("\n", "   "))
        return
    try:
        yc_folder_resource = IAMResource.folder(folder_id)
        await as_cli.authorize(
            iam_token=iam_token,
            permission="datalens.instances.use",
            resource_path=[yc_folder_resource],
            request_id=request_id,
        )
    except Exception as exc:
        logging.error("authorization error: %s", repr(exc).replace("\n", "   "))
        return
    logging.info("user=%r", user)


def smain(iam_token, request_id=REQUEST_ID, folder_id=FOLDER_ID):
    as_cli = DLASClient.create(
        endpoint="as.private-api.cloud-preprod.yandex.net:4286",
    )
    try:
        user = as_cli.authenticate_sync(
            iam_token=iam_token,
            request_id=request_id,
        )
    except Exception as exc:
        logging.error("authentication error: %s", repr(exc).replace("\n", "   "))
        return
    try:
        yc_folder_resource = IAMResource.folder(folder_id)
        as_cli.authorize_sync(
            iam_token=iam_token,
            permission="datalens.instances.use",
            resource_path=[yc_folder_resource],
            request_id=request_id,
        )
    except Exception as exc:
        logging.error("authorization error: %s", repr(exc).replace("\n", "   "))
        return
    logging.info("user=%r", user)


def main():
    logging.basicConfig(level=1)
    logging.getLogger("sh").setLevel(logging.INFO)

    iam_token = sh.sh("-c", "yc iam create-token --profile=dlb-preprod").stdout.decode("utf-8").strip()
    logging.info("iam_token=%r", iam_token[:8] + "…" + iam_token[-8:])

    smain(iam_token=iam_token)

    asyncio.run(amain(iam_token=iam_token))


if __name__ == "__main__":
    main()
