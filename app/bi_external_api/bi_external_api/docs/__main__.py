import json
import os
import subprocess
from typing import Optional

import click

from bi_external_api.docs import dc_grpc_doc_render
from bi_external_api.docs.main_dc import DoubleCloudDocsBuilder


def get_dc_public_api_root() -> str:
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../../../../..",
            "cloud/doublecloud/public-api",
        )
    )


def generate_visualization_configs_docs(target_dir: str) -> None:
    builder = DoubleCloudDocsBuilder()
    docs = builder.build()
    os.makedirs(target_dir, exist_ok=True)
    docs.render(target_dir, locale="en")


def get_settings(settings_path: Optional[str] = None) -> dict[str, str]:
    effective_settings_path: str = (
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../temp/doc_gen_settings.json"))
        if settings_path is None
        else settings_path
    )

    if not os.path.exists(effective_settings_path):
        raise ValueError(f"Settings file not exists: {effective_settings_path}")

    with open(effective_settings_path) as f:
        return json.load(f)


def generate_proto_doc_json():
    settings = get_settings()
    googleapis_repo_path = settings["google_apis_repo_path"]

    result = subprocess.run(
        [os.path.join(get_dc_public_api_root(), "generate_docs_json.bash")],
        env={**os.environ, "GOOGLE_APIS_REPO_PATH": googleapis_repo_path},
        cwd=get_dc_public_api_root(),
    )
    if result.returncode != 0:
        raise ValueError("Proto spec docs JSON generated with errors")


@click.group()
def cli():
    pass


@cli.command()
@click.argument("docs_path")
def dc_docs_visualization_configs(docs_path: str):
    generate_visualization_configs_docs(target_dir=docs_path)


@cli.command()
@click.argument("docs_path")
@click.option("--proto-doc-json-path", default=None)
@click.option("--gen-vis-conf-docs", is_flag=True, default=False)
def dc_docs_grpc(
    docs_path: str,
    proto_doc_json_path: Optional[str],
    gen_vis_conf_docs: bool,
):
    if not os.path.exists(docs_path):
        raise ValueError(f"{docs_path} not exists!")
    if not os.path.isdir(docs_path):
        raise ValueError(f"{docs_path} is not a directory!")

    effective_proto_doc_json_path: str

    if proto_doc_json_path is not None:
        print("Proto doc JSON path is configured. Generation of new one will be skipped!")
        effective_proto_doc_json_path = proto_doc_json_path
    else:
        # Default out path of https://a.yandex-team.ru/arc_vcs/cloud/doublecloud/public-api/generate_docs_json.bash
        effective_proto_doc_json_path = os.path.abspath(
            os.path.join(
                get_dc_public_api_root(),
                "out/dc_api_docs.json",
            )
        )
        generate_proto_doc_json()

    if not os.path.exists(effective_proto_doc_json_path):
        raise ValueError(
            "JSON with propo-spec docs not exists." f" May be not generated? Path: {effective_proto_doc_json_path}"
        )

    with open(effective_proto_doc_json_path, "r") as proto_doc_json_file:
        proto_doc_json = json.load(proto_doc_json_file)

    dc_grpc_doc_render.main(
        proto_doc_json=proto_doc_json,
        render_target=docs_path,
    )

    if gen_vis_conf_docs:
        vis_conf_docs_dir = os.path.join(docs_path, "visualization/configs")
        os.makedirs(vis_conf_docs_dir, exist_ok=True)
        generate_visualization_configs_docs(vis_conf_docs_dir)


if __name__ == "__main__":
    cli()
