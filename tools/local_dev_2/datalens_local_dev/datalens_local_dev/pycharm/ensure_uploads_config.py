from __future__ import annotations

from os import path
import sys

from datalens_local_dev.constants import (
    PKG_ROOT_PATH,
    SUBMODULE_DEFAULT_EXCLUDES,
)
from datalens_local_dev.get_submodules import rel_path_list_for_pycham_upload
from datalens_local_dev.idea import find_idea_dir
from lxml import etree as ET


def main():
    deployment_fp = path.join(find_idea_dir(), "deployment.xml")
    parser = ET.XMLParser(remove_blank_text=True)
    tree = ET.parse(deployment_fp, parser)
    server_data_node = tree.find("./component/serverData/paths/serverdata")
    if server_data_node is None:
        print("Uploading to dev machine was not initially configured. Please make some dummy mapping with GUI.")
        sys.exit(-1)

    # Cleanup
    for child in list(server_data_node):
        server_data_node.remove(child)

    submodules = rel_path_list_for_pycham_upload()

    # Mappings
    mappings_node = ET.SubElement(server_data_node, "mappings")
    for submodule in submodules:
        ET.SubElement(
            mappings_node,
            "mapping",
            dict(
                deploy=path.join(PKG_ROOT_PATH, submodule),
                local=path.join("$PROJECT_DIR$", submodule),
                web="/",
            ),
        )

    # Excludes
    excluded_path_node = ET.SubElement(server_data_node, "excludedPaths")
    for submodule in submodules:
        for default_exclude in SUBMODULE_DEFAULT_EXCLUDES:
            ET.SubElement(
                excluded_path_node,
                "excludedPath",
                dict(
                    local="true",
                    path=path.join("$PROJECT_DIR$", submodule, default_exclude),
                ),
            )
            # TODO FIX: add egg info

    tree.write(deployment_fp, pretty_print=True, xml_declaration=True, encoding="utf-8")
    print("[Pycharm] Ensure uploads config done")


if __name__ == "__main__":
    main()
