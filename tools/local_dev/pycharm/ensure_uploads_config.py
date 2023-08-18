from __future__ import annotations

import sys
from os import path

from lxml import etree as ET

from pycharm.common import IDEA_DIR, get_submodules, PKG_ROOT_PATH, SUBMODULE_DEFAULT_EXCLUDES


def main():
    deployment_fp = path.join(IDEA_DIR, "deployment.xml")
    parser = ET.XMLParser(remove_blank_text=True)
    tree = ET.parse(deployment_fp, parser)
    server_data_node = tree.find("./component/serverData/paths/serverdata")
    if server_data_node is None:
        print("Uploading to dev machine was not initially configured. Please make some dummy mapping with GUI.")
        sys.exit(-1)

    # Cleanup
    for child in list(server_data_node):
        server_data_node.remove(child)

    submodules = get_submodules()

    # Mappings
    mappings_node = ET.SubElement(server_data_node, "mappings")
    for submodule in submodules:
        ET.SubElement(mappings_node, "mapping", dict(
            deploy=path.join(PKG_ROOT_PATH, submodule),
            local=path.join("$PROJECT_DIR$", submodule),
            web="/"
        ))

    # Excludes
    excluded_path_node = ET.SubElement(server_data_node, "excludedPaths")
    for submodule in submodules:
        for default_exclude in SUBMODULE_DEFAULT_EXCLUDES:
            ET.SubElement(excluded_path_node, "excludedPath", dict(
                local="true",
                path=path.join("$PROJECT_DIR$", submodule, default_exclude)
            ))
            # TODO FIX: add egg info

    tree.write(deployment_fp, pretty_print=True, xml_declaration=True, encoding="utf-8")


if __name__ == '__main__':
    main()
