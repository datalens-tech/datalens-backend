import argparse
from collections import defaultdict
import json
import logging
import os

import requests
from requests.adapters import (
    HTTPAdapter,
    Retry,
)

logging.basicConfig(level=logging.INFO)


DLS_URL = "_dls/nodes/all/{node_id}/permissions"
US_URL = "private/entriesByKeyPattern?keyPattern=%"

DLS_HEADERS = {
    "X-User-Id": "system_user:root",
    "X-DL-Allow-Superuser": "1",
    "X-API-Key": os.environ["DLS_API_KEY"],
}

ALLOWED_SCOPES = {"widget", "dash", "dataset", "folder", "connection"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Patch DLS permissions using user map")
    parser.add_argument("--user-map", help="file with user map")
    parser.add_argument("--nodes-source", help="source for nodes, either a tenant or a local file")
    parser.add_argument("--comment", help="comment for permission request (i.e. a ticket number)")
    parser.add_argument("--remove-old-permissions", action="store_true", help="if set, old permissions will be removed")
    parser.add_argument("--dry-run", action="store_true", help="if set, won't apply diffs to DLS")
    parser.add_argument("--verbose", action="store_true", help="print skipped nodes")

    return parser.parse_args()


def raise_for_status(r: requests.Response) -> None:
    if r.status_code != 200:
        logging.error(f"Error code: {r.status_code}, content: {r.content}")
        r.raise_for_status()


def get_nodes_from_tenant(tenant_id: str) -> list[str]:
    us_host = os.environ["US_HOST"]
    token = os.environ["US_MASTER_TOKEN"]
    r = requests.get(
        f"{us_host}/{US_URL}", headers={"x-us-master-token": token, "x-dl-tenantid": tenant_id}, verify=False
    )
    raise_for_status(r)
    return [entry["entryId"] for entry in r.json() if entry["scope"] in ALLOWED_SCOPES]


def patch_node(
    session: requests.Session,
    node_id: str,
    user_map: dict[str, str],
    comment: str,
    remove_old: bool,
    dry_run: bool,
    verbose: bool,
) -> bool:
    if verbose:
        logging.info(f"Getting all permissions for the node {node_id}")
    dls_host = os.environ["DLS_HOST"]
    dls_url = DLS_URL.format(node_id=node_id)
    r = session.get(f"{dls_host}/{dls_url}", headers=DLS_HEADERS, verify=False)
    raise_for_status(r)
    node = r.json()

    if not node["editable"]:
        logging.warning(f"Skipping node {node_id} because it's not editable")
        return False

    diff = {
        "added": defaultdict(list),
        "removed": defaultdict(list),
    }
    for kind, subjects in node["permissions"].items():
        for subject in subjects:
            old_user = subject["name"]
            if (new_user := user_map.get(old_user)) is not None:
                diff["added"][kind].append({"subject": new_user, "comment": comment})
                if remove_old:
                    diff["removed"][kind].append({"subject": old_user, "comment": comment})

    if diff["added"] or diff["removed"]:
        logging.info(f"Patching the node {node_id}")
        logging.info(json.dumps(diff, indent=4))
        if not dry_run:
            r = session.patch(f"{dls_host}/{dls_url}", headers=DLS_HEADERS, json={"diff": diff}, verify=False)
            raise_for_status(r)
        return True
    elif verbose:
        logging.info(f"No patching is needed for the node {node_id}, skipping")
    return False


def add_prefix(user: str) -> str:
    if not user.startswith("user:"):
        return "user:" + user
    return user


def main():
    args = parse_args()
    nodes: list[str]
    try:
        with open(args.nodes_source) as fin:
            nodes = [line.strip() for line in fin]
    except FileNotFoundError:
        # assume it's a tenant name, get nodes using the US
        logging.info(f"Getting nodes from the tenant {args.nodes_source}")
        nodes = get_nodes_from_tenant(args.nodes_source)
    logging.info(f"Found {len(nodes)} nodes")

    user_map = {}
    with open(args.user_map) as fin:
        for line in fin:
            old_user, new_user = map(add_prefix, line.strip().split())
            user_map[old_user] = new_user

    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=list(range(500, 600)))
    s.mount("https://", HTTPAdapter(max_retries=retries))

    patched_count, skipped_count, error_count = 0, 0, 0
    for node in nodes:
        try:
            patched = patch_node(
                s, node, user_map, args.comment, args.remove_old_permissions, args.dry_run, args.verbose
            )
            patched_count += int(patched)
            skipped_count += int(not patched)
        except Exception as e:
            logging.error(f"Error while patching the node {node}: {str(e)}")
            error_count += 1
    logging.info(
        f"Finished patching. {patched_count} nodes patched, {skipped_count} skipped, "
        f"{error_count} errors encountered"
    )


if __name__ == "__main__":
    main()
