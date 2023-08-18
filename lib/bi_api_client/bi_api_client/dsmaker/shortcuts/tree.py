from __future__ import annotations

import json

from bi_constants.enums import FieldRole

from bi_api_client.dsmaker.primitives import Dataset, RequestLegendItem


def make_request_legend_items_for_tree_branches(
        tree_title: str,
        branches: list[tuple[int, list[str]]],
        dataset: Dataset,
) -> list[RequestLegendItem]:
    """
    Generate request legend items for tree branches specified like this:

    branches=[
        (1, []),
        (2, ['Company']),
        (3, ['Company', 'R&D']),
    ]
    """

    dept_tree = dataset.find_field(title=tree_title)
    branch_items = []
    for idx, (level, prefix) in enumerate(branches):
        dimension_values: dict[int, str]
        if level <= 1:
            dimension_values = {}
        else:
            # The only dimension is the tree itself,
            # so add the prefix to the condition
            assert idx > 0
            dimension_values = {idx - 1: json.dumps(prefix)}

        branch_items.append(
            dept_tree.as_req_legend_item(
                role=FieldRole.tree,
                tree_level=level,
                tree_prefix=prefix,
                dimension_values=dimension_values,
                legend_item_id=idx,
                block_id=idx,
            )
        )

    return branch_items
