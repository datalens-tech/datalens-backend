from __future__ import annotations

from collections import OrderedDict
from http import HTTPStatus
import json
from typing import (
    Any,
    Callable,
)
from typing import OrderedDict as OrderedDictTyping
from typing import Sequence

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    OrderedField,
    ResultField,
)
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_client.dsmaker.shortcuts.tree import make_request_legend_items_for_tree_branches
from dl_api_lib_testing.helpers.data_source import data_source_settings_from_table
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    BIType,
    FieldRole,
)
from dl_core_testing.database import (
    C,
    Db,
    make_table,
)


TREE_DATA = [
    # / Company
    {"id": 10, "dept": ["Company"], "salary": 0, "is_person": 0},
    # / Company / R&D
    {"id": 11, "dept": ["Company", "R&D"], "salary": 0, "is_person": 0},
    # / Company / R&D / Cloud
    {"id": 12, "dept": ["Company", "R&D", "Cloud"], "salary": 0, "is_person": 0},
    {"id": 13, "dept": ["Company", "R&D", "Cloud", "Dan"], "salary": 100, "is_person": 1},
    # / Company / R&D / Cloud / Rain
    {"id": 14, "dept": ["Company", "R&D", "Cloud", "Rain"], "salary": 0, "is_person": 0},
    {"id": 15, "dept": ["Company", "R&D", "Cloud", "Rain", "Jessica"], "salary": 100, "is_person": 1},
    {"id": 16, "dept": ["Company", "R&D", "Cloud", "Rain", "Claude"], "salary": 100, "is_person": 1},
    # / Company / R&D / Cloud / Snow
    {"id": 17, "dept": ["Company", "R&D", "Cloud", "Snow"], "salary": 0, "is_person": 0},
    {"id": 18, "dept": ["Company", "R&D", "Cloud", "Snow", "Ivan"], "salary": 100, "is_person": 1},
    {"id": 19, "dept": ["Company", "R&D", "Cloud", "Snow", "Ivonne"], "salary": 100, "is_person": 1},
    {"id": 20, "dept": ["Company", "R&D", "Cloud", "Snow", "Angela"], "salary": 100, "is_person": 1},
    # / Company / R&D / Cloud / Lightning
    {"id": 21, "dept": ["Company", "R&D", "Cloud", "Lightning"], "salary": 0, "is_person": 0},
    {"id": 22, "dept": ["Company", "R&D", "Cloud", "Lightning", "Gary"], "salary": 100, "is_person": 1},
    # / Company / R&D / Search
    {"id": 23, "dept": ["Company", "R&D", "Search"], "salary": 0, "is_person": 0},
    {"id": 24, "dept": ["Company", "R&D", "Search", "Agnes"], "salary": 100, "is_person": 1},
    # / Company / Sales
    {"id": 25, "dept": ["Company", "Sales"], "salary": 0, "is_person": 0},
    {"id": 26, "dept": ["Company", "Sales", "Alice"], "salary": 10000, "is_person": 1},
    # / Company / Sales / CN
    {"id": 27, "dept": ["Company", "Sales", "CN"], "salary": 0, "is_person": 0},
    # / Company / Sales / EU
    {"id": 28, "dept": ["Company", "Sales", "EU"], "salary": 0, "is_person": 0},
    {"id": 29, "dept": ["Company", "Sales", "EU", "Noname"], "salary": 50, "is_person": 1},
    # / Company / Sales / US
    {"id": 30, "dept": ["Company", "Sales", "US"], "salary": 0, "is_person": 0},
]


def make_tree_dataset(db: Db, connection_id: str, api_v1: SyncHttpDatasetApiV1) -> Dataset:
    columns = [
        C("id", BIType.integer, vg=lambda rn, **kwargs: TREE_DATA[rn]["id"]),
        C("dept", BIType.array_str, vg=lambda rn, **kwargs: TREE_DATA[rn]["dept"]),
        C("salary", BIType.integer, vg=lambda rn, **kwargs: TREE_DATA[rn]["salary"]),
        C("is_person", BIType.integer, vg=lambda rn, **kwargs: TREE_DATA[rn]["is_person"]),
    ]
    db_table = make_table(db, columns=columns, rows=len(TREE_DATA))
    ds = Dataset()
    ds.sources["source_1"] = ds.source(connection_id=connection_id, **data_source_settings_from_table(db_table))
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.result_schema["dept_tree"] = ds.field(formula="TREE([dept])")
    ds.result_schema["person_count"] = ds.field(formula="COUNT_IF(BOOL([is_person]))")
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset
    return ds


def get_tree(
    branches: list[tuple[int, list[str]]],
    dataset: Dataset,
    data_api: SyncHttpDataApiV2,
    order_by: Sequence[ResultField | OrderedField] = (),
    measures: Sequence[tuple[str, Callable]] = (("person_count", int),),
) -> OrderedDictTyping[tuple[str, ...], tuple[Any, ...]]:
    branch_items = make_request_legend_items_for_tree_branches(
        branches=branches,
        dataset=dataset,
        tree_title="dept_tree",
    )
    result_resp = data_api.get_result(
        dataset=dataset,
        fields=[
            *branch_items,
            *[dataset.find_field(title=name) for name, cast_func in measures],
        ],
        order_by=list(order_by),
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    result = OrderedDict(
        (tuple(json.loads(row[0])), tuple([cast_func(row[idx + 1]) for idx, (name, cast_func) in enumerate(measures)]))
        for row in data_rows
    )

    block_metas = [block_meta for block_meta in result_resp.json["blocks"]]
    assert len(block_metas) == len(branch_items)
    for block_meta in block_metas:
        assert block_meta["query"] is not None

    return result


class TestResultWithTrees(DefaultApiTestBase):
    def test_simple_tree(self, dataset_api, data_api, db, saved_connection_id):
        connection_id = saved_connection_id
        ds = make_tree_dataset(db=db, connection_id=connection_id, api_v1=dataset_api)

        tree = get_tree(
            [
                (1, []),
            ],
            dataset=ds,
            data_api=data_api,
        )
        assert tree == {
            ("Company",): (10,),
        }

        tree = get_tree(
            [
                (1, []),
                (2, ["Company"]),
            ],
            dataset=ds,
            data_api=data_api,
        )
        assert tree == {
            ("Company",): (10,),
            (
                "Company",
                "R&D",
            ): (8,),
            (
                "Company",
                "Sales",
            ): (2,),
        }

        tree = get_tree(
            [
                (1, []),
                (2, ["Company"]),
                (3, ["Company", "R&D"]),
            ],
            dataset=ds,
            data_api=data_api,
        )
        assert tree == {
            ("Company",): (10,),
            (
                "Company",
                "R&D",
            ): (8,),
            ("Company", "R&D", "Cloud"): (7,),
            ("Company", "R&D", "Search"): (1,),
            (
                "Company",
                "Sales",
            ): (2,),
        }

        tree = get_tree(
            [
                (1, []),
                (2, ["Company"]),
                (3, ["Company", "R&D"]),
                (4, ["Company", "R&D", "Cloud"]),
            ],
            dataset=ds,
            data_api=data_api,
        )
        assert tree == {
            ("Company",): (10,),
            (
                "Company",
                "R&D",
            ): (8,),
            ("Company", "R&D", "Cloud"): (7,),
            ("Company", "R&D", "Cloud", "Dan"): (1,),
            ("Company", "R&D", "Cloud", "Rain"): (2,),
            ("Company", "R&D", "Cloud", "Snow"): (3,),
            ("Company", "R&D", "Cloud", "Lightning"): (1,),
            ("Company", "R&D", "Search"): (1,),
            (
                "Company",
                "Sales",
            ): (2,),
        }

    def test_simple_tree_with_order_by(self, dataset_api, data_api, db, saved_connection_id):
        connection_id = saved_connection_id
        ds = make_tree_dataset(db=db, connection_id=connection_id, api_v1=dataset_api)

        # Order by measure
        tree = get_tree(
            dataset=ds,
            data_api=data_api,
            branches=[(1, []), (2, ["Company"]), (3, ["Company", "R&D"])],
            order_by=[
                ds.find_field(title="person_count"),
            ],
        )
        assert tree == OrderedDict(
            (
                (("Company",), (10,)),
                (
                    (
                        "Company",
                        "Sales",
                    ),
                    (2,),
                ),
                (
                    (
                        "Company",
                        "R&D",
                    ),
                    (8,),
                ),
                (("Company", "R&D", "Search"), (1,)),
                (("Company", "R&D", "Cloud"), (7,)),
            )
        )

        # Order by branch
        tree = get_tree(
            dataset=ds,
            data_api=data_api,
            branches=[(1, []), (2, ["Company"]), (3, ["Company", "R&D"])],
            order_by=[
                ds.find_field(title="dept_tree"),
            ],
        )
        assert tree == OrderedDict(
            (
                (("Company",), (10,)),
                (
                    (
                        "Company",
                        "R&D",
                    ),
                    (8,),
                ),
                (("Company", "R&D", "Cloud"), (7,)),
                (("Company", "R&D", "Search"), (1,)),
                (
                    (
                        "Company",
                        "Sales",
                    ),
                    (2,),
                ),
            )
        )

    def test_simple_tree_with_order_by_and_rsum(
        self,
        dataset_api,
        data_api,
        db,
        saved_connection_id,
    ):
        connection_id = saved_connection_id
        ds = make_tree_dataset(db=db, connection_id=connection_id, api_v1=dataset_api)
        ds.result_schema["rsum_measure"] = ds.field(formula="RSUM([person_count])")

        # Order by measure
        tree = get_tree(
            dataset=ds,
            data_api=data_api,
            branches=[(1, []), (2, ["Company"]), (3, ["Company", "R&D"])],
            order_by=[
                ds.find_field(title="person_count"),
            ],
            measures=[
                ("person_count", int),
                ("rsum_measure", int),
            ],
        )
        assert tree == OrderedDict(
            (
                (("Company",), (10, 10)),
                (
                    (
                        "Company",
                        "Sales",
                    ),
                    (2, 2),
                ),
                (
                    (
                        "Company",
                        "R&D",
                    ),
                    (8, 10),
                ),
                (("Company", "R&D", "Search"), (1, 1)),
                (("Company", "R&D", "Cloud"), (7, 8)),
            )
        )

        # Order by tree path
        tree = get_tree(
            dataset=ds,
            data_api=data_api,
            branches=[(1, []), (2, ["Company"]), (3, ["Company", "R&D"])],
            order_by=[
                ds.find_field(title="dept_tree"),
            ],
            measures=[
                ("person_count", int),
                ("rsum_measure", int),
            ],
        )
        assert tree == OrderedDict(
            (
                (("Company",), (10, 10)),
                (
                    (
                        "Company",
                        "R&D",
                    ),
                    (8, 8),
                ),
                (("Company", "R&D", "Cloud"), (7, 7)),
                (("Company", "R&D", "Search"), (1, 8)),
                (
                    (
                        "Company",
                        "Sales",
                    ),
                    (2, 10),
                ),
            )
        )

    def test_tree_and_totals_incompatibility_error(self, dataset_api, data_api, db, saved_connection_id):
        connection_id = saved_connection_id
        ds = make_tree_dataset(db=db, connection_id=connection_id, api_v1=dataset_api)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="dept_tree").as_req_legend_item(
                    role=FieldRole.tree,
                    tree_level=1,
                    tree_prefix=[],
                    dimension_values={},
                ),
                ds.find_field(title="person_count").as_req_legend_item(role=FieldRole.total),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST
        assert result_resp.bi_status_code == "ERR.DS_API.BLOCK.ITEM_COMPATIBILITY"

    def test_multiple_trees_error(self, dataset_api, data_api, db, saved_connection_id):
        connection_id = saved_connection_id
        ds = make_tree_dataset(db=db, connection_id=connection_id, api_v1=dataset_api)

        ds.result_schema["dept_tree_2"] = ds.field(formula="TREE([dept])")

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="dept_tree").as_req_legend_item(
                    role=FieldRole.tree,
                    tree_level=1,
                    tree_prefix=[],
                    dimension_values={},
                ),
                ds.find_field(title="dept_tree_2").as_req_legend_item(
                    role=FieldRole.tree,
                    tree_level=1,
                    tree_prefix=[],
                    dimension_values={},
                ),
                ds.find_field(title="person_count"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST
        assert result_resp.bi_status_code == "ERR.DS_API.TREE.MULTIPLE"

    def test_invalid_data_type_for_tree_error(self, dataset_api, data_api, db, saved_connection_id):
        connection_id = saved_connection_id
        ds = make_tree_dataset(db=db, connection_id=connection_id, api_v1=dataset_api)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="id").as_req_legend_item(
                    role=FieldRole.tree,
                    tree_level=1,
                    tree_prefix=[],
                    dimension_values={},
                ),
                ds.find_field(title="person_count"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST
        assert result_resp.bi_status_code == "ERR.DS_API.LEGEND.ROLE_DATA_TYPE_MISMATCH"

    def test_invalid_legend_item_reference_error(self, dataset_api, data_api, db, saved_connection_id):
        connection_id = saved_connection_id
        ds = make_tree_dataset(db=db, connection_id=connection_id, api_v1=dataset_api)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="dept_tree").as_req_legend_item(
                    role=FieldRole.tree,
                    tree_level=1,
                    tree_prefix=[],
                    dimension_values={5: "8"},
                ),
                ds.find_field(title="person_count"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST
        assert result_resp.bi_status_code == "ERR.DS_API.LEGEND.ITEM_REFERENCE"
