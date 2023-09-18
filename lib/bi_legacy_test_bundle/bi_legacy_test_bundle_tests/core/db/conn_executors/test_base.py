from __future__ import annotations

import contextlib
from typing import (
    Generator,
    List,
)

import attr
import pytest

from bi_legacy_test_bundle_tests.core.common_ce import BaseConnExecutorSet
from dl_core.connection_models import DBIdent
from dl_core_testing.database import make_schema


# noinspection PyMethodMayBeStatic
class BaseSchemaSupportedExecutorSet(BaseConnExecutorSet):
    # List tables
    # ###########
    @contextlib.contextmanager
    def _get_table_names_test_case(self, db) -> Generator[BaseConnExecutorSet.ListTableTestCase, None, None]:
        """Override fixture to take in account schemas"""
        # Creating schema
        schema_name = make_schema(db)
        with super()._get_table_names_test_case(db=db, schema_name=schema_name) as test_case:
            test_case = test_case.clone(full_match_required=True)
            yield test_case
        # TODO FIX: Drop created schema

    # List schema
    # ###########
    @attr.s(auto_attribs=True, frozen=True)
    class ListSchemaTestCase:
        target_db_ident: DBIdent
        expected_schemas: List[str]
        # If false - result of get_schema_names must contains all schemas, otherwise - should be totally equals
        full_match_required: bool

    @pytest.fixture()
    def get_schema_names_test_case(self, db) -> "ListSchemaTestCase":
        schema_name_list = [make_schema(db) for _ in range(3)]
        yield self.ListSchemaTestCase(
            target_db_ident=DBIdent(None),
            expected_schemas=schema_name_list,
            full_match_required=False,
        )
        # TODO FIX: Drop created schema

    def test_get_schema_names(self, sync_exec_wrapper, get_schema_names_test_case):
        actual_schema_names = sync_exec_wrapper.get_schema_names(get_schema_names_test_case.target_db_ident)

        if get_schema_names_test_case.full_match_required:
            assert sorted(actual_schema_names) == sorted(get_schema_names_test_case.expected_schemas)
        else:
            assert set(actual_schema_names).issuperset(set(get_schema_names_test_case.expected_schemas))

    @pytest.mark.asyncio
    async def test_get_schema_names_sync(self, executor, get_schema_names_test_case):
        actual_schema_names = await executor.get_schema_names(get_schema_names_test_case.target_db_ident)

        if get_schema_names_test_case.full_match_required:
            assert sorted(actual_schema_names) == sorted(get_schema_names_test_case.expected_schemas)
        else:
            assert set(actual_schema_names).issuperset(set(get_schema_names_test_case.expected_schemas))
