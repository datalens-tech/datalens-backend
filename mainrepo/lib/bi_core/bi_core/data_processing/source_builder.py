from __future__ import annotations

from collections import defaultdict
from itertools import chain
import logging
from typing import (
    Any,
    ClassVar,
    Collection,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from bi_constants.enums import JoinType
from bi_core import exc
from bi_core.components.ids import AvatarId
from bi_core.data_processing.prepared_components.primitives import (
    PreparedMultiFromInfo,
    PreparedSingleFromInfo,
)
from bi_core.query.bi_query import SqlSourceType
from bi_core.query.expression import JoinOnExpressionCtx

LOGGER = logging.getLogger(__name__)

JoinExpressionType = Union[ClauseElement, str]

_MAX_AVATAR_REUSAGE_CNT = 10


class AvatarJoinInfo(NamedTuple):
    avatar_id: AvatarId
    join_type: JoinType
    on_clause: JoinExpressionType


@attr.s
class SqlSourceBuilder:
    """
    :param avatar_alias_mapper: a string-to-string function that should be used for converting avatar aliases
    :param prep_component_manager: an ``PreparedComponentManagerBase`` instance to provide access to data sources
        via avatar IDs. These may be real, straight from the dataset, or may be virtual, such as nested queries
    """

    # Constants
    _COMMON_JOIN_PARAMS: ClassVar[Dict[str, bool]] = dict(all=False, any=False, global_=False)
    _JOIN_PARAMS_BY_TYPE: ClassVar[Dict[JoinType, Dict[str, Any]]] = {
        JoinType.inner: {"type": "inner"},
        JoinType.left: {"isouter": True, "type": "left"},
        JoinType.right: {"type": "right"},
        JoinType.full: {"isouter": True, "full": True, "type": "full"},
    }

    def validate_prep_src_infos(self, required_prep_src_infos: Collection[PreparedSingleFromInfo]) -> None:
        if len({prep_src_info.target_connection_ref for prep_src_info in required_prep_src_infos}) != 1:
            raise ValueError("There is a divergence in data source connections")

    def make_join_clause(
        self,
        left: SqlSourceType,
        right: SqlSourceType,
        join_type: JoinType,
        on_clause: JoinExpressionType,
    ) -> SqlSourceType:
        join_params = dict(self._COMMON_JOIN_PARAMS, **self._JOIN_PARAMS_BY_TYPE[join_type])
        result = sa.join(left, right, on_clause)  # type: ignore
        for name, value in join_params.items():
            setattr(result, name, value)
        return result

    def collect_avatar_join_info_list(
        self,
        root_avatar_id: AvatarId,
        required_avatar_ids: Collection[AvatarId],
        join_on_expressions: Collection[JoinOnExpressionCtx] = (),
    ) -> Tuple[Set[AvatarId], List[AvatarJoinInfo]]:
        avatar_join_info_list: List[AvatarJoinInfo] = []
        used_avatar_ids: Set[AvatarId] = {root_avatar_id}
        used_avatar_ids_as_list: List[AvatarId] = []

        relations_by_left: Dict[Optional[AvatarId], List[JoinOnExpressionCtx]] = defaultdict(list)
        for rel in join_on_expressions:
            relations_by_left[rel.left_id].append(rel)

        def _recursive_collect(avatar_id: Optional[AvatarId], ambiguous_relations: bool) -> None:
            avatar_user_relations = [
                rel
                for rel in relations_by_left[avatar_id]
                if ((rel.left_id is not None or ambiguous_relations) and rel.right_id in required_avatar_ids)
            ]
            for prep_relation in avatar_user_relations:
                child_avatar_id = prep_relation.right_id
                if used_avatar_ids_as_list.count(child_avatar_id) > _MAX_AVATAR_REUSAGE_CNT:
                    raise exc.DatasetConfigurationError("Avatar re-usage limit has been reached")
                used_avatar_ids.add(child_avatar_id)
                used_avatar_ids_as_list.append(child_avatar_id)

                relation_expr_ctx = prep_relation.expression
                relation_expr: JoinExpressionType = relation_expr_ctx.expression
                LOGGER.info(f"Including relation between avatars {avatar_id} and {child_avatar_id}")
                avatar_join_info_list.append(
                    AvatarJoinInfo(
                        avatar_id=child_avatar_id, join_type=prep_relation.join_type, on_clause=relation_expr
                    )
                )
                if not ambiguous_relations:
                    # feature-managed relations are not recursive
                    _recursive_collect(avatar_id=child_avatar_id, ambiguous_relations=ambiguous_relations)

        # feature-managed relations can have references to any of the other tables
        # in their formula expressions, so they must be joined to last
        # (note that they all have root as their left avatar)
        # so first collect user joins, and then feature joins
        _recursive_collect(root_avatar_id, ambiguous_relations=False)
        # feature-managed relations are all formally bound to root and have no real left ID (are ambiguous)
        _recursive_collect(None, ambiguous_relations=True)

        return used_avatar_ids, avatar_join_info_list

    def build_from_avatar_join_info(
        self,
        *,
        root_avatar_id: AvatarId,
        avatar_join_info_list: List[AvatarJoinInfo],
        prepared_sources: Collection[PreparedSingleFromInfo],
    ) -> SqlSourceType:
        prep_sources_as_dict: Dict[AvatarId, PreparedSingleFromInfo] = {psi.id: psi for psi in prepared_sources}

        root_prep_src_info = prep_sources_as_dict[root_avatar_id]
        source = root_prep_src_info.non_null_sql_source
        for avatar_join_info in avatar_join_info_list:
            avatar_id = avatar_join_info.avatar_id
            prep_src_info = prep_sources_as_dict[avatar_id]
            join_type = avatar_join_info.join_type
            if not prep_src_info.supports_join_type(join_type):
                raise exc.DatasetConfigurationError(f"Data source does not support {join_type.name.upper()} JOIN")
            right = prep_src_info.non_null_sql_source
            source = self.make_join_clause(
                left=source,
                right=right,
                join_type=join_type,
                on_clause=avatar_join_info.on_clause,
            )

        return source

    def build_source(
        self,
        *,
        root_avatar_id: AvatarId,
        prepared_sources: Collection[PreparedSingleFromInfo],
        join_on_expressions: Collection[JoinOnExpressionCtx] = (),
        use_empty_source: bool = False,
    ) -> PreparedMultiFromInfo:
        """
        Compile source for the FROM-clause of the future SELECT statements.
        Validate joint info of all data sources for role in dataset
         (e.g. dialect, database name, query compiler)

        :param root_avatar_id: ID of the avatar that is the root of the required avatar tree
        :param prepared_sources: SQL sources to be joined
        :param join_on_expressions: The relations to be used for joining
        :param use_empty_source: force usage of an empty sql source
        """

        required_avatar_ids = {psi.id for psi in prepared_sources}
        self.validate_prep_src_infos(prepared_sources)

        prepared_sources = sorted(prepared_sources, key=lambda prep_src_info: prep_src_info.id)
        assert prepared_sources
        first_prep_source = prepared_sources[0]
        data_source_list = tuple(
            chain.from_iterable((prep_src_info.data_source_list or ()) for prep_src_info in prepared_sources)
        )
        pass_db_query_to_user = all(prep_src_info.pass_db_query_to_user for prep_src_info in prepared_sources)

        used_avatar_ids, avatar_join_info_list = self.collect_avatar_join_info_list(
            root_avatar_id=root_avatar_id,
            required_avatar_ids=required_avatar_ids,
            join_on_expressions=join_on_expressions,
        )
        if used_avatar_ids != required_avatar_ids:
            LOGGER.warning(
                f"Used avatar IDs are different from required: required: {required_avatar_ids}, "
                f"used: {used_avatar_ids}"
            )

        joint_source: Optional[SqlSourceType] = None
        if not use_empty_source:
            joint_source = self.build_from_avatar_join_info(
                root_avatar_id=root_avatar_id,
                avatar_join_info_list=avatar_join_info_list,
                prepared_sources=prepared_sources,
            )

        joint_dsrc_info = PreparedMultiFromInfo(
            sql_source=joint_source,
            data_source_list=data_source_list,
            query_compiler=first_prep_source.query_compiler,
            db_name=first_prep_source.db_name,
            connect_args=first_prep_source.connect_args,
            supported_join_types=frozenset(JoinType),  # TODO: determine this honestly
            pass_db_query_to_user=pass_db_query_to_user,
            target_connection_ref=first_prep_source.target_connection_ref,
        )
        return joint_dsrc_info
