import logging
from typing import (
    ClassVar,
    Optional,
    Sequence,
)

import attr

from dl_query_processing.compilation.primitives import CompiledMultiQueryBase
from dl_query_processing.multi_query.mutators.base import MultiQueryMutatorBase
from dl_query_processing.multi_query.splitters.base import MultiQuerySplitterBase
from dl_query_processing.multi_query.tools import (
    CompiledMultiQueryPatch,
    apply_query_patch,
    build_requirement_subtree,
)
from dl_query_processing.utils.name_gen import PrefixedIdGen

LOGGER = logging.getLogger(__name__)


@attr.s
class SplitterMultiQueryMutator(MultiQueryMutatorBase):
    """
    Mutator base on splitter objects that are applied to each flat query
    that is part of the multi-query.
    """

    _splitters: Sequence[MultiQuerySplitterBase] = attr.ib(kw_only=True)
    _do_log: bool = attr.ib(kw_only=True, default=True)

    _max_iterations: ClassVar[int] = 1000

    def log(self, msg: str, level: int = logging.INFO) -> None:
        if self._do_log:
            LOGGER.log(level=level, msg=msg)

    def mutate_multi_query(self, multi_query: CompiledMultiQueryBase) -> CompiledMultiQueryBase:
        query_id_gen = PrefixedIdGen("q")
        expr_id_gen = PrefixedIdGen("e")
        for splitter in self._splitters:
            skip_queries: set[str] = set()

            iteration_cnt = 0
            while True:
                # Prevent infinite loop
                if iteration_cnt >= self._max_iterations:
                    raise RuntimeError("Maximum multi-query iterations")

                # Iterate over the whole multi-query object each time
                # because we don't know, how its structure has been changed
                multi_query_patch: Optional[CompiledMultiQueryPatch] = None
                for query in multi_query.iter_queries():
                    # Skip queries that have already been handled
                    if query.id in skip_queries:
                        continue

                    requirement_subtree = build_requirement_subtree(multi_query, query.id)
                    multi_query_patch = splitter.split_query(
                        query=query,
                        requirement_subtree=requirement_subtree,
                        query_id_gen=query_id_gen,
                        expr_id_gen=expr_id_gen,
                    )
                    skip_queries.add(query.id)
                    if multi_query_patch is not None:
                        break

                if multi_query_patch is not None:
                    # We have a patch. This means that the multi-query has to be mutated
                    multi_query = apply_query_patch(multi_query=multi_query, patch=multi_query_patch)
                    iteration_cnt += 1
                else:
                    # No patch. This means that there is nothing left to mutate in all of the multi-query.
                    # Leave.
                    break

            if iteration_cnt > 0:
                self.log(
                    f"Applied mutation patches in {iteration_cnt} iterations "
                    f"for splitter {splitter.__class__.__name__}"
                )

        return multi_query
