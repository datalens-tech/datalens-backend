import attr

from bi_query_processing.compilation.primitives import CompiledMultiQueryBase


@attr.s
class MultiQueryMutatorBase:
    """
    Applies mutations at multi-query level.
    """

    def mutate_multi_query(self, multi_query: CompiledMultiQueryBase) -> CompiledMultiQueryBase:
        raise NotImplementedError
