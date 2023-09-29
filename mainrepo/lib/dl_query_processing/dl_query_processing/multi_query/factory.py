import abc

import attr

from dl_core.fields import ResultSchema
from dl_query_processing.enums import ExecutionLevel
from dl_query_processing.multi_query.mutators.base import MultiQueryMutatorBase
from dl_query_processing.multi_query.mutators.splitter_based import SplitterMultiQueryMutator
from dl_query_processing.multi_query.mutators.winfunc_compeng import DefaultCompengMultiQueryMutator
from dl_query_processing.multi_query.splitters.prefiltered import PrefilteredFieldMultiQuerySplitter
from dl_query_processing.multi_query.splitters.query_fork import QueryForkQuerySplitter


@attr.s
class MultiQueryMutatorFactoryBase(abc.ABC):
    result_schema: ResultSchema = attr.ib(kw_only=True)

    @abc.abstractmethod
    def get_mutators(self) -> list[MultiQueryMutatorBase]:
        raise NotImplementedError


@attr.s
class DefaultMultiQueryMutatorFactory(MultiQueryMutatorFactoryBase):
    def get_mutators(self) -> list[MultiQueryMutatorBase]:
        return [
            SplitterMultiQueryMutator(
                splitters=[
                    QueryForkQuerySplitter(),
                ]
            ),
            DefaultCompengMultiQueryMutator(),
        ]


@attr.s
class NoCompengMultiQueryMutatorFactory(MultiQueryMutatorFactoryBase):
    def get_mutators(self) -> list[MultiQueryMutatorBase]:
        return [
            SplitterMultiQueryMutator(
                splitters=[
                    QueryForkQuerySplitter(),
                ]
            ),
        ]


@attr.s
class SimpleFieldSplitterMultiQueryMutatorFactory(MultiQueryMutatorFactoryBase):
    def get_mutators(self) -> list[MultiQueryMutatorBase]:
        return [
            SplitterMultiQueryMutator(
                splitters=[
                    PrefilteredFieldMultiQuerySplitter(crop_to_level_type=ExecutionLevel.compeng),
                    QueryForkQuerySplitter(),
                ]
            ),
            DefaultCompengMultiQueryMutator(),
        ]
