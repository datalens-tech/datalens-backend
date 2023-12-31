from contextlib import contextmanager

from sqlalchemy import exc
import sqlalchemy.orm.query as query_module
import sqlalchemy.orm.context as context_module
from sqlalchemy.orm.query import Query as BaseQuery
from sqlalchemy.orm.util import _ORMJoin as _StandardORMJoin

from ..ext.clauses import (
    sample_clause,
    ArrayJoin,
)


class Query(BaseQuery):
    _with_totals = False
    _final = None
    _sample = None
    _array_join = None

    def _compile_context(self, *args, **kwargs):

        @contextmanager
        def replace_join():
            original = context_module._ORMJoin
            try:
                context_module._ORMJoin = _ORMJoin
                yield
            finally:
                context_module._ORMJoin = original

        with replace_join():
            context = super(Query, self)._compile_context(*args, **kwargs)
        statement = context.compile_state.statement

        statement._with_totals = self._with_totals
        statement._final_clause = self._final
        statement._sample_clause = sample_clause(self._sample)
        statement._array_join = self._array_join

        return context

    def with_totals(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_totals() can be used only with specified "
                "GROUP BY, call group_by()"
            )

        self._with_totals = True

        return self

    def array_join(self, *columns):
        self._array_join = ArrayJoin(*columns)
        return self

    def final(self):
        self._final = True

        return self

    def sample(self, sample):
        self._sample = sample

        return self

    def join(self, *props, **kwargs):
        type = kwargs.pop('type', None)
        strictness = kwargs.pop('strictness', None)
        distribution = kwargs.pop('distribution', None)
        rv = super(Query, self).join(*props, **kwargs)
        joineds = list(set(rv._from_obj) - set(self._from_obj))
        if not joineds:
            # Bad hack for sa1.4:
            rv._legacy_setup_joins[-1][-1].update(
                full=(rv._legacy_setup_joins[-1][-1]["full"], type, strictness, distribution),
            )
            return rv
        new = _ORMJoin._from_standard(joineds[0],
                                      type=type,
                                      strictness=strictness,
                                      distribution=distribution)

        @contextmanager
        def replace_join():
            original = query_module.orm_join
            try:
                query_module.orm_join = new
                yield
            finally:
                query_module.orm_join = original

        with replace_join():
            return super(Query, self).join(*props, **kwargs)

    def outerjoin(self, *props, **kwargs):
        kwargs['type'] = kwargs.get('type') or 'LEFT OUTER'
        return self.join(*props, **kwargs)


class _ORMJoin(_StandardORMJoin):
    @classmethod
    def _from_standard(cls, standard_join, type, strictness, distribution):
        return cls(
            standard_join.left,
            standard_join.right,
            standard_join.onclause,
            type=type,
            strictness=strictness,
            distribution=distribution
        )

    def __init__(
            self, left, right, onclause=None,
            isouter=False, full=False,
            type=None, strictness=None, distribution=None,
            **kwargs,
    ):
        super(_ORMJoin, self).__init__(left, right, onclause)

        # Bad hack for sa1.4:
        if isinstance(full, tuple):
            assert type is None
            assert strictness is None
            assert distribution is None
            full, type, strictness, distribution = full

        self.distribution = distribution
        self.strictness = str
        self.type = type
        self.strictness = None
        if strictness:
            self.strictness = strictness
        self.distribution = distribution
        self.type = type

    def __call__(self, *args, **kwargs):
        return self
