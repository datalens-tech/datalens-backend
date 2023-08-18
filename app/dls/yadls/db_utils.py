from __future__ import annotations

import asyncio
from asyncio import iscoroutine
import contextlib
import functools
import logging
import re
import time
from typing import Optional

import async_timeout

from bi_utils.aio import alist

from .utils import to_text, cut_string
from .exceptions import NotFound, MultipleObjectsReturned, LimitReached


__all__ = (
    'MultipleObjectsReturned',
    'db_get_one',
    'db_insert_one',
    'DatabaseRouting',
)


LOGGER = logging.getLogger(__name__)
LOG = LOGGER


async def db_get_one(rows, not_found_msg=None):
    """
    Similar to django's `queryset.get()`.

    NOTE: Consumes at most 2 rows, so might as well add `limit 2` to the select.

    :returns: row
    :raises: NotFound
    :raises: MultipleObjectsReturned
    """
    result = sentinel = object()
    async for row in rows:
        if result is sentinel:
            result = row
        else:
            raise MultipleObjectsReturned(dict(rows=(result, row)))
    if result is sentinel:
        raise NotFound(not_found_msg)
    return result


async def db_insert_one(conn, stmt, simple_key=True):
    """ Execute one insert statement returning the primary key """
    res = await conn.execute(stmt)
    res = await alist(res)
    assert len(res) == 1
    res = res[0]
    if simple_key:
        assert len(res) == 1
        res = res[0]
    return res


def compile_pg_query_u(stmt, dialect):
    from psycopg2.extensions import adapt as sqlescape
    comp = stmt.compile(dialect=dialect)
    params = comp.params
    sql = comp.string
    params = {key: sqlescape(val) for key, val in params.items()}
    result = sql % params
    return result


def compile_pg_query_b(stmt, dialect):
    from psycopg2.extensions import adapt as sqlescape
    comp = stmt.compile(dialect=dialect)
    params = comp.params
    sql = comp.string
    enc = dialect.encoding
    params = {
        key.encode(enc): val.encode(enc) if isinstance(val, str) else val
        for key, val in params.items()}
    sql = sql.encode(enc)
    params = {key: sqlescape(val) for key, val in params.items()}
    # That's where it gets very dubious:
    params = {
        key: str(val).encode(enc) if not isinstance(val, bytes) else val
        for key, val in params.items()}
    result = sql % params
    result = result.decode(enc)
    return result


compile_pg_query = compile_pg_query_b


class DebugConnectionWrapper:

    _max_repr_len = 4000

    def __init__(self, conn, name):
        self._conn = conn
        self._logger = logging.getLogger('db.{}'.format(name))

    def __getattr__(self, key):
        return getattr(self._conn, key)

    def _postprocess_stmt_repr(self, stmt_repr):
        stmt_repr = stmt_repr.replace('\n', '   ')
        stmt_repr = cut_string(stmt_repr, self._max_repr_len)
        return stmt_repr

    def _log_timing(self, message, timing, stmt_repr, meta='-', stage=-1):
        self._logger.debug(
            'DB %s (%s): %.3fs, meta=%r, %s',
            message, stage, timing, meta, stmt_repr)

    async def execute(self, stmt, *args, **kwargs):
        t_prepare_start = time.time()
        conn = self._conn
        wrapped_func = conn.execute

        stmt_compile = getattr(stmt, 'compile', None)
        if stmt_compile is not None:
            dialect = conn._dialect
            stmt_repr = compile_pg_query(stmt, dialect=dialect)
        else:
            stmt_repr = str(stmt)

        stmt_repr = self._postprocess_stmt_repr(stmt_repr)

        t_prepare_end = time.time()
        self._log_timing(
            message='execute_prepare', timing=t_prepare_end - t_prepare_start,
            stmt_repr=stmt_repr, stage=0)
        t_call_start = time.time()

        prev_echo = conn._connection._echo
        conn._connection._echo = False
        try:
            result = await wrapped_func(stmt, *args, **kwargs)
        finally:
            conn._connection._echo = prev_echo

        t_call_end = time.time()

        result_meta = None
        try:
            result_meta = dict(
                return_rows=getattr(result, 'return_rows', None),
                rowcount=getattr(result, 'rowcount', None),
                columns=len(result.cursor.description),
                statusmessage=result.cursor.statusmessage,
            )
            stmt_repr = to_text(result.cursor.query)
            stmt_repr = self._postprocess_stmt_repr(stmt_repr)
        except Exception:
            pass

        self._log_timing(
            message='execute_call', timing=t_call_end - t_call_start,
            meta=result_meta, stmt_repr=stmt_repr, stage=1)
        return result


class ConnectionAcquireTimeout(asyncio.TimeoutError):
    """ ... """


class DatabaseRouting:
    """
    3-state database router: provides a `conn` attribute which is either of:
      * Connection to a readonly replica
      * Connection to the master host
      * Connection to the master host within a transaction.
    """

    conn = None
    conn_writing = None
    conn_reading = None
    tx = None
    inner_tx = None

    _stack: Optional[contextlib.AsyncExitStack] = None

    def __init__(
            self, db_pool_writing, db_pool_reading,
            parent=None, debug=True,
            connect_timeout=None, connect_timeout_callback=None):

        assert db_pool_writing
        assert db_pool_reading
        self.db_pool_writing = db_pool_writing
        self.db_pool_reading = db_pool_reading

        self.debug = debug
        self.connect_timeout = connect_timeout
        self.connect_timeout_callback = connect_timeout_callback

        if parent is None:
            self._on_after_commit_handlers = []
            self._on_after_commit_once_handlers = []
        else:
            self._on_after_commit_handlers = parent._on_after_commit_handlers
            self._on_after_commit_once_handlers = parent._on_after_commit_once_handlers

    def on_after_commit(self, func):
        self._on_after_commit_handlers.append(func)

    def on_after_commit_once(self, func):
        self._on_after_commit_once_handlers.append(func)

    async def ensure_db_writing(self):
        if self.conn_writing is not None:
            return
        if self._stack is None:
            raise RuntimeError("`ensure_db_writing` is only allowed within `manage` context")
        self._manage_cmstack(writing=True)

    @contextlib.asynccontextmanager
    async def manage(self, writing=True, tx=False, nested_tx=False):
        """
        Async context manager to ensure the state is as desired until the end
        of the uppermost `manage` context.
        """
        kwargs = dict(writing=writing, tx=tx, nested_tx=nested_tx)
        stack = self._stack
        if self._stack is not None:
            async with self._manage_i(**kwargs) as res:
                yield res
            return

        stack = contextlib.AsyncExitStack()
        # Ensure the last exit will clean the attrs.
        await stack.enter_async_context(self._cleanup_cm())
        self._stack = stack

        async with stack:
            async with self._manage_i(**kwargs) as res:
                yield res

    @contextlib.asynccontextmanager
    async def _manage_i(self, writing=True, tx=False, nested_tx=False):
        assert self._stack is not None

        await self._manage_cmstack(writing=writing, tx=tx)

        if nested_tx:
            if not tx:
                raise ValueError((
                    "nested transaction only makes sense when requesting an"
                    " uppermost transaction too"))
            # XXXXX: what happens if `self._stack` is exited before this CM?
            # Supposedly the transaction should become invalid.
            conn_writing = self.conn_writing
            assert conn_writing is not None
            assert self.conn is conn_writing
            prev_inner_tx = self.inner_tx
            try:
                with conn_writing.begin_nested() as inner_tx:
                    self.inner_tx = inner_tx
                    yield inner_tx
            finally:
                self.inner_tx = prev_inner_tx
            return

        yield self.conn

    @contextlib.asynccontextmanager
    async def _cleanup_cm(self):
        try:
            yield None
        finally:
            self._stack = None
            self.tx = None
            self.conn = None
            self.conn_writing = None
            self.conn_reading = None

    @contextlib.asynccontextmanager
    async def _after_tx_handlers_cm(self):
        try:
            yield None
        except Exception:
            await self._handle_on_after_rollback()
            raise
        await self._handle_on_after_commit()

    async def _handle_on_after_rollback(self):
        pass  # not supported yet.

    @staticmethod
    async def _run_handlers(lst, kwargs=None):
        if kwargs is None:
            kwargs = {}
        for func in lst:
            result = func()
            if iscoroutine(result):
                result = await result

    async def _handle_on_after_commit(self):
        kwargs = dict(db_manager=self)
        await self._run_handlers(self._on_after_commit_handlers, kwargs=kwargs)
        # Supports run-one handlers that add more run-once handlers.
        while True:
            run_once_handlers = self._on_after_commit_once_handlers[:]
            if not run_once_handlers:
                break
            self._on_after_commit_once_handlers[:] = []
            await self._run_handlers(run_once_handlers, kwargs=kwargs)

    @contextlib.asynccontextmanager
    async def _acquire_cm(self):
        if not self.connect_timeout:
            yield None
            return
        try:
            async with async_timeout.timeout(self.connect_timeout) as cmres:
                # raise asyncio.TimeoutError("test")  # haxdebug
                yield cmres
        except asyncio.TimeoutError as exc:
            LOGGER.exception("Connection acquire timed out: %r", exc)
            if self.connect_timeout_callback is not None:
                self.connect_timeout_callback(exc=exc)
            raise ConnectionAcquireTimeout(exc) from exc

    async def _manage_cmstack(self, writing=True, tx=False):
        """
        `self.manage` innards that expect `self._stack` to be managed.
        """
        stack = self._stack
        assert stack is not None
        if not writing:
            # `self.conn` check is for the case when `writing=True` is already
            # initialized, in which case the `self.conn_writing` is to be used,
            # under the assumption that it is suitable for reading.
            if self.conn is None and self.conn_reading is None:
                async with self._acquire_cm():
                    conn = await stack.enter_async_context(self.db_pool_reading.acquire())

                conn = self._maybe_debug_wrap_conn(conn, name='reading')
                self.conn_reading = conn
                self.conn = conn
        else:  # if writing:
            if self.conn_writing is None:
                async with self._acquire_cm():
                    conn = await stack.enter_async_context(self.db_pool_writing.acquire())

                conn = self._maybe_debug_wrap_conn(conn, name='writing')
                self.conn_writing = conn
                # Wrote anything => keep using the 'master' connection by default.
                self.conn = conn

        if tx:
            if not writing:
                raise ValueError((
                    "transaction over the read-only database is not supported here"))
            if self.tx is None:
                await stack.enter_async_context(self._after_tx_handlers_cm())
                assert self.conn_writing is not None
                self.tx = await stack.enter_async_context(self.conn_writing.begin())

    def _maybe_debug_wrap_conn(self, conn, name):
        if not self.debug:
            return conn
        return DebugConnectionWrapper(conn, name)


def iterate_by_pk(stmt, db_conn, pk=None, size=5000, max_steps=900000, return_by_one=True):
    """ Iterate over query results using a primary key filter """
    from sqlalchemy.sql.expression import tuple_ as sa_tuple

    if pk is None:
        _froms = stmt.froms
        if len(_froms) != 1:
            raise Exception((
                "Cannot automatically determine the primary key for a statement"
                " with multiple `from`s"))
        pk = tuple(_froms[0].primary_key.columns)

    if not isinstance(pk, (list, tuple)):
        pk = (pk,)

    pk = tuple(pk)
    if len(pk) == 1:
        pk_expr = pk[0]
    else:
        pk_expr = sa_tuple(*pk)

    # stmt_orig = stmt
    stmt = stmt.order_by(*pk)
    stmt = stmt.limit(size)
    for col in pk:  # ensure all primary key columns are also in the results
        stmt = stmt.column(col)
    stmt_base = stmt

    for _ in range(max_steps):
        results = db_conn.execute(stmt)
        results = list(results)
        if not results:
            return  # supposedly done.
        if return_by_one:
            for item in results:
                yield item
        else:
            yield results
        last_row = results[-1]
        last_pk_value = tuple([last_row[col.name] for col in pk])
        next_filter = pk_expr > (last_pk_value[0] if len(pk) == 1 else last_pk_value)
        stmt = stmt_base.where(next_filter)

    raise LimitReached("reached `max_steps`", max_steps)


def upsert_statement(table, values, key, **kwargs):
    import sqlalchemy.dialects.postgresql as pg
    stmt = pg.insert(table).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=key,
        set_=values,
        **kwargs)
    return stmt


def bulk_upsert_statement(
        table, items, key, columns=None, where=None, returning_hax=False,
        auto_where=False, **kwargs):
    import sqlalchemy.dialects.postgresql as pg

    # https://stackoverflow.com/a/34529505
    # ???: https://gist.github.com/clhenrick/de68e7bf948c8e62b41e34b1340e0707
    # https://stackoverflow.com/a/44865375
    if columns is None:
        columns = set(fld for item in items for fld in item.keys())
    columns = set(columns) - set(key)  # uncertain

    if returning_hax and not columns:
        # HAX: do a fake update to ensure that do_update is done and `returning` works.
        # WARNING: not performant (each conflicting row will be rewritten entirely by postgresql)
        columns = key[:1]

    insert_statement = pg.insert(table).values(items)

    return bulk_upsert_for_insert_statement(
        insert_statement, key=key, columns=columns, table=table,
        where=where, auto_where=auto_where, **kwargs)


def bulk_upsert_for_insert_statement(
        insert_statement, key, columns, table=None,
        where=None, auto_where=True, **kwargs):

    # For abundant details, see:
    # https://stackoverflow.com/a/42217872

    if table is None:
        table = insert_statement.table
    table = getattr(table, '__table__', table)

    # Ensure not unnecessary writing is done. Not compatible with `returning`.
    if auto_where:
        where_pieces = [
            getattr(table.c, fld) != getattr(insert_statement.excluded, fld)
            for fld in columns
        ]
        if where is not None:
            where_pieces = [where] + where_pieces
        where = sa_any(where_pieces)

    stmt = insert_statement.on_conflict_do_update(
        index_elements=key,
        # WARNING: this is not very (relatively) performant for large amounts of unchanged rows.
        set_={fld: getattr(insert_statement.excluded, fld) for fld in columns},
        where=where,
        **kwargs)
    return stmt


def common_upsert_objects_straight(table, items, db_conn, key, id_col='id', **kwargs):
    """ A common case for bulk-upserting stuff """
    table_obj = getattr(table, '__table__', table)
    stmt = bulk_upsert_statement(
        table=table, items=items, key=key, returning_hax=True, **kwargs)
    stmt = stmt.returning(*(getattr(table_obj.c, col) for col in (id_col,) + tuple(key)))
    results = db_conn.execute(stmt)
    # results = list(results)

    result = {
        tuple(getattr(row, col) for col in key): getattr(row, id_col)
        for row in results}
    if len(key) == 1:
        result = {item_key[0]: item_id for item_key, item_id in result.items()}
    return result


def common_upsert_objects_twopart(
        table, items, db_conn, key, id_col='id', return_all=True, logger=LOG, **kwargs):
    """ A common case for bulk-upserting stuff (minimized modifications version) """
    from sqlalchemy.sql.expression import tuple_ as sa_tuple

    table_obj = getattr(table, '__table__', table)
    stmt = bulk_upsert_statement(
        table=table, items=items, key=key, auto_where=True, **kwargs)

    results_cols = [
        getattr(table_obj.c, col)
        for col in (id_col,) + tuple(key)
    ]
    stmt = stmt.returning(*results_cols)

    if logger is not None:
        logger.debug("upsert %r <- %r", table, len(items))

    results_p1 = db_conn.execute(stmt)
    results_p1 = list(results_p1)
    results = results_p1
    if logger is not None:
        logger.debug(" ... %r changes", len(results_p1))

    if return_all:  # otherwise, only the changed/added ones will be returned.
        # Get he key -> id mapping

        # # TODO?: merge with `results` above, throw out items that were already returned?
        # results_p1_set = set(tuple(row[col] for col in key) for row in results_p1)
        # items_filtered = [
        #     item for item in items
        #     if tuple(item[col] for col in key) not in results_p1_set
        # ]

        if len(key) == 1:
            key_col = key[0]
            filter_col = getattr(table_obj.c, key_col)
            values = [item[key_col] for item in items]
        else:
            filter_col = sa_tuple(*(getattr(table_obj.c, fld) for fld in key))
            values = [
                tuple(item[col] for col in key)
                for item in items
            ]
        stmt_get = (
            table_obj.select(filter_col.in_(values))
            .with_only_columns(results_cols))
        if logger is not None:
            logger.debug(" ... retrieve `id`s.")
        results_p2 = db_conn.execute(stmt_get)
        results = results_p2
        # results = itertools.chain(results_p1, results_p2)

    result = {
        tuple(getattr(row, col) for col in key): getattr(row, id_col)
        for row in results}
    if len(key) == 1:
        result = {item_key[0]: item_id for item_key, item_id in result.items()}

    return result


def sa_all(parts):
    """
    Join the sqlalchemy conditions over `AND`.

    :param parts: a nonempty iterable of conditionals.
    """
    return functools.reduce(lambda val1, val2: val1 & val2, parts)


def sa_any(parts):
    """
    Join the sqlalchemy conditions over `OR`.

    :param parts: a nonempty iterable of conditionals.
    """
    return functools.reduce(lambda val1, val2: val1 | val2, parts)


def can_retry_error(exc, check_attr=True):
    import psycopg2
    from sqlalchemy.exc import StatementError
    if check_attr and getattr(exc, '_already_retried', None):
        return False

    if isinstance(exc, StatementError):
        # easier to work with the pg-specific base here:
        exc = exc.orig

    if isinstance(exc, (
            psycopg2.DatabaseError,
            psycopg2.OperationalError,
            psycopg2.InterfaceError)):
        return True
    if isinstance(exc, psycopg2.InternalError):
        if re.match(
                '^cannot execute (UPDATE|INSERT|DELETE) in a read-only transaction$',
                exc.message):
            return True
    return False


async def get_or_create_aio(conn, table, key, values, on_before_create=None, for_update=False):
    """
    ...

    NOTE: involves `on conflict do nothing`.

    :param conn: ..., aiopg connection.

    :param table: ..., sqlalchemy table or a declarative table.

    :param key: either of: list of fields specified in the `value`; or,
    dictionary `{field: value, ...}` with key filters on the table.

    :raises: NotFound
    :raises: MultipleObjectsReturned
    """
    assert key, "shouldn't be empty"
    import sqlalchemy.dialects.postgresql as pg
    if isinstance(key, (list, tuple)):
        key = {fld: values[fld] for fld in key}
    elif isinstance(key, dict):
        pass
    else:
        raise Exception("Unexpected `key` type")
    table_obj = getattr(table, '__table__', table)
    key_where = sa_all(getattr(table_obj.c, fld) == val for fld, val in key.items())
    select = table_obj.select(key_where)
    if for_update:
        select = select.with_for_update()
    # savepoint transaction for the get-or-create logic.
    async with await conn.begin_nested():
        rows = await conn.execute(select)
        try:
            result = await db_get_one(rows)
        except NotFound:
            if on_before_create is not None:
                await on_before_create(conn=conn, table=table, table_obj=table_obj, key=key, values=values)
            insert = pg.insert(table_obj).values(values).on_conflict_do_nothing()
            await conn.execute(insert)
            rows = await conn.execute(select)
            result = await db_get_one(rows)  # NOTE: might still raise NotFound if the usage is incorrect.
    return result


def get_or_create_sasess(session, model, defaults=None, **kwargs):
    from sqlalchemy.sql import ClauseElement
    instance = session.query(model).filter_by(**kwargs).first()
    if instance is not None:
        return instance, False

    params = {
        key: value
        for key, value in kwargs.items()
        if not isinstance(value, ClauseElement)}
    params.update(defaults or {})
    instance = model(**params)
    session.add(instance)
    return instance, True
