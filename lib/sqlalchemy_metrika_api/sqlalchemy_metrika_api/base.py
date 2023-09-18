from __future__ import annotations

import datetime
import logging
import re
from urllib.parse import urlencode

import dateutil.parser
from sqlalchemy import (
    Unicode,
    exc,
    pool,
    util,
)
from sqlalchemy.engine import (
    default,
    reflection,
)
from sqlalchemy.sql import (
    compiler,
    elements,
    operators,
    sqltypes,
)
from sqlalchemy_metrika_api import (
    appmetrica_dbapi,
    metrika_dbapi,
)
from sqlalchemy_metrika_api.api_info import appmetrica as appmetrica_api_info
from sqlalchemy_metrika_api.api_info import metrika as metrika_api_info
from sqlalchemy_metrika_api.exceptions import (
    MetrikaApiDimensionInCalc,
    MetrikaApiGroupByNotSupported,
    MetrikaApiNoMetricsNorGroupBy,
    NotSupportedError,
)

LOGGER = logging.getLogger(__name__)


MAX_LIMIT_VALUE = 10000
DEFAULT_DATE_PERIOD = 60  # days


class MetrikaApiReqPreparer(compiler.IdentifierPreparer):
    illegal_initial_characters = {"$"}
    legal_characters = re.compile(r"^[A-Z0-9_:<>\-$]+$", re.I)  # added ":<>-"

    def __init__(self, dialect, **kwargs):
        kwargs.update(initial_quote="'", escape_quote="'")
        super(MetrikaApiReqPreparer, self).__init__(dialect, **kwargs)

    def _requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""
        lc_value = value.lower()
        return (
            lc_value in self.reserved_words
            or value[0] in self.illegal_initial_characters
            or not self.legal_characters.match(util.text_type(value))
            # or (lc_value != value)
        )


class MetrikaApiReqCompiler(compiler.SQLCompiler):
    _extra_select_params = None
    _extra_bind_params = None
    _ordered_columns = None
    _labeled_columns_map = None
    _group_by_fields = None
    _casts = None
    _date_filter_present = None

    def _flush_tmp_properties(self):
        self._extra_select_params = {}
        self._extra_bind_params = {}
        self._ordered_columns = False
        self._labeled_columns_map = {}
        self._group_by_fields = []
        self._casts = {}
        self._date_filter_present = False

    @property
    def api_info(self):
        return metrika_api_info

    def check_field_is_date_datetime(self, field_name):
        field_info = self.api_info.fields_by_name.get(field_name)
        return field_info and field_info["type"] in ("date", "datetime")

    def visit_column(
        self,
        column,
        add_to_result_map=None,
        include_table=True,
        _is_in_filter=False,
        **kwargs,
    ):
        name = orig_name = column.name
        if name is None:
            name = self._fallback_column_name(column)

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(name, orig_name, (column, name, column.key), column.type)

        # Replacing label with real field name
        if name in self._labeled_columns_map:
            name = self._labeled_columns_map[name][0]

        if _is_in_filter and self.check_field_is_date_datetime(name):
            self._date_filter_present = True

        if is_literal:
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(name)

        return name

    def visit_eq_binary(self, binary, operator_, **kw):
        return self._generate_generic_binary(binary, "==", **kw)  # "==" instead of "="

    def visit_inv_unary(self, element, operator_, **kw):
        return "NOT(%s)" % self.process(element.element, **kw)

    def bindparam_string(self, name, _extra_quoting=True, **kw):
        if _extra_quoting:
            return "'%s'" % (self.bindtemplate % {"name": name})
        else:
            return self.bindtemplate % {"name": name}

    def render_literal_value(self, value, type_):
        if isinstance(type_, Unicode):
            assert isinstance(value, str)
            value = value.replace("\\", "\\\\").replace("'", "\\'")

            if self.dialect.identifier_preparer._double_percents:
                value = value.replace("%", "%%")

            return "'%s'" % value

        processor = type_._cached_literal_processor(self.dialect)
        if processor:
            return processor(value)
        else:
            raise NotImplementedError("Don't know how to literal-quote value %r" % value)

    def visit_bindparam(
        self,
        bindparam,
        within_columns_clause=False,
        literal_binds=False,
        skip_bind_expression=False,
        _extra_quoting=True,
        **kwargs,
    ):
        if literal_binds or (within_columns_clause and self.ansi_bind_rules):
            if bindparam.value is None and bindparam.callable is None:
                raise exc.CompileError(
                    "Bind parameter '%s' without a " "renderable value not allowed here." % bindparam.key
                )
            return self.render_literal_bindparam(bindparam, within_columns_clause=True, **kwargs)

        if bindparam.callable:
            value = bindparam.effective_value
        else:
            value = bindparam.value

        def escape(value: str) -> str:
            return value.replace("\\", "\\\\").replace("'", "\\'")

        if bindparam.expanding:
            assert not isinstance(value, str)
            return "({})".format(
                ", ".join(f"'{escape(str(piece))}'" if _extra_quoting else escape(str(piece)) for piece in value)
            )

        if isinstance(value, list) and len(value) == 1:
            value = value[0]

        if isinstance(value, str):
            value = escape(value)

        if _extra_quoting:
            return "'%s'" % value
        return value

    def visit_between_op_binary(self, binary, operator, **kw):
        left_value = binary.left._compiler_dispatch(self, **kw)
        if self.check_field_is_date_datetime(left_value):
            if len(binary.right.clauses) != 2:
                raise exc.CompileError("Unexpected between arguments count")
            self._extra_select_params.update(
                date1=binary.right.clauses[0]._compiler_dispatch(self, _extra_quoting=False),
                date2=binary.right.clauses[1]._compiler_dispatch(self, _extra_quoting=False),
            )
        else:
            raise NotSupportedError(
                "BETWEEN operator supported only for date/datetime fields. " "Requested field: {}".format(left_value)
            )

        return ""

    def visit_not_between_op_binary(self, binary, operator, **kw):
        raise NotSupportedError()

    def visit_notbetween_op_binary(self, binary, operator, **kw):
        raise NotSupportedError()

    @util.memoized_property
    def _like_percent_literal(self):
        return elements.literal_column("'*'", type_=sqltypes.STRINGTYPE)

    def visit_contains_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        binary.right.value = "*%s*" % binary.right.value
        return self.visit_like_op_binary(binary, operator, **kw)

    def visit_not_contains_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        binary.right.value = "*%s*" % binary.right.value
        return self.visit_notlike_op_binary(binary, operator, **kw)

    def visit_notcontains_op_binary(self, binary, operator, **kw):
        return self.visit_not_contains_op_binary(binary, operator, **kw)

    def visit_startswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        binary.right.value = "%s*" % binary.right.value
        return self.visit_like_op_binary(binary, operator, **kw)

    def visit_not_startswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        binary.right.value = "%s*" % binary.right.value
        return self.visit_notlike_op_binary(binary, operator, **kw)

    def visit_notstartsswith_op_binary(self, binary, operator, **kw):
        return self.visit_not_startsswith_op_binary(binary, operator, **kw)

    def visit_endswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        binary.right.value = "*%s" % binary.right.value
        return self.visit_like_op_binary(binary, operator, **kw)

    def visit_not_endswith_op_binary(self, binary, operator, **kw):
        binary = binary._clone()
        binary.right.value = "*%s" % binary.right.value
        return self.visit_notlike_op_binary(binary, operator, **kw)

    def visit_notendswith_op_binary(self, binary, operator, **kw):
        return self.visit_not_endswith_op_binary(binary, operator, **kw)

    def visit_like_op_binary(self, binary, operator, **kw):
        return "%s=*%s" % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw),
        )

    def visit_notlike_op_binary(self, binary, operator, **kw):
        return self.visit_not_like_op_binary(binary, operator, **kw)

    def visit_not_like_op_binary(self, binary, operator, **kw):
        return "%s!*%s" % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw),
        )

    def visit_ilike_op_binary(self, binary, operator, **kw):
        raise NotSupportedError()

    def visit_not_ilike_op_binary(self, binary, operator, **kw):
        raise NotSupportedError()

    def visit_notilike_op_binary(self, binary, operator, **kw):
        raise NotSupportedError()

    def _get_clause_name(self, clause, **kwargs):
        if hasattr(clause, "name"):
            return clause.name
        elif hasattr(clause, "clause") and hasattr(clause.clause, "name"):
            return clause.clause.name
        else:
            return clause._compiler_dispatch(self, **kwargs)

    def visit_sum_func(self, func, **kwargs):
        clauses = func.clause_expr.element.clauses
        for cla in clauses:
            cla_name = self._get_clause_name(cla, **kwargs)
            if cla_name in self.api_info.fields_by_name and self.api_info.fields_by_name[cla_name]["is_dim"]:
                raise MetrikaApiDimensionInCalc('Not able to use dimensions in calculations: "%s"' % cla_name)

        return " + ".join([cla._compiler_dispatch(self, **kwargs) for cla in clauses])

    def visit_clauselist(self, clauselist, _group_by_clause=False, **kwargs):
        sep = clauselist.operator
        if sep is None:
            sep = " "
        elif sep == operators.comma_op:
            sep = ","  # instead of ", "
        else:
            sep = compiler.OPERATORS[clauselist.operator]

        if _group_by_clause:
            clauses = []
            for cla in clauselist.clauses:
                cla_name = self._get_clause_name(cla, **kwargs)
                if cla_name in self._labeled_columns_map and self._labeled_columns_map[cla_name][1] is not None:
                    clauses.append(self._labeled_columns_map[cla_name][1])
                else:
                    clauses.append(cla)

            for cla in clauses:
                cla_name = self._get_clause_name(cla, **kwargs)
                if cla_name not in self.api_info.fields_by_name or not self.api_info.fields_by_name[cla_name]["is_dim"]:
                    raise MetrikaApiGroupByNotSupported('Grouping by field "%s" is not possible' % cla_name)

            self._group_by_fields = [name for name in (self._get_clause_name(cla, **kwargs) for cla in clauses) if name]
        else:
            clauses = clauselist.clauses

        return sep.join(s for s in (c._compiler_dispatch(self, **kwargs) for c in clauses) if s)

    def visit_select(self, *args, **kwargs):
        self._flush_tmp_properties()
        return super().visit_select(*args, **kwargs)

    def _compose_select_body(
        self,
        text,
        select,
        compile_state,
        inner_columns,
        froms,
        byfrom,
        toplevel,
        kwargs,
    ):
        """
        https://tech.yandex.ru/metrika/doc/api2/api_v1/data-docpage/
        """

        query_params = {}

        if self.linting & compiler.COLLECT_CARTESIAN_PRODUCTS:
            from_linter = compiler.FromLinter({}, set())
            warn_linting = self.linting & compiler.WARN_LINTING
            if toplevel:
                self.from_linter = from_linter
        else:
            from_linter = None
            warn_linting = False

        if froms:
            query_params.update(
                ids=",".join(
                    [f._compiler_dispatch(self, asfrom=True, from_linter=from_linter, **kwargs) for f in froms]
                )
            )
        else:
            raise NotSupportedError('empty "FROM" clause')

        filters = []
        if select._where_criteria:
            where_str = self._generate_delimited_and_list(
                select._where_criteria, from_linter=from_linter, _is_in_filter=True, **kwargs
            )
            if where_str:
                filters.append(where_str)

        if warn_linting:
            from_linter.warn()

        if select._having_criteria:
            having_str = self._generate_delimited_and_list(
                select._having_criteria,
                _is_in_filter=True,
                **kwargs,
            )
            if having_str:
                filters.append(having_str)

        if filters:
            query_params.update(filters=" AND ".join(filters))

        if select._group_by_clauses:
            # Here only generating self._group_by_fields list.
            # It will be written into query_params later, after metrics list generation.
            select._group_by_clause._compiler_dispatch(self, _group_by_clause=True, **kwargs)

        metrics_cols = []
        metrics_dims_cols = []
        for col in inner_columns:
            col_desc = self.api_info.fields_by_name.get(col)
            if col_desc and col_desc["is_dim"]:
                metrics_dims_cols.append(col)
            else:
                metrics_cols.append(col)
        if not metrics_cols:
            if metrics_dims_cols and select._distinct:
                for dim in metrics_dims_cols:
                    self._group_by_fields.append(dim)

            if self._group_by_fields:
                # Hack to be able to get dimensions values
                # without necessity to specify any unnecessary metric field explicitly.
                fields_namespace = self.api_info.get_namespace_by_name(self._group_by_fields[0])
                any_metric = self.api_info.metrics_by_namespace[fields_namespace][0]["name"]
                metrics_cols.append(any_metric)
            else:
                raise MetrikaApiNoMetricsNorGroupBy("Not found neither metrics to select nor dimensions for group by.")
        query_params.update(metrics=",".join(metrics_cols))

        if self._group_by_fields:
            query_params.update(dimensions=",".join(self._group_by_fields))

        if select._order_by_clauses:
            order_expr = select._order_by_clause._compiler_dispatch(self, **kwargs)
            if order_expr:
                query_params.update(sort=order_expr)

        if select._limit_clause is not None and select._limit_clause.value < MAX_LIMIT_VALUE:
            limit_val = select._limit_clause.value
        else:
            limit_val = MAX_LIMIT_VALUE
        query_params.update(limit=limit_val)

        if select._offset_clause is not None:
            # In Metrika API offset starts from 1
            offset_val = int(select._offset_clause.value) + 1
            query_params.update(offset=offset_val)

        if select._for_update_arg is not None:
            raise NotSupportedError("FOR UPDATE")

        query_params.update(self._extra_select_params)

        for date_param in ("date1", "date2"):
            if date_param in query_params:
                value = query_params[date_param]
                if isinstance(value, list):
                    value = value[0]
                if not type(value) == datetime.date:
                    if isinstance(value, datetime.datetime):
                        value = value.date()
                    else:
                        value = dateutil.parser.parse(value).date()
                query_params[date_param] = value

        today = datetime.date.today()
        if not self._date_filter_present and not query_params.get("date1") and not query_params.get("date2"):
            # TODO: use counter timezone
            dt1 = today - datetime.timedelta(days=DEFAULT_DATE_PERIOD)
            query_params.update(date1=dt1, date2=today)

        if "date2" in query_params and query_params["date2"] > today:
            query_params.update(date2=today)

        return urlencode(query_params)

    def visit_table(self, table, asfrom=False, ashint=False, **kwargs):
        if asfrom or ashint:
            return table.name
        else:
            return ""

    def visit_label(
        self,
        label,
        add_to_result_map=None,
        within_label_clause=False,
        within_columns_clause=False,
        render_label_as_label=None,
        **kw,
    ):
        # only render labels within the columns clause
        # or ORDER BY clause of a select.  dialect-specific compilers
        # can modify this behavior.
        render_label_with_as = within_columns_clause and not within_label_clause
        render_label_only = render_label_as_label is label

        if render_label_only or render_label_with_as:
            if isinstance(label.name, elements._truncated_label):
                labelname = self._truncated_identifier("colident", label.name)
            else:
                labelname = label.name

        if render_label_with_as:
            if add_to_result_map is not None:
                add_to_result_map(
                    labelname,
                    label.name,
                    (label, labelname) + label._alt_names,
                    label.type,
                )

            element_processed = label.element._compiler_dispatch(
                self, within_columns_clause=True, within_label_clause=True, **kw
            )

            def _unwrap_to_column_clause(element):
                LOGGER.info(f"element {element} {type(element)}")
                if isinstance(element, elements.ColumnClause):
                    return element
                if hasattr(element, "clause"):
                    return _unwrap_to_column_clause(element.clause)
                else:
                    LOGGER.warning("Unable dispatch to ColumnClause")
                    return None

            internal_column_clause = _unwrap_to_column_clause(label.element)
            self._labeled_columns_map[labelname] = (element_processed, internal_column_clause)

            return element_processed
        else:
            return label.element._compiler_dispatch(self, within_columns_clause=False, **kw)

    def visit_asc_op_unary_modifier(self, unary, modifier, **kw):
        return unary.element._compiler_dispatch(self, **kw)

    def visit_desc_op_unary_modifier(self, unary, modifier, **kw):
        return "-" + unary.element._compiler_dispatch(self, **kw)

    def visit_cast(self, cast, **kwargs):
        param_name = cast.clause._compiler_dispatch(self, **kwargs)
        cast_type = cast.typeclause._compiler_dispatch(self, **kwargs)
        self._casts[param_name] = cast_type
        return param_name

    def visit_insert(self, *args, **kwargs):
        raise NotSupportedError("INSERT")

    def visit_update(self, *args, **kwargs):
        raise NotSupportedError("UPDATE")

    def construct_params(
        self,
        params=None,
        _group_number=None,
        _check=True,
        extracted_parameters=None,
        escape_names=True,
    ):
        prepared_params = super().construct_params(params, _group_number, _check, extracted_parameters)
        result_cols = []
        for col in self._result_columns:
            field_name = self._labeled_columns_map.get(col[0], (col[0],))[0]
            result_cols.append(
                dict(
                    label=col[0],
                    name=field_name,
                    src_key=self.api_info.fields_by_name.get(field_name, {}).get("src_key"),
                )
            )
        prepared_params["__RESULT_COLUMNS__"] = result_cols
        prepared_params.update(self._extra_bind_params)
        if self._casts:
            prepared_params["__CASTS__"] = self._casts
        LOGGER.info("Metrica query prepared params: %s", prepared_params)
        return prepared_params


# class MetrikaApiExecutionContext(default.DefaultExecutionContext):
#     def get_result_proxy(self):
#         res_proxy = result.ResultProxy(self)
#         return res_proxy


class MetrikaApiDialect(default.DefaultDialect):
    name = "metrika_api"
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    returns_unicode_strings = True
    supports_native_boolean = False
    supports_views = False
    supports_statement_cache = False

    poolclass = pool.SingletonThreadPool  # pool.NullPool

    # execution_ctx_cls = MetrikaApiExecutionContext
    statement_compiler = MetrikaApiReqCompiler
    preparer = MetrikaApiReqPreparer

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        res = connection.execute(metrika_dbapi.InternalCommands.get_tables.value)
        return [item[0] for item in res]

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection)

    def get_columns(self, connection, table_name, schema=None, **kw):
        metrika_fields = connection.execute(metrika_dbapi.InternalCommands.get_columns.value)

        columns = []
        for col in metrika_fields:
            columns.append(
                {
                    "name": col["name"],
                    "type": metrika_dbapi.metrika_types_to_sqla[col["type"]],
                    "nullable": not col["is_dim"],
                }
            )
        return columns

    @classmethod
    def dbapi(cls):
        return metrika_dbapi

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True

    def do_rollback(self, dbapi_connection):
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []


class AppMetricaApiReqCompiler(MetrikaApiReqCompiler):
    @property
    def api_info(self):
        return appmetrica_api_info


class AppMetricaApiDialect(MetrikaApiDialect):
    name = "appmetrica_api"
    statement_compiler = AppMetricaApiReqCompiler
    supports_statement_cache = False

    @classmethod
    def dbapi(cls):
        return appmetrica_dbapi
