from __future__ import annotations

import datetime
import os

import pytest
import sqlalchemy
import sqlalchemy_metrika_api

METRIKA_SAMPLE_COUNTER_ID = "44147844"
APPMETRICA_SAMPLE_COUNTER_ID = "1111"


def _get_oauth_from_env():
    return os.environ.get("METRIKA_OAUTH", None)


@pytest.fixture
def metrika_sample_counter_id():
    return METRIKA_SAMPLE_COUNTER_ID


@pytest.fixture
def appmetrica_sample_counter_id():
    return APPMETRICA_SAMPLE_COUNTER_ID


@pytest.fixture(scope="function")
def shrink_metrika_default_date_period(monkeypatch):
    """
    To reduce load to Metrika API and tests run time.
    """
    monkeypatch.setattr(sqlalchemy_metrika_api.base, "DEFAULT_DATE_PERIOD", 3)


def _metrika_db_engine():
    return sqlalchemy.create_engine("metrika_api://:{}@/hits".format(_get_oauth_from_env()))


def _appmetrica_db_engine():
    return sqlalchemy.create_engine("appmetrica_api://:{}@/installs".format(_get_oauth_from_env()))


@pytest.fixture(scope="function")
def metrika_db_engine(shrink_metrika_default_date_period):
    return _metrika_db_engine()


@pytest.fixture(scope="function")
def metrika_db_engine_with_accuracy(shrink_metrika_default_date_period):
    return sqlalchemy.create_engine("metrika_api://:{}@/hits?accuracy=0.1".format(_get_oauth_from_env()))


@pytest.fixture(scope="function")
def appmetrica_db_engine(shrink_metrika_default_date_period):
    return _appmetrica_db_engine()


@pytest.fixture(params=[_metrika_db_engine, _appmetrica_db_engine])
def db_engine(request, shrink_metrika_default_date_period):
    return request.param()


def _gen_m_expr1(eng):
    expr = sqlalchemy.select(
        columns=[
            sqlalchemy.type_coerce(sqlalchemy.column("ym:pv:pageviews"), sqlalchemy.Float()),
            sqlalchemy.column("ym:pv:users").label("users"),
            sqlalchemy.column("ym:pv:date").label("date"),
            # sqlalchemy.column('ym:pv:URLDomain'),
            sqlalchemy.column("ym:pv:browserName").label("browser"),
        ]
    )
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.where(sqlalchemy.column("date").between("2019-02-01", "2019-02-03"))
    expr = expr.where(sqlalchemy.column("browser") == "Yandex Browser")
    # expr = expr.group_by(sqlalchemy.column('ym:pv:screenResolution'))
    # expr = expr.group_by(sqlalchemy.column('ym:pv:date'))
    expr = expr.group_by(sqlalchemy.column("date"))
    expr = expr.group_by(sqlalchemy.column("browser"))
    expr = expr.limit(5)
    # expr = expr.offset(50)
    expr = expr.order_by(sqlalchemy.column("ym:pv:date"))
    return expr


def _gen_m_expr_distinct_via_group_by(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:browserName").label("browser")])
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.group_by(sqlalchemy.column("browser"))
    return expr


def _gen_m_expr_distinct(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:browserName")]).distinct()
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    return expr


def _gen_m_expr_date_min(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:date").label("date")])
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.group_by(sqlalchemy.column("date"))
    expr = expr.limit(1)
    # expr = expr.order_by('date')
    expr = expr.order_by(sqlalchemy.column("date"))
    return expr


def _gen_m_expr_date_max(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:date").label("date")])
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.group_by(sqlalchemy.column("date"))
    expr = expr.limit(1)
    expr = expr.order_by(sqlalchemy.desc(sqlalchemy.column("date")))
    return expr


def _gen_m_expr_order_by(eng):
    expr = sqlalchemy.select(
        columns=[
            sqlalchemy.column("ym:pv:date").label("date"),
            sqlalchemy.column("ym:pv:users").label("users"),
        ]
    )
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.group_by(sqlalchemy.column("date"))
    expr = expr.order_by(sqlalchemy.desc(sqlalchemy.literal_column("users")))
    return expr


def _gen_m_expr_cast(eng):
    expr = sqlalchemy.select(
        columns=[
            sqlalchemy.cast(sqlalchemy.column("ym:pv:pageviews"), sqlalchemy.INTEGER),
            sqlalchemy.cast(sqlalchemy.column("ym:pv:users"), sqlalchemy.String).label("users_str"),
            sqlalchemy.type_coerce(
                sqlalchemy.type_coerce(sqlalchemy.column("ym:pv:users"), sqlalchemy.Integer), sqlalchemy.Integer
            ).label("users_int"),
        ]
    )
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    return expr


def _gen_m_expr_date_between(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:pageviews"), sqlalchemy.column("ym:pv:users")])
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.where(sqlalchemy.column("ym:pv:date").between("2019-02-01", "2019-02-03"))
    return expr


def _gen_m_expr_datetime_between_date(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:pageviews"), sqlalchemy.column("ym:pv:users")])
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.where(sqlalchemy.column("ym:pv:dateTime").between("2019-02-01", "2019-02-03"))
    return expr


def _gen_m_expr_datetime_between_datetime(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:pageviews"), sqlalchemy.column("ym:pv:users")])
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.where(
        sqlalchemy.column("ym:pv:dateTime").between(
            datetime.datetime.fromisoformat("2019-02-01 00:00:00"),
            datetime.datetime.fromisoformat("2019-02-03 23:59:59"),
        )
    )
    return expr


def _gen_m_expr_datetime_between_datetime_str(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:pageviews"), sqlalchemy.column("ym:pv:users")])
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.where(sqlalchemy.column("ym:pv:dateTime").between("2019-02-01 00:00:00", "2019-02-03 23:59:59"))
    return expr


def __gen_select_users_browser(eng):
    expr = sqlalchemy.select(
        columns=[
            sqlalchemy.column("ym:pv:users").label("users"),
            sqlalchemy.column("ym:pv:browserName").label("browser"),
        ]
    )
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.group_by(sqlalchemy.column("browser"))
    return expr


def _gen_m_expr_like(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(sqlalchemy.column("browser").like("Yandex*"))
    return expr


def _gen_m_expr_not_like(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(sqlalchemy.column("browser").notlike("Yandex*"))
    return expr


def _gen_m_expr_not_like_1(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(sqlalchemy.not_(sqlalchemy.column("browser").like("Yandex*")))
    return expr


def _gen_m_expr_contains(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(sqlalchemy.column("browser").contains("Yandex"))
    return expr


def _gen_m_expr_not_contains(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(sqlalchemy.not_(sqlalchemy.column("browser").contains("Yandex")))
    return expr


def _gen_m_expr_startswith(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(sqlalchemy.column("browser").startswith("Yandex"))
    return expr


def _gen_m_expr_not_endswith(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(sqlalchemy.not_(sqlalchemy.column("browser").endswith("Chrome")))
    return expr


def _gen_m_expr_not(eng):
    expr = __gen_select_users_browser(eng)
    expr = expr.where(
        sqlalchemy.not_(
            sqlalchemy.or_(
                sqlalchemy.column("browser").like("Yandex*"),
                sqlalchemy.column("browser") == "Google Chrome",
            )
        )
    )
    return expr


def _gen_m_expr_not_in(eng):
    expr = sqlalchemy.select(
        columns=[
            sqlalchemy.column("ym:pv:users").label("users"),
            sqlalchemy.column("ym:pv:browserName").label("browser"),
        ]
    )
    expr = expr.select_from(sqlalchemy.Table(METRIKA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.group_by(sqlalchemy.column("browser"))
    expr = expr.where(sqlalchemy.column("browser").notin_(["Yandex Browser", "Google Chrome"]))
    return expr


def _gen_am_expr1(eng):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:u:sessions"), sqlalchemy.column("ym:u:activeUsers")])
    expr = expr.group_by(sqlalchemy.column("ym:u:dayOfWeekName"))
    expr = expr.select_from(sqlalchemy.Table(APPMETRICA_SAMPLE_COUNTER_ID, sqlalchemy.MetaData(bind=eng)))
    expr = expr.where(sqlalchemy.column("ym:u:date").between("2019-05-01", "2019-05-03"))
    return expr


metrika_expr_functions_list = [
    _gen_m_expr1,
    _gen_m_expr_date_max,
    _gen_m_expr_date_min,
    _gen_m_expr_distinct,
    _gen_m_expr_distinct_via_group_by,
    _gen_m_expr_cast,
    _gen_m_expr_date_between,
    _gen_m_expr_datetime_between_date,
    _gen_m_expr_datetime_between_datetime,
    _gen_m_expr_datetime_between_datetime_str,
    _gen_m_expr_like,
    _gen_m_expr_not_like,
    _gen_m_expr_not_like_1,
    _gen_m_expr_contains,
    _gen_m_expr_not_contains,
    _gen_m_expr_not,
    _gen_m_expr_not_in,
    _gen_m_expr_order_by,
]

appmetrica_expr_functions_list = [
    _gen_am_expr1,
]


@pytest.fixture(params=metrika_expr_functions_list)
def metrika_expr_func(request):
    return request.param


@pytest.fixture(params=appmetrica_expr_functions_list)
def appmetrica_expr_func(request):
    return request.param


@pytest.fixture
def m_expr_distinct(metrika_sample_counter_id, metrika_db_engine):
    expr = sqlalchemy.select(columns=[sqlalchemy.column("ym:pv:dateTime")]).distinct()
    expr = expr.select_from(sqlalchemy.Table(metrika_sample_counter_id, sqlalchemy.MetaData(bind=metrika_db_engine)))
    return expr


@pytest.fixture()
def metrika_expr_func_expr1():
    return _gen_m_expr1


@pytest.fixture
def metrika_expr_select_date_users(metrika_sample_counter_id, metrika_db_engine):
    expr = sqlalchemy.select(
        columns=[sqlalchemy.column("ym:pv:date").label("date"), sqlalchemy.column("ym:pv:users")],
    )
    expr = expr.select_from(sqlalchemy.Table(metrika_sample_counter_id, sqlalchemy.MetaData(bind=metrika_db_engine)))
    expr = expr.group_by(sqlalchemy.column("date"))
    return expr
