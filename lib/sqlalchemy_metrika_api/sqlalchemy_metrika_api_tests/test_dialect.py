from __future__ import annotations

import datetime
from urllib.parse import parse_qs

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import registry
from sqlalchemy.exc import DatabaseError
from sqlalchemy_metrika_api import exceptions

registry.register("metrika_api", "sqlalchemy_metrika_api.base", "MetrikaApiDialect")
registry.register("appmetrica_api", "sqlalchemy_metrika_api.base", "AppMetricaApiDialect")


def _test_execute_expr(_expr_func, _db_engine):
    expr = _expr_func(_db_engine)

    compiled = str(expr)
    print(compiled)
    res = _db_engine.execute(expr)
    # print(res._cursor_description())
    data = res.fetchall()
    for row in data:
        print(row)


def test_execute_expr(metrika_expr_func, metrika_db_engine):
    return _test_execute_expr(metrika_expr_func, metrika_db_engine)


def test_execute_appmetrica_expr(appmetrica_expr_func, appmetrica_db_engine):
    return _test_execute_expr(appmetrica_expr_func, appmetrica_db_engine)


def test_get_columns(db_engine):
    insp = sa.inspect(db_engine)
    cols = insp.get_columns("123456")
    print(cols)
    assert isinstance(cols, list)
    assert isinstance(cols[0], dict)
    assert "name" in cols[0] and "type" in cols[0]


def test_distincts(m_expr_distinct, metrika_db_engine):
    res = metrika_db_engine.execute(m_expr_distinct)
    # print(res._cursor_description())
    data = res.fetchall()
    values = [row[0] for row in data]
    print(values)
    print(set(values))
    print(sorted(set(values)))
    assert len(data) == len(set(values))


def test_accuracy(metrika_expr_func_expr1, metrika_db_engine_with_accuracy):
    expr = metrika_expr_func_expr1(metrika_db_engine_with_accuracy)
    res = metrika_db_engine_with_accuracy.execute(expr)
    assert res.cursor.connection.accuracy == "0.1"


def test_exceptions(m_expr_distinct, metrika_db_engine):
    engine_invalid_token = sa.create_engine("metrika_api://:qwerty@/hits")
    with pytest.raises(DatabaseError) as exc_info:
        engine_invalid_token.execute(m_expr_distinct)
    assert isinstance(exc_info.value.orig, exceptions.MetrikaApiAccessDeniedException)
    assert "Invalid oauth_token" in str(exc_info.value.orig)

    expr_invalid_counter_id = sa.select(columns=[sa.column("ym:pv:users")])
    expr_invalid_counter_id = expr_invalid_counter_id.select_from(
        sa.Table("2147483647", sa.MetaData(bind=metrika_db_engine))
    )
    with pytest.raises(DatabaseError) as exc_info:
        metrika_db_engine.execute(expr_invalid_counter_id)
    assert isinstance(exc_info.value.orig, exceptions.MetrikaApiObjectNotFoundException)
    assert "Entity not found" in str(exc_info.value.orig)


def test_future_date(metrika_expr_select_date_users, metrika_db_engine):
    today = datetime.date.today()
    expr = metrika_expr_select_date_users.where(
        sa.column("ym:pv:date").between(
            datetime.datetime.fromisoformat((today - datetime.timedelta(days=2)).isoformat()),
            datetime.datetime.fromisoformat("2039-02-03"),  # date from future
        )
    )
    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    max_date = max([row["date"] for row in data])
    assert max_date == today
    assert len(data) == 3


def test_date_gt(metrika_expr_select_date_users, metrika_db_engine):
    start_dt = (datetime.date.today() - datetime.timedelta(days=5)).isoformat()
    expr = metrika_expr_select_date_users.where(sa.column("ym:pv:date") > start_dt)
    print(str(expr))

    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    assert len(data) == 5


def test_date_gte(metrika_expr_select_date_users, metrika_db_engine):
    start_dt = (datetime.date.today() - datetime.timedelta(days=5)).isoformat()
    expr = metrika_expr_select_date_users.where(sa.column("ym:pv:date") >= start_dt)
    print(str(expr))

    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    assert len(data) == 6


def test_date_gt_lt(metrika_expr_select_date_users, metrika_db_engine):
    start_dt = (datetime.date.today() - datetime.timedelta(days=5)).isoformat()
    end_dt = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()
    expr = metrika_expr_select_date_users.where(sa.column("ym:pv:date") > start_dt)
    expr = expr.where(sa.column("ym:pv:date") < end_dt)
    print(str(expr))

    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    print(data)
    assert len(data) == 2


def test_date_eq(metrika_expr_select_date_users, metrika_db_engine):
    start_dt = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    expr = metrika_expr_select_date_users.where(sa.column("ym:pv:date") == start_dt)
    print(str(expr))

    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    assert len(data) == 1


def test_date_gt_lt_by_alias(metrika_expr_select_date_users, metrika_db_engine):
    start_dt = (datetime.date.today() - datetime.timedelta(days=5)).isoformat()
    end_dt = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()
    expr = metrika_expr_select_date_users.where(sa.column("date") > start_dt)
    expr = expr.where(sa.column("date") < end_dt)
    print(str(expr))

    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    print(data)
    assert len(data) == 2


def test_calculations(metrika_sample_counter_id, metrika_db_engine):
    expr = sa.select(
        columns=[
            sa.column("ym:pv:date").label("date"),
            sa.column("ym:pv:users").label("users"),
            sa.column("ym:pv:pageviews").label("pageviews"),
            sa.column("ym:pv:pageviewsPerDay"),
            (sa.column("ym:pv:pageviews") + sa.column("ym:pv:users")).label("pv_plus_u"),
            sa.func.sum(
                sa.column("ym:pv:pageviews"),
                sa.column("ym:pv:users"),
            ).label("sum_pv_u"),
            (sa.column("ym:pv:pageviews") - sa.column("users")).label("pv_minus_u"),
            (sa.column("ym:pv:pageviews") + 1).label("pv_plus_1"),
            (sa.column("ym:pv:users") - 2).label("u_minus_2"),
            (sa.column("ym:pv:pageviews") / sa.column("ym:pv:users")).label("pv_per_u"),
            (sa.column("ym:pv:pageviews") * sa.column("ym:pv:users") - sa.column("ym:pv:pageviewsPerDay") + 14).label(
                "some_strange_expr"
            ),
        ],
    )
    expr = expr.select_from(sa.Table(metrika_sample_counter_id, sa.MetaData(bind=metrika_db_engine)))
    expr = expr.group_by(sa.column("date"))
    print(str(expr))

    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    print(data)
    assert all([(row["pageviews"] + row["users"]) == row["pv_plus_u"] for row in data])
    assert all([(row["pageviews"] + row["users"]) == row["sum_pv_u"] for row in data])
    assert all([(row["pageviews"] - row["users"]) == row["pv_minus_u"] for row in data])
    assert all([row["pv_plus_1"] == row["pageviews"] + 1 for row in data])
    assert all([row["u_minus_2"] == row["users"] - 2 for row in data])
    assert all([round(row["pageviews"] / row["users"], 2) == round(row["pv_per_u"], 2) for row in data])
    assert all(
        [
            round(row["pageviews"] * row["users"] - row["ym:pv:pageviewsPerDay"] + 14, 2)
            == round(row["some_strange_expr"], 2)
            for row in data
        ]
    )


def test_counter_id(metrika_sample_counter_id, metrika_db_engine):
    expr = sa.select(
        columns=[
            sa.column("ym:pv:date").label("date"),
            sa.column("ym:pv:users").label("users"),
            sa.column("ym:pv:counterID").label("counter_id"),
            sa.column("ym:pv:counterIDName").label("counter_name"),
        ],
    )
    expr = expr.select_from(sa.Table(metrika_sample_counter_id, sa.MetaData(bind=metrika_db_engine)))
    expr = expr.where(sa.column("counter_id").in_([metrika_sample_counter_id]))
    expr = expr.where(sa.column("counter_name").in_(["Metrica live demo"]))
    expr = expr.group_by(
        sa.column("date"),
        sa.column("counter_id"),
        sa.column("counter_name"),
    )
    print(str(expr))

    res = metrika_db_engine.execute(expr)
    data = res.fetchall()
    print(data)
    assert all([(row["counter_id"] == metrika_sample_counter_id for row in data)])


def test_multicounter_req(metrika_sample_counter_id, metrika_db_engine):
    expr = sa.select(
        columns=[
            sa.column("ym:pv:date").label("date"),
            sa.column("ym:pv:users").label("users"),
            sa.column("ym:pv:counterID").label("counter_id"),
            sa.column("ym:pv:counterIDName").label("counter_name"),
        ],
    )
    expr = expr.select_from(
        sa.Table("50514217,51341415", sa.MetaData(bind=metrika_db_engine)),
    )
    start_dt = (datetime.date.today() - datetime.timedelta(days=5)).isoformat()
    end_dt = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()
    expr = expr.where(sa.column("date").between(start_dt, end_dt))
    expr = expr.group_by(
        sa.column("date"),
        sa.column("counter_id"),
        sa.column("counter_name"),
    )
    query = parse_qs(str(expr))
    print(query)
    assert query["ids"] == ["50514217,51341415"]
