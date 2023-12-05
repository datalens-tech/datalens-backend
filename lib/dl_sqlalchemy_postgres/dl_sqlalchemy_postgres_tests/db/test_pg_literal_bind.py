import datetime

import pytest
import pytz
import sqlalchemy
import sqlalchemy.engine as sqlalchemy_engine
import sqlalchemy.orm as sqlalchemy_orm


@pytest.mark.parametrize(
    "timezone",
    (
        None,
        datetime.timezone.utc,
        pytz.timezone("America/New_York"),
    ),
    ids=["no_timezone", "utc_timezone", "ny_timezone"],
)
@pytest.mark.parametrize("microseconds", (0, 123356), ids=["no_microseconds", "with_microseconds"])
def test_pg_literal_bind_datetimes(
    timezone: datetime.timezone | None,
    microseconds: int,
    sa_engine: sqlalchemy_engine.Engine,
    sa_session: sqlalchemy_orm.Session,
):
    value = datetime.datetime(2020, 1, 1, 3, 4, 5, microseconds).replace(tzinfo=timezone)

    query = sqlalchemy.select([sqlalchemy.literal(value)])
    compiled = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))
    res_direct = list(sa_session.execute(query))
    res_literal = list(sa_session.execute(compiled))
    assert res_direct == res_literal, dict(literal_query=compiled)
