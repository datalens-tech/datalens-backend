import contextlib

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, declarative_base


class Sales(declarative_base()):
    __tablename__ = 'integration_tests_sales'

    row_id = sa.Column(name='Row ID', type_=sa.Integer, primary_key=True)
    region = sa.Column(name='Region', type_=sa.String)
    category = sa.Column(name='Category', type_=sa.String)
    sales = sa.Column(name='Sales', type_=sa.Float)
    order_date = sa.Column(name='Order Date', type_=sa.Date)


@contextlib.contextmanager
def create_session(postgres_secrets):
    engine = create_integration_test_pg_engine(postgres_secrets)
    metadata = Sales.metadata

    metadata.drop_all(bind=engine, checkfirst=True)
    metadata.create_all(bind=engine, checkfirst=True)

    session = Session(bind=engine)

    try:
        yield session
    except Exception:
        session.rollback()
    finally:
        session.close()


def create_integration_test_pg_engine(postgres_secrets):
    host_fqdn = postgres_secrets['host_fqdn']  # This is used to circumvent IPv4 shenanigans in public cloud
    # Although we also need public cloud IPv4 host address to simulate queries to 'foreign' host in RQE scenarios
    host = postgres_secrets['host'] if host_fqdn is None else host_fqdn  # Here we fall back to IPv4 if FQDN is not set
    conn_settings = {
        'host': host,
        'port': postgres_secrets['port'],
        'database': postgres_secrets['database'],
        'username': postgres_secrets['username'],
        'password': postgres_secrets['password'],
        'drivername': 'postgresql'
    }
    url = URL.create(**conn_settings)
    return create_engine(url)


def upload_data_from_df(
        pandas_df,
        integration_tests_postgres_db
):
    sales = [
        Sales(
            row_id=index,
            region=row['Region'],
            category=row['Category'],
            sales=row['Sales'],
            order_date=row['Order Date']
        ) for index, row in pandas_df.iterrows()
    ]
    with create_session(integration_tests_postgres_db) as session:
        session.bulk_save_objects(objects=sales)
        session.commit()
