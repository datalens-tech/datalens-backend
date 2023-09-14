import csv
import datetime
import io
import random
import string

import pytest


@pytest.fixture(scope="session")
def sample_data_str() -> str:
    output = io.StringIO()
    csv_writer = csv.writer(output, dialect=csv.excel)
    csv_writer.writerow(["some string", "some int", "some float", "some date", "some datetime"])
    today = datetime.date.today()
    now = datetime.datetime.now()
    for _i in range(10000):
        csv_writer.writerow(
            [
                "".join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(3, 1000))),
                random.randint(-100, 100),
                random.randint(1, 1000) * 1.0 / random.randint(1, 1000),
                (today - datetime.timedelta(days=random.randint(0, 100))).isoformat(),
                (now - datetime.timedelta(days=random.randint(0, 100), minutes=random.randint(0, 10000))).isoformat(),
            ]
        )
    return output.getvalue()


@pytest.fixture(scope="session")
def sample_data_bytes_utf8(sample_data_str) -> bytes:
    return sample_data_str.encode("utf-8")
