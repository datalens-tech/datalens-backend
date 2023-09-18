import csv
import datetime
import io
import random
import string


def generate_sample_csv_data_str(row_count: int = 10000, str_cols_count: int = 3) -> str:
    output = io.StringIO()
    csv_writer = csv.writer(output, dialect=csv.excel)
    csv_writer.writerow(
        ["some int", "some float", "some date", "some datetime"] + [f"some string {i}" for i in range(str_cols_count)]
    )
    today = datetime.date.today()
    now = datetime.datetime.now()
    printable = string.printable + "АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя"
    for _i in range(row_count):
        csv_writer.writerow(
            [
                random.randint(-100, 100),
                random.randint(1, 1000) * 1.0 / random.randint(1, 1000),
                (today - datetime.timedelta(days=random.randint(0, 100))).isoformat(),
                (now - datetime.timedelta(days=random.randint(0, 100), minutes=random.randint(0, 10000))).isoformat(),
            ]
            + ["".join(random.choices(printable, k=random.randint(25, 35)))] * str_cols_count
        )
    return output.getvalue()
