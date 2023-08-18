# coding: utf8
"""
...
"""

from __future__ import unicode_literals

import json

from clickhouse_sqlalchemy.parsers.jsoncompact import JSONCompactChunksParser

try:
    from io import BytesIO
except ImportError:
    from cStringIO import StringIO as BytesIO

from clickhouse_sqlalchemy_tests.testcase import BaseTestCase

from clickhouse_sqlalchemy.parsers import (
    FORMATS,
    Parts,
)


class TSVParserTestCase(BaseTestCase):

    def test_the_edge_case(self):
        """
        SQL

            select '' as value group by value with totals
            format TabSeparatedWithNamesAndTypes

        """
        chunks = [b'value\nString\n\n\n\n']
        fmt_info = FORMATS['TabSeparatedWithNamesAndTypes with totals']()
        result = fmt_info.generator(chunks)
        result = list(result)
        expected = [
            (Parts.META, [{'name': 'value', 'type': 'String'}]),
            (Parts.DATA, ['']),
            (Parts.TOTALS, ['']),
        ]
        self.assertEqual(result, expected)

    def test_the_edgiest_case(self):
        """
        SQL:

            select arrayJoin(['', 'a', 'b']) as value group by value with
            totals order by value desc format TabSeparatedWithNamesAndTypes

        """
        chunks = [b'value\nString\nb\na\n\n\n\n']
        fmt_info = FORMATS['TabSeparatedWithNamesAndTypes with totals']()
        result = fmt_info.generator(chunks)
        result = list(result)
        expected = [
            (Parts.META, [{'name': 'value', 'type': 'String'}]),
            (Parts.DATA, ['b']),
            (Parts.DATA, ['a']),
            (Parts.DATA, ['']),
            (Parts.TOTALS, ['']),
        ]
        self.assertEqual(result, expected)

    def test_the_normal_case(self):
        """
        SQL:

            select arrayJoin(['', '', '', '']) as value
            format TabSeparatedWithNamesAndTypes

        """
        chunks = [b'value\nString\n\n\n\n\n']
        fmt_info = FORMATS['TabSeparatedWithNamesAndTypes']()
        result = fmt_info.generator(chunks)
        result = list(result)
        expected = [
            (Parts.META, [{'name': 'value', 'type': 'String'}]),
            (Parts.DATA, ['']),
            (Parts.DATA, ['']),
            (Parts.DATA, ['']),
            (Parts.DATA, ['']),
        ]
        self.assertEqual(result, expected)


JSONCOMPACT_SAMPLE_DATA_SIMPLE = '''
{
\t"meta":
\t[
\t\t{
\t\t\t"name": "fielddate",
\t\t\t"type": "Nullable(String)"
\t\t},
\t\t{
\t\t\t"name": "lock_wait_times",
\t\t\t"type": "Nullable(String)"
\t\t}
\t],

\t"data":
\t[
\t\t["2019-04-01", "1"],
\t\t["2019-04-01", "2"],
\t\t["2019-04-01", "3"]
\t],

\t"totals": [null,null],

\t"extremes":
\t{
\t\t"min": ["2019-04-01","1"],
\t\t"max": ["2019-04-01","3"]
\t},

\t"rows": 3,

\t"rows_before_limit_at_least": 3,

\t"statistics":
\t{
\t\t"elapsed": 0.481989638,
\t\t"rows_read": 0,
\t\t"bytes_read": 0
\t}
}
'''.encode('utf-8')

JSONCOMPACT_SAMPLE_DATA_EMPTY = '''{
\t"meta":
\t[
\t\t{
\t\t\t"name": "test_x",
\t\t\t"type": "DateTime"
\t\t}
\t],

\t"data":
\t[

\t],

\t"rows": 0,

\t"statistics":
\t{
\t\t"elapsed": 0.000019615,
\t\t"rows_read": 0,
\t\t"bytes_read": 0
\t}
}
'''.encode('utf-8')

JSONCOMPACT_SAMPLE_WITH_TIMEOUT = '''
{
\t"meta":
\t[
\t\t{
\t\t\t"name": "res_0",
\t\t\t"type": "Nullable(String)"
\t\t}
\t],

\t"data":
\t[
\t\t["EKB"],
\t\t["RST"],
\t\t["SAM"],
\t\t["SOF"],
\t\t["SPB"]
\t],

\t"rows": 5,

\t"rows_before_limit_at_least": 105,

\t"statistics":
\t{
\t\t"elapsed": 1.193702828,
\t\t"rows_read": 96642607,
\t\t"bytes_read": 6624351318
\t}
}
Code: 159. DB::Exception: Timeout exceeded: elapsed 32.001073883 seconds, maximum: 32. (TIMEOUT_EXCEEDED) (version 22.8.16.1)
'''.encode('utf-8')

EMPTY_DATA_CHUNKS = [
    b'{\n\t"meta":\n\t[\n\t\t{\n\t\t\t"name": "res_o",\n\t\t\t"type": "String"\n\t\t}\n\t],\n\n\t"data":\n\t[\n\n\t]',
    b',\n\n\t"rows": 0,\n\n\t"statistics":\n\t{\n\t\t"elapsed": 0.000345268,\n\t\t"rows_read": 0,\n\t\t"bytes_read": 0\n\t}\n}\n',
]

SPLIT_AFTER_DATA_CHUNKS = [
    b'{\n\t"meta":\n\t[\n\t\t{\n\t\t\t"name": "res_0",\n\t\t\t"type": "Nullable(Int64)"\n\t\t}\n\t],\n\n\t"data":\n\t[\n\t\t["1"],\n\t\t["2"],\n\t\t["3"],\n\t\t["4"],\n\t\t'
    b'["5"],\n\t\t["6"],\n\t\t["7"],\n\t\t["8"],\n\t\t["9"],\n\t\t["1' + 1200 * b'0' + b'"]\n\t]',
    b',\n\n\t"rows": 10,\n\n\t"rows_before_limit_at_least": 10,\n\n\t"statistics":\n\t{\n\t\t"elapsed": 0.00487046,\n\t\t"rows_read": 10,\n\t\t"bytes_read": 13375\n\t}\n}\n',
]

DATA_WITH_TIMEOUT_CHUNKS = [
    b'{\n\t"meta":\n\t[\n\t\t{\n\t\t\t"name": "res_0",\n\t\t\t"type": "Nullable(String)"\n\t\t}\n\t],\n\n\t"data":\n\t[\n\t\t["EKB"],\n\t\t["RST"],\n\t\t["SAM"],\n\t\t["SOF"],',
    b'\n\t\t["SPB"]\n\t],\n\n\t"rows": 5,\n\n\t"rows_before_limit_at_least": 105,\n\n\t"statistics":\n\t{\n\t\t"elapsed": 1.193702828,\n\t\t"rows_read": 96642607,\n\t\t',
    b'"bytes_read": 6624351318\n\t}\n}\nCode: 159. DB::Exception: Timeout exceeded: elapsed 32.001073883 seconds, maximum: 32. (TIMEOUT_EXCEEDED) (version 22.8.16.1)\n'
]


def build_mocked_jsoncompact_result(events):
    """
    Given an iterable of events, build an entire JSONCompact data result.
    """
    mocked_result = {'data': []}
    for part, item in events:
        if isinstance(item, (bytes, str)):
            item = json.loads(item)
        if part == Parts.META:
            mocked_result['meta'] = item
        elif part == Parts.DATA:
            mocked_result['data'].append(item)
        elif part == Parts.DATACHUNK:
            mocked_result['data'].extend(item)
        elif part == Parts.TOTALS:
            mocked_result['totals'] = item
        elif part == Parts.STATS:
            mocked_result.update(item)
        else:
            raise Exception("Unknown part", part)
    return mocked_result


class JSONCompactParserTestCase(BaseTestCase):

    def _process(self, data_source_bytes, chunk_size, check_json=True):

        data_source = BytesIO(data_source_bytes)

        results = []
        chunk_inputs = []
        chunk_results = []

        def gather_results():
            results.append((
                list(chunk_inputs),
                list(chunk_results)))
            chunk_inputs[:] = []
            chunk_results[:] = []

        fmt_info = FORMATS['JSONCompact']()
        parser = fmt_info.sans_io()
        while True:
            event, data = parser.next_event()
            if event == Parts.NEED_DATA:
                gather_results()
                assert data is None

                chunk = data_source.read(chunk_size)
                if not chunk:
                    break
                parser.receive_data(chunk)
                chunk_inputs.append(chunk)

            elif event == Parts.FINISHED:
                gather_results()
                assert data is None

                chunk = data_source.read()
                if chunk:
                    chunk_inputs.append(chunk)
                    gather_results()

                break

            else:
                chunk_results.append((event, data))

        gather_results()

        data_results = [
            (part, json.loads(item) if isinstance(item, bytes) else item)
            for chunk_inputs, chunk_results in results
            for part, item in chunk_results]

        result = build_mocked_jsoncompact_result(data_results)
        if check_json:
            expected = json.loads(data_source_bytes)
            self.assertEqual(result, expected)
        else:
            expected = None
        return data_results, result, expected

    def test_jsoncompact_chunks_parser(self):
        self._process(
            data_source_bytes=JSONCOMPACT_SAMPLE_DATA_SIMPLE,
            chunk_size=123)

    def test_jsoncompact_empty_data(self):
        self._process(
            data_source_bytes=JSONCOMPACT_SAMPLE_DATA_EMPTY,
            chunk_size=128000)

    def test_jsoncompact_with_timeout(self):
        self._process(
            data_source_bytes=JSONCOMPACT_SAMPLE_WITH_TIMEOUT,
            chunk_size=128000,
            check_json=False)

    def _parse(self, chunks):
        parser = JSONCompactChunksParser()
        event = None
        chunk = None
        data = None
        for chunk in chunks:
            parser.receive_data(chunk)
            event, data = parser.next_event()
            while (event != parser.parts.NEED_DATA and
                   event != parser.parts.FINISHED):
                yield event, data
                event, data = parser.next_event()
        if event is not None and event != parser.parts.FINISHED:
            raise ValueError(
                "Unexpected last event",
                dict(event=event, data=data, chunk=chunk),
            )

    def test_empty_data_chunks(self):
        resp = list(self._parse(EMPTY_DATA_CHUNKS))
        data = resp[1]
        assert data[0] == 'PART_DATACHUNK'
        assert data[1] == []

    def test_split_after_data_chunks(self):
        resp = list(self._parse(SPLIT_AFTER_DATA_CHUNKS))

        data_parts = [part[1] for part in resp if part[0] == 'PART_DATACHUNK']
        rows = sum(len(part) for part in data_parts)
        assert rows == 10

        parts = [part[0] for part in resp]
        assert parts == ['PART_META', 'PART_DATACHUNK', 'PART_DATACHUNK', 'PART_STATS']

    def test_data_with_timeout(self):
        resp = list(self._parse(DATA_WITH_TIMEOUT_CHUNKS))
        assert resp[1] == ('PART_DATACHUNK', [['EKB'], ['RST'], ['SAM']])
        assert resp[2] == ('PART_DATACHUNK', [['SOF'], ['SPB']])
        assert resp[3] == ('PART_STATS', {'rows': 5, 'rows_before_limit_at_least': 105,
                                          'statistics': {'bytes_read': 6624351318, 'elapsed': 1.193702828,
                                                         'rows_read': 96642607}})
