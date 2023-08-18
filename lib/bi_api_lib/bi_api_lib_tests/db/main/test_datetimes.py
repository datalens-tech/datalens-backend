"""
Various datetime / timezone related behaviors tests
(to see and approve/rollback the difference when anything changes)

Common note:
in 'Europe/Moscow' timezone,
at '2010-10-31T00:00:00' the tzoffset is '+04:00' (daylight saving time),
at '2010-10-31T05:00:00' the tzoffset is '+03:00',
local datetimes between '2010-10-31T02:00:00' and '2010-10-31T03:00:00' are ambiguous.

`...MostlyAwareVersion` is one of the proposed changes, which includes split into naive/aware.
"""

from __future__ import annotations

import datetime
from typing import Any, List, Optional, Tuple, Union

import attr
import pytest

from bi_constants.enums import CreateDSFrom

from bi_api_client.dsmaker.primitives import Dataset

from bi_api_lib_tests.utils import get_result_schema


def dt_to_ts(dt: datetime) -> float:
    """
    Helper to make a UTC timestamp of a datetime object.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return dt.timestamp()


def dt_to_api_ts(dt_iso: str) -> int:
    """
    Helper to make a data-api response timestamp
    (seconds-precision stringified int)
    of datetime args.
    """
    try:
        dt = datetime.datetime.strptime(dt_iso, '%Y-%m-%d')
    except ValueError:
        dt_iso = dt_iso.replace('T', ' ').replace('Z', '')
        dt = datetime.datetime.strptime(dt_iso, '%Y-%m-%d %H:%M:%S')
    ts = dt_to_ts(dt)
    return str(int(ts))


@attr.s
class Exp:
    name: str = attr.ib()
    formula: Optional[str] = attr.ib(default=None)
    expected_type: Optional[str] = attr.ib(default=None)
    expected_data: Optional[List[Any]] = attr.ib(default=None)


class KeepMarker:
    """ marker-class """


KEEP = KeepMarker()


@attr.s
class ExpMod(Exp):
    name: str = attr.ib()
    formula: Union[KeepMarker, Optional[str]] = attr.ib(default=KEEP)
    expected_type: Union[KeepMarker, Optional[str]] = attr.ib(default=KEEP)
    expected_data: Union[KeepMarker, Optional[List[Any]]] = attr.ib(default=KEEP)


@attr.s
class ExpResult:
    type: Optional[str] = attr.ib(default=None)
    data: Optional[List[Any]] = attr.ib(default=None)

    @classmethod
    def from_expr(cls, expr: Exp):
        return cls(type=expr.expected_type, data=expr.expected_data)


def massevolve(base_exprs: Tuple[Exp, ...], *overrides: ExpMod):
    """
    Usage:

        exprs = massevolve(
            BaseClass.exprs,
            some_field=dict(expected_type=None, expected_data=[-1]),
        )
    """
    overrides_map = {
        expr_mod.name: {
            key: value
            for key, value in attr.asdict(expr_mod).items()
            if not isinstance(value, KeepMarker)}
        for expr_mod in overrides}
    result = tuple(
        attr.evolve(
            base_expr,
            **overrides_map.pop(base_expr.name, {}))
        for base_expr in base_exprs)
    if overrides_map:
        raise Exception("Not all evolves were involved", list(overrides_map.keys()))
    return result


class BaseCHSelectTest:

    ch_query: str = 'select 1'
    exprs: Tuple[Exp] = ()

    @staticmethod
    def make_ch_subselect_dataset(query, conn_id, request, client, api_v1):
        """ Given a CH SQL query, make a dataset that uses the query as a table """
        ds = Dataset()
        ds.sources['source_1'] = ds.source(
            connection_id=conn_id,
            source_type=CreateDSFrom.CH_SUBSELECT,
            parameters=dict(
                subsql=query,
            ))
        ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
        ds = api_v1.apply_updates(dataset=ds).dataset
        ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

        ds_id = ds.id

        def teardown(ds_id=ds_id, client=client):
            client.delete('/api/v1/datasets/{}'.format(ds_id))

        request.addfinalizer(teardown)

        return ds_id

    @classmethod
    def make_ch_subselect_result(
            cls, query, exprs, conn_id,
            request, client, api_v1, data_api_v1):

        ds_id = cls.make_ch_subselect_dataset(
            query=query, conn_id=conn_id,
            request=request, client=client, api_v1=api_v1)
        result_schema = get_result_schema(client, ds_id)

        formula_expr_fields = [
            dict(
                formula=expr.formula,
                id=expr.name,
            )
            for expr in exprs
            if expr.formula is not None]
        updates = [
            dict(action='add', field=dict(
                calc_mode='formula',
                source='',
                formula=fld['formula'],
                type='DIMENSION',
                description='',
                title=fld['id'],
                hidden=False,
                aggregation='none',
                guid=fld['id'],
                datasetId=ds_id,
            ))
            for fld in formula_expr_fields]

        body = dict(
            updates=updates,
            where=[],
            columns=(
                [col['guid'] for col in result_schema] +
                [fld['id'] for fld in formula_expr_fields] +
                []),
        )

        response = data_api_v1.get_response_for_dataset_result(
            dataset_id=ds_id, raw_body=body)
        assert response.status_code == 200, response.json
        res_body = response.json
        res_data = res_body['result']['data']

        res_schema = res_data['Type'][-1][-1]
        titles = [title for title, _ in res_schema]

        def _unwrap_type(typestruct):
            assert len(typestruct) == 2
            wrapper, typestruct = typestruct
            assert wrapper == 'OptionalType'

            assert len(typestruct) == 2
            wrapper, typestruct = typestruct
            assert wrapper == 'DataType'

            assert isinstance(typestruct, str)
            return typestruct

        types = {
            title: _unwrap_type(typestruct)
            for title, typestruct in res_schema}

        data_lists = res_data['Data']
        data = [dict(zip(titles, row)) for row in data_lists]

        expr_results = {
            title: ExpResult(
                type=types[title],
                data=[row[title] for row in data])
            for title in titles}

        return dict(
            expr_results=expr_results,
            titles=titles,
            types=types,
            data=data,
        )

    def test_result(self, request, client, api_v1, data_api_v1, ch_subselectable_connection_id):
        conn_id = ch_subselectable_connection_id

        result = self.make_ch_subselect_result(
            query=self.ch_query, exprs=self.exprs,
            conn_id=conn_id, request=request, client=client,
            api_v1=api_v1, data_api_v1=data_api_v1,)
        expected_expr_results = {
            # TODO: support `None` in expected_type / expected_data
            # (mangle `expr_results` accordingly)
            expr.name: ExpResult.from_expr(expr)
            for expr in self.exprs}
        assert result['expr_results'] == expected_expr_results


class TestCHSelectTest(BaseCHSelectTest):
    """ Test the tooling """
    ch_query = '''
        select '1' as a, 2 as b, 3.3 as c
    '''
    exprs = (
        # `name, formula, expected_type, expected_data`
        Exp('a', None, 'String', ['1']),
        Exp('b', None, 'Int64', ['2']),
        Exp('c', None, 'Double', ['3.3']),
        Exp('e01', '[b] + [c]', 'Double', ['5.3']),
    )


class TestCHDatetimesPassthrough(BaseCHSelectTest):
    """ Simple 'select some datetime values' case """
    ch_query = '''
        select
            '2010-10-31 00:00:00' as dt_str,
            toDateTime(dt_str) as dt_sys,
            toDateTime(dt_str, 'Europe/Moscow') as dt_msk,
            toDateTime(dt_str, 'America/New_York') as dt_nyc,
            toDateTime(dt_str) as dt_utc
    '''
    exprs = (
        # `name, formula, expected_type, expected_data`
        Exp('dt_str', None, 'String', ['2010-10-31 00:00:00']),
        Exp('dt_sys', None, 'GenericDatetime', ['2010-10-31T00:00:00']),
        Exp('dt_msk', None, 'GenericDatetime', ['2010-10-31T00:00:00']),
        Exp('dt_nyc', None, 'GenericDatetime', ['2010-10-31T00:00:00']),
        Exp('dt_utc', None, 'GenericDatetime', ['2010-10-31T00:00:00']),
    )


@pytest.mark.skip('proposed changes')
class TestCHDatetimesPassthroughMostlyAwareVersion(TestCHDatetimesPassthrough):
    exprs = massevolve(
        TestCHDatetimesPassthrough.exprs,
        # `name, formula, expected_type, expected_data`
        ExpMod('dt_sys', expected_type='DatetimeTZNaive'),
        ExpMod('dt_msk', KEEP, 'DatetimeTZAware', '2010-10-31T00:00:00+04:00'),
        ExpMod('dt_nyc', KEEP, 'DatetimeTZAware', '2010-10-31T00:00:00-04:00'),
        ExpMod('dt_utc', KEEP, 'DatetimeTZAware', '2010-10-31T00:00:00+00:00'),
    )


class TestCHCENaiveDatetimesPassthrough(BaseCHSelectTest):
    """ ClickHouse -> CompEng passhtough of kind-of-'naive' datetimes """
    ch_query = '''
        select
            '2010-10-31 00:00:00' as dt_str,
            toDateTime(dt_str) as dt_sys
    '''
    exprs = (
        # `name, formula, expected_type, expected_data`
        Exp('dt_str', None, 'String', ['2010-10-31 00:00:00']),
        Exp('dt_sys', None,
            # as-is / naive / implicit UTC
            'GenericDatetime', ['2010-10-31T00:00:00']),

        Exp('some_rank', 'rank(min([dt_str]))', 'Int64', ['1']),

        Exp('dt_over_pg', '[dt_sys] + [some_rank] * 0',
            'GenericDatetime', ['2010-10-31T00:00:00']),

        Exp('dt_over_pg_str', 'str([dt_over_pg])',
            'String', ['2010-10-31 00:00:00']),

        Exp('dt_over_pg_int', 'int([dt_over_pg])',
            'Int64', [dt_to_api_ts('2010-10-31')]),
    )


@pytest.mark.skip('proposed changes')
class TestCHCENaiveDatetimesPassthroughMostlyAwareVersion(TestCHCENaiveDatetimesPassthrough):
    exprs = massevolve(
        TestCHCENaiveDatetimesPassthrough.exprs,
        # `name, formula, expected_type, expected_data`
        ExpMod('dt_sys', expected_type='DatetimeTZNaive'),
        ExpMod('dt_over_pg', expected_type='DatetimeTZNaive'),
    )


class TestCHCEAwareDatetimesPassthrough(BaseCHSelectTest):
    ch_query = '''
        select
            '2010-10-31 00:00:00' as dt_str,
            toDateTime(dt_str, 'Europe/Moscow') as dt_msk
    '''
    exprs = (
        # `name, formula, expected_type, expected_data`
        Exp('dt_str', None, 'String', ['2010-10-31 00:00:00']),
        Exp('dt_msk', None, 'GenericDatetime', ['2010-10-30T20:00:00']),
        Exp('some_rank', 'rank(min([dt_str]))', 'Int64', ['1']),

        Exp('dt_over_pg', '[dt_msk] + [some_rank] * 0',
            'GenericDatetime', ['2010-10-30T20:00:00']),

        Exp('dt_over_pg_str', 'str([dt_over_pg])',
            'String', ['2010-10-30 20:00:00']),

        Exp('dt_over_pg_int', 'int([dt_over_pg])',
            'Int64', [dt_to_api_ts('2010-10-30 20:00:00')]),
    )


@pytest.mark.skip('proposed changes')
class TestCHCEAwareDatetimesPassthroughMostlyAwareVersion(TestCHCEAwareDatetimesPassthrough):
    exprs = massevolve(
        TestCHCEAwareDatetimesPassthrough.exprs,
        # `name, formula, expected_type, expected_data`
        ExpMod('dt_msk', KEEP, 'DatetimeTZAware', ['2010-10-31T00:00:00+04:00']),
        ExpMod('dt_over_pg', KEEP, 'DatetimeTZAware', ['2010-10-31T00:00:00+04:00']),
        # pg `timestamptz` stringification logic.
        ExpMod('dt_over_pg_str', expected_data=['2010-10-30 20:00:00+00']),
    )


class TestNaiveDatetimeOverFormula(BaseCHSelectTest):
    """ Make a tz-naive datetime through a formula and do some processing """
    ch_query = "select '2010-10-31 00:00:00' as dt_str"
    exprs = (
        # `name, formula, expected_type, expected_data`
        Exp('dt_str', None, 'String', ['2010-10-31 00:00:00']),

        Exp('dtc_some', 'datetime([dt_str])',
            'GenericDatetime', ['2010-10-31T00:00:00']),
        Exp('dtc_some_str', 'str([dtc_some])',
            'String', ['2010-10-31 00:00:00']),

        Exp('dtc_some_shifted', "dateadd([dtc_some], 'day', 1)",
            'GenericDatetime', ['2010-11-01T00:00:00']),
        Exp('dtc_some_shifted_str', 'str([dtc_some_shifted])',
            'String', ['2010-11-01 00:00:00']),
        Exp('dtc_some_shifted_ts', 'int([dtc_some_shifted])',
            'Int64', [dt_to_api_ts('2010-11-01')]),

        Exp('dtc_some_intshifted', '[dtc_some] + 1', 'GenericDatetime',
            ['2010-11-01T00:00:00']),
        Exp('dtc_some_intshifted_ts', 'int([dtc_some_intshifted])',
            'Int64', [dt_to_api_ts('2010-11-01')]),
    )


@pytest.mark.skip('proposed changes')
class TestNaiveDatetimeOverFormulaMostlyAwareVersion(TestNaiveDatetimeOverFormula):
    # TODO?: forbid the `int()` of tz-naive datetimes.
    exprs = massevolve(
        TestNaiveDatetimeOverFormula.exprs,
        # `name, formula, expected_type, expected_data`
        ExpMod('dtc_some', expected_type='DatetimeTZNaive'),
        ExpMod('dtc_some_shifted', expected_type='DatetimeTZNaive'),
        ExpMod('dtc_some_intshifted', expected_type='DatetimeTZNaive'),
    )


class TestAwareDatetimeOverFormula(BaseCHSelectTest):
    """ Make a tz-aware datetime through a formula and do some processing """
    ch_query = "select '2010-10-31 00:00:00' as dt_str"
    exprs = (
        # `name, formula, expected_type, expected_data`
        Exp('dt_str', None, 'String', ['2010-10-31 00:00:00']),

        Exp('dtc_msk', "datetime([dt_str], 'Europe/Moscow')",
            'GenericDatetime', ['2010-10-31T00:00:00']),

        Exp('dtc_msk_str', 'str([dtc_msk])',
            'String', ['2010-10-31 00:00:00']),

        # Note: CH adds 25 hours when using 'interval 1 day' over shift
        # from dailyght-saving-time (+04:00 here) to standard-time (+03:00 here).
        Exp('dtc_msk_shifted', "dateadd([dtc_msk], 'day', 1)",
            'GenericDatetime', ['2010-11-01T00:00:00']),
        Exp('dtc_msk_shifted_str', 'str([dtc_msk_shifted])',
            'String', ['2010-11-01 00:00:00']),
        Exp('dtc_msk_shifted_ts', 'int([dtc_msk_shifted])',
            'Int64', [dt_to_api_ts('2010-10-31 21:00:00')]),

        # 24-hours shifted (datetime + int in CH)
        Exp('dtc_msk_intshifted', '[dtc_msk] + 1',
            'GenericDatetime', ['2010-10-31T23:00:00']),
        Exp('dtc_msk_intshifted_ts', 'int([dtc_msk_intshifted])',
            'Int64', [dt_to_api_ts('2010-10-31 20:00:00')]),
    )


@pytest.mark.skip('proposed changes')
class TestAwareDatetimeOverFormulaMostlyAwareVersion(TestAwareDatetimeOverFormula):
    exprs = massevolve(
        TestAwareDatetimeOverFormula.exprs,
        # `name, formula, expected_type, expected_data`
        ExpMod('dtc_msk', KEEP,
               'DatetimeTZAware', ['2010-10-31T00:00:00+04:00']),

        # +25 hours shift, looks like +24 hours shift in naive time
        ExpMod('dtc_msk_shifted', KEEP,
               'DatetimeTZAware', ['2010-11-01T00:00:00+03:00']),
        # CH limitation, currently.
        ExpMod('dtc_msk_shifted_str', expected_data=['2010-11-01 00:00:00']),
        ExpMod('dtc_msk_shifted_ts', expected_data=[dt_to_api_ts('2010-10-31 21:00:00')]),

        # +24 hours shift, but looks like +23 hours shift in naive time.
        ExpMod('dtc_msk_intshifted', KEEP,
               'DatetimeTZAware', ['2010-10-31T23:00:00+03:00']),
        ExpMod('dtc_msk_intshifted_ts', expected_data=[dt_to_api_ts('2010-10-31 20:00:00')]),
    )


class TestAwareDatetimeMultipleTimezones(BaseCHSelectTest):
    ch_query = "select '2010-10-31 00:00:00' as dt_str"
    exprs = (
        # `name, formula, expected_type, expected_data`
        Exp('dt_str', None, 'String', ['2010-10-31 00:00:00']),
        # For convenient overriding:
        Exp('dtc_str', '[dt_str]', 'String', ['2010-10-31 00:00:00']),

        # Yep, all result values look exactly the same now.
        Exp('dtc_msk', "datetime([dtc_str], 'Europe/Moscow')",
            'GenericDatetime', ['2010-10-31T00:00:00']),

        # Note: using `dtc_msk` as base.
        Exp('dtc_utc', "datetime([dtc_msk], 'Etc/UTC')",
            'GenericDatetime', ['2010-10-30T20:00:00']),  # +00:00

        Exp('dtc_nyc', "datetime([dtc_msk], 'America/New_York')",
            'GenericDatetime', ['2010-10-30T16:00:00']),
        Exp('dtc_nyc_ts', 'int([dtc_nyc])',
            'Int64', [dt_to_api_ts('2010-10-30 20:00:00')]),

        Exp('dtc_pch', "datetime([dtc_msk], 'Pacific/Chatham')",
            'GenericDatetime', ['2010-10-31T09:45:00']),
        Exp('dtc_pch_ts', 'int([dtc_pch])',
            'Int64', [dt_to_api_ts('2010-10-30 20:00:00')]),
    )


@pytest.mark.skip('proposed changes')
class TestAwareDatetimeMultipleTimezonesMostlyAwareVersion(TestAwareDatetimeMultipleTimezones):
    exprs = massevolve(
        TestAwareDatetimeMultipleTimezones.exprs,
        # `name, formula, expected_type, expected_data`
        # Semantics: 'interpret naive string at ...' (with some ambiguity)
        ExpMod('dtc_msk', KEEP, 'DatetimeTZAware', ['2010-10-31T00:00:00+04:00']),
        # `dtc_msk value at UTC`
        ExpMod('dtc_utc', KEEP, 'DatetimeTZAware', ['2010-10-30T20:00:00+00:00']),
        ExpMod('dtc_nyc', KEEP, 'DatetimeTZAware', ['2010-10-30T16:00:00-04:00']),
        ExpMod('dtc_nyc_ts', expected_data=[dt_to_api_ts('2010-10-30 20:00:00')]),
        ExpMod('dtc_pch', KEEP, 'DatetimeTZAware', ['2010-10-31T09:45:00+13:45']),
        ExpMod('dtc_pch_ts', expected_data=[dt_to_api_ts('2010-10-30 20:00:00')]),
    )


class TestAwareDatetimeMultipleTimezonesOverCompeng(BaseCHSelectTest):
    """ ... Essentially doubles as a postgres test """
    ch_query = "select '2010-10-31 00:00:00' as dt_str"
    exprs = massevolve(
        TestAwareDatetimeMultipleTimezones.exprs,
        # Mimic the CH behavior (?),
        # so all result values still look exactly the same,
        # so reuse them.
        ExpMod('dtc_str', formula="str([dt_str] + if(false, str([some_rank]), ''))"),
        ExpMod('dtc_msk', expected_data=['2010-10-30T20:00:00']),
        ExpMod('dtc_nyc_ts', expected_data=[dt_to_api_ts('2010-10-30 16:00:00')]),
        ExpMod('dtc_pch_ts', expected_data=[dt_to_api_ts('2010-10-31 09:45:00')]),
    ) + (
        # Required addition.
        Exp('some_rank', 'rank(min([dt_str]))', 'Int64', ['1']),
    )


@pytest.mark.skip('proposed changes')
class TestAwareDatetimeMultipleTimezonesOverCompengMostlyAwareVersion(
        TestAwareDatetimeMultipleTimezonesMostlyAwareVersion):
    # Should match the CH behavior when going over compeng.
    exprs = massevolve(
        TestAwareDatetimeMultipleTimezonesMostlyAwareVersion.exprs,
        ExpMod('dtc_str', formula="str([dt_str] + if(false, str([some_rank]), ''))")
    ) + (
        # Required addition.
        Exp('some_rank', 'rank(min([dt_str]))', 'Int64', ['1']),
    )


class TestAwareDatetimeByFunc(BaseCHSelectTest):
    """
    Test getting a DatetimeTZ result using a currently undocumented (semi-internal) formula function.
    """
    ch_query = "select '2010-10-31 00:00:00' as dt_str"
    exprs = (
        Exp('dt_str', None, 'String', ['2010-10-31 00:00:00']),
        Exp('dttzc_msk', "datetimetz('2010-10-31 00:00:00', 'Europe/Moscow')",
            'DatetimeTZ', ['2010-10-31T00:00:00+04:00']),
        Exp('dttz_msk', "datetimetz([dt_str], 'Europe/Moscow')",
            'DatetimeTZ', ['2010-10-31T00:00:00+04:00']),
    )


# TODO later: chyt direct / chyt materialized tests
