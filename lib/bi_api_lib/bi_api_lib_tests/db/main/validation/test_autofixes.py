from __future__ import annotations

from http import HTTPStatus

from bi_constants.enums import AggregationFunction, CalcMode, FieldType

from bi_api_client.dsmaker.primitives import Dataset


def test_auto_aggregation_with_explicit_aggregation(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    # check aggregation and another field at same level
    # - should assume all non-aggregated fields are in GROUP BY
    cloned_field = ds.result_schema[0]
    ds.result_schema['New Field'] = ds.field(
        avatar_id=cloned_field.avatar_id, source=cloned_field.source,
        calc_mode=CalcMode.direct, aggregation=AggregationFunction.max,
    )
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    field = ds.result_schema['New Field']
    assert field.aggregation == AggregationFunction.max
    assert field.calc_mode == CalcMode.direct
    assert not field.has_auto_aggregation

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True, updates=[
        dict(
            action='update_field',
            field=dict(
                guid=field.id,
                calc_mode=CalcMode.formula.name,
                formula='100',
            )
        )
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    field = ds.result_schema['New Field']
    assert field.aggregation == AggregationFunction.max
    assert field.calc_mode == CalcMode.formula
    assert not field.source
    assert not field.avatar_id
    assert not field.has_auto_aggregation

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True, updates=[
        dict(
            action='update_field',
            field=dict(
                guid=field.id,
                formula='MIN(100)',
            )
        )
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    field = ds.result_schema['New Field']
    assert field.aggregation == AggregationFunction.none
    assert field.calc_mode == CalcMode.formula
    assert field.has_auto_aggregation

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True, updates=[
        dict(
            action='update_field',
            field=dict(
                guid=field.id,
                calc_mode=CalcMode.direct.name,
                source=cloned_field.source,
                avatar_id=cloned_field.avatar_id,
            )
        )
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    field = ds.result_schema['New Field']
    assert field.aggregation == AggregationFunction.none
    assert field.calc_mode == CalcMode.direct
    assert not field.formula
    assert not field.has_auto_aggregation


def test_auto_agg_formula_to_non_agg_direct(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    # Remove the first field so that its title (that coincides with an exisitnf db table column) can be used
    first_field = ds.result_schema[0]
    title = first_field.title
    ds = api_v1.apply_updates(dataset=ds, updates=[
        ds.result_schema[0].delete(),
    ]).dataset

    # Create aggregated formula field so that it would have the `has_auto_aggregation` property set to `True`
    ds.result_schema[title] = ds.field(formula='COUNT()')
    ds = api_v1.apply_updates(dataset=ds).dataset
    assert ds.result_schema[title].has_auto_aggregation
    assert ds.result_schema[title].type == FieldType.MEASURE

    # Change it to direct and make sure that its aggregation-related attributes are updated accordingly
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True, updates=[
        ds.result_schema[title].update(
            calc_mode=CalcMode.direct.name,
            source=first_field.source,
            avatar_id=first_field.avatar_id,
        )
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert not ds.result_schema[title].has_auto_aggregation
    assert ds.result_schema[title].type == FieldType.DIMENSION
