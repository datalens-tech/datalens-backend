from __future__ import annotations

import pytest

from dl_constants.enums import RLSSubjectType, RLSPatternType, DataSourceRole

from dl_core.fields import BIField
from dl_core_testing.dataset_wrappers import DatasetTestWrapper


@pytest.fixture()
def dataset_for_rls(saved_ch_dataset, default_sync_usm, default_service_registry):
    dataset = saved_ch_dataset
    service_registry = default_service_registry
    us_manager = default_sync_usm
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    dsrc = ds_wrapper.get_data_source_strict(
        source_id=dataset.get_single_data_source_id(),
        role=DataSourceRole.origin,
    )
    conn_executor = service_registry.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    raw_schema = ds_wrapper.get_new_raw_schema(
        role=DataSourceRole.origin,
        conn_executor_factory=lambda: conn_executor,
    )
    for col in raw_schema:
        dataset.result_schema.fields.append(BIField.make(**dataset.create_result_schema_field(column=col)))
    return dataset


def test_rls_simple(default_sync_usm, dataset_for_rls):
    dataset = dataset_for_rls

    assert not dataset.rls.has_restrictions
    dataset.rls.add_field_restriction_for_subject(
        field_guid=dataset.result_schema.fields[0].guid,
        subject_id='qwerty',
        subject_type=RLSSubjectType.user,
        subject_name='Qwerty Uiop',
        allowed_value='QQQ',
    )

    default_sync_usm.save(dataset)

    dataset = default_sync_usm.get_by_id(entry_id=dataset.uuid)

    assert dataset.rls.has_restrictions
    allow_all_values, allow_userid, allowed_values = dataset.rls.get_field_restriction_for_subject(
        field_guid=dataset.result_schema.fields[0].guid,
        subject_id='qwerty',
        subject_type=RLSSubjectType.user,
    )
    assert not allow_all_values
    assert not allow_userid
    assert allowed_values == ['QQQ']


def test_rls_twofields(default_sync_usm, dataset_for_rls):
    dataset = dataset_for_rls

    field_a = dataset.result_schema.fields[0].guid
    field_b = dataset.result_schema.fields[1].guid

    subject_id = '1120000000037624'
    subject_name = 'robot-statbox-theta'

    assert not dataset.rls.has_restrictions
    dataset.rls.add_field_restriction_for_subject(
        subject_type=RLSSubjectType.user, subject_id=subject_id, subject_name=subject_name,
        field_guid=field_a, allowed_value='a3',
    )
    dataset.rls.add_field_restriction_for_subject(
        subject_type=RLSSubjectType.user, subject_id=subject_id, subject_name=subject_name,
        field_guid=field_a, allowed_value='a4',
    )
    dataset.rls.add_field_restriction_for_subject(
        subject_type=RLSSubjectType.user, subject_id=subject_id, subject_name=subject_name,
        field_guid=field_b, allowed_value='b4',
    )

    default_sync_usm.save(dataset)

    dataset = default_sync_usm.get_by_id(entry_id=dataset.uuid)

    assert dataset.rls.has_restrictions
    restrictions = dataset.rls.get_subject_restrictions(
        subject_type=RLSSubjectType.user, subject_id=subject_id,
    )
    expected = {
        field_a: ['a3', 'a4'],
        field_b: ['b4'],
    }
    assert restrictions == expected


# TODO?: combine with the test above into a single parametrized test.
WILDCARD_ENTRYSETS = {
    'fld1': [
        dict(allowed_value='f1_val_1_for_all', subject_type=RLSSubjectType.all),
        dict(allowed_value='f1_val_2_for_all', subject_type=RLSSubjectType.all),
        dict(pattern_type=RLSPatternType.all, subject_id='user_su_1'),
        dict(pattern_type=RLSPatternType.all, subject_id='user_su_2'),
        dict(allowed_value='f1_val_for_u3_u4', subject_id='user_3'),
        dict(allowed_value='f1_val_for_u3_u4', subject_id='user_4'),
        dict(allowed_value='f1_val_for_u5', subject_id='user_5'),
    ],
    'fld2': [
        dict(allowed_value='f2_val_1_for_all', subject_type=RLSSubjectType.all),
        dict(allowed_value='f2_val_for_su2', subject_id='user_su_2'),
        dict(allowed_value='f2_val_for_u4', subject_id='user_4'),
    ],
}
WILDCARD_EXPECTED_RESTRICTIONS = {
    'user_su_1': {'fld2': ['f2_val_1_for_all']},  # no fld1 filter
    'user_su_2': {'fld2': ['f2_val_1_for_all', 'f2_val_for_su2']},  # no fld1 filter
    'user_3': {
        'fld1': ['f1_val_1_for_all', 'f1_val_2_for_all', 'f1_val_for_u3_u4'],
        'fld2': ['f2_val_1_for_all']},
    'user_4': {
        'fld1': ['f1_val_1_for_all', 'f1_val_2_for_all', 'f1_val_for_u3_u4'],
        'fld2': ['f2_val_1_for_all', 'f2_val_for_u4']},
    'user_5': {
        'fld1': ['f1_val_1_for_all', 'f1_val_2_for_all', 'f1_val_for_u5'],
        'fld2': ['f2_val_1_for_all']},
}


def _feed_rls_entries(rls, entrysets):
    for field_guid, entries in entrysets.items():
        for entry in entries:

            subject_type = entry.get('subject_type')
            subject_id = entry.get('subject_id')
            subject_name = entry.get('subject_name') or subject_id

            if subject_type == RLSSubjectType.all:
                assert subject_id in (None, '*')
                assert subject_name in (None, '*')
                subject_id = '*'
                subject_name = '*'
            elif subject_type is None:
                subject_type = RLSSubjectType.user

            rls.add_field_restriction_for_subject(
                field_guid=field_guid,
                subject_type=subject_type,
                subject_id=subject_id,
                subject_name=subject_name,
                pattern_type=entry.get('pattern_type', RLSPatternType.value),
                allowed_value=entry.get('allowed_value'),
            )


def test_rls_wildcards(default_sync_usm, dataset_for_rls):
    entrysets = WILDCARD_ENTRYSETS
    expected_restrictions = WILDCARD_EXPECTED_RESTRICTIONS
    dataset = dataset_for_rls

    field_alias_to_guid = {
        'fld1': dataset.result_schema.fields[0].guid,
        'fld2': dataset.result_schema.fields[1].guid,
    }
    field_guid_to_alias = {val: key for key, val in field_alias_to_guid.items()}
    entrysets = {
        field_alias_to_guid[field_name]: entries
        for field_name, entries in entrysets.items()}
    assert not dataset.rls.has_restrictions
    _feed_rls_entries(dataset.rls, entrysets)

    default_sync_usm.save(dataset)
    dataset = default_sync_usm.get_by_id(entry_id=dataset.uuid)
    assert dataset.rls.has_restrictions

    restrictions = {
        user_id: dataset.rls.get_subject_restrictions(
            subject_type=RLSSubjectType.user,
            subject_id=user_id)
        for user_id in expected_restrictions.keys()}
    # Map back fields to aliases for readability
    restrictions = {
        user_id: {
            field_guid_to_alias[field_guid]: field_restrictions
            for field_guid, field_restrictions in user_restrictions.items()}
        for user_id, user_restrictions in restrictions.items()}
    assert restrictions == expected_restrictions
