from bi_constants.enums import RLSSubjectType

from bi_core.rls import RLSSubject

from bi_api_lib.utils.rls import FieldRLSSerializer


def test_group_names_by_account_type():
    account_types = FieldRLSSerializer._group_names_by_account_type(
        ['@sa:123', 'user1', 'user2', '@sa:456']
    )
    assert account_types == FieldRLSSerializer.AccountGroups(
        ['user1', 'user2'],
        ['@sa:123', '@sa:456']
    )


def test_parse_sa_str():
    parsed = FieldRLSSerializer._parse_sa_account_str('@sa:123')
    assert parsed == RLSSubject(
        subject_type=RLSSubjectType.user,
        subject_id='123',
        subject_name='@sa:123'
    )
