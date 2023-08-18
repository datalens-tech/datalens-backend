# https://charts-beta.yandex-team.ru/gateway/v1/navigation?path=Users%2F
# https://charts.yandex-team.ru/gateway/v1/navigation?path=Users%2F

from __future__ import annotations

import json
import test_integrated as m

# items = json.load(open('charts_beta_personal_folders.json'))
items = json.load(open('charts_prod_user_dirs.json'))


def make_body_ex():
    return dict(diff=dict(
        added=dict(acl_view=[dict(
            subject='group:962',
            comment='Изменение прав по-умолчанию задним числом https://st.yandex-team.ru/BI-334',
        )]),
        removed=dict(acl_view=[dict(
            subject='system_group:all_active_users',
        )]),
    ))


def make_body():
    return dict(diff=dict(
        added=dict(),
        removed=dict(),
        modified=dict(acl_view=[dict(
            subject='system_group:all_active_users',
            comment='Изменение прав по-умолчанию задним числом; https://st.yandex-team.ru/BI-334',
            new=dict(
                subject='group:962'
            )
        )]),
    ))


resps = [
    m.req(
        '/nodes/all/{}/permissions'.format(item['entryId']),
        requester='system_user:root',
        method='PATCH',
        json=make_body(),
        _rfs=False)
    for item in items]
print(resps[0].json())
print(resps)

