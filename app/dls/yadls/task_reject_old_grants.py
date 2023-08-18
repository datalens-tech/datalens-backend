from __future__ import annotations

import datetime
from typing import Any, Optional

from bi_utils.aio import alist

from .db_utils import db_get_one
from .manager_aiopg import DLSPG


async def reject_old_grant_requests_async(**kwargs: Any) -> None:
    async with DLSPG.create_default(manage=False) as mgr:
        return await _reject_old_grant_requests_async_i(mgr, **kwargs)


async def _reject_old_grant_requests_async_i(
        self: DLSPG,
        ttl_days: float = 360,
        now: Optional[datetime.datetime] = None
) -> None:
    if now is None:
        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    cutoff_dt = now - datetime.timedelta(days=ttl_days)

    Grant = self.Grant
    NodeConfig = self.NodeConfig
    Subject = self.Subject

    tbl = Grant.__table__
    tbl = tbl.join(NodeConfig, Grant.node_config_id == NodeConfig.id)
    tbl = tbl.join(Subject, Grant.subject_id == Subject.id)
    stmt = tbl.select(
        (Grant.state == 'pending') &
        (Grant.mtime < cutoff_dt)
    )
    stmt = stmt.order_by('id')
    stmt = stmt.with_only_columns([
        Grant.guid, Grant.perm_kind, Grant.state, Grant.mtime,
        NodeConfig.node_identifier,
        Subject.name.label('subject_name'), Subject.meta.label('subject_meta'),
    ])
    async with self.db.manage(writing=False):
        rows_resp = await self.db_conn.execute(stmt)
        # Not currently trying to memory-optimize it. Otherwise could process chunks by `limit`.
        rows = await alist(rows_resp)
        root_user = await self._get_subject_node_async('system_user:root', expected_kind='user')

    comment = f'Auto-rejected after {int(ttl_days)} days'

    for item in rows:
        grant_id = item['guid']
        try:
            async with self.db.manage(writing=True, tx=True):
                await _regular_reject_old_grant_requests_one(
                    self,
                    item,
                    comment=comment,
                    cutoff_dt=cutoff_dt,
                    requester_user=root_user,
                )
        except Exception as exc:
            self.logger.debug("Failed to reject an old grant %r: %r", grant_id, exc)


async def _regular_reject_old_grant_requests_one(
        self: DLSPG,
        item: dict,
        comment: str,
        cutoff_dt: datetime.datetime,
        requester_user: Any
) -> dict:
    grant_id = item['guid']

    Grant = self.Grant

    node = await self._get_node_async(
        node=item['node_identifier'],
        # only fetches the active, anyway.
        fetch_permissions=False,
        assume_entry=True,
        # Node works as the central lock place for permissions.
        for_update=True,
    )

    # node = await self._prefetch_node_permissions_async(node, active=None)
    # perms_orig = node.get_permissions_ext()

    grant_locked_stmt = Grant.__table__.select(Grant.guid == grant_id)
    grant_locked_resp = await self.db_conn.execute(grant_locked_stmt)
    grant_locked = await db_get_one(grant_locked_resp)
    if not (grant_locked['state'] == 'pending'):
        self.logger.debug("Grant not pending anymore: %r", item)
        return None  # type: ignore  # TODO: fix
    if not (grant_locked['mtime'] < cutoff_dt):
        self.logger.debug("Grant not stale anymore: %r", item)
        return None  # type: ignore  # TODO: fix

    self.logger.debug("Rejecting an old grant %r: %r", grant_id, item)
    modify_res = await self._modify_node_permissions_async(
        node=node,
        requester=requester_user,
        diff=dict(
            removed={
                item['perm_kind']: [
                    dict(subject=item['subject_name']),
                ],
            },
        ),
        default_comment=comment,
        force_editable=True,
    )
    self.logger.debug("Rejected an old grant %r: %r", grant_id, modify_res)
    return modify_res
