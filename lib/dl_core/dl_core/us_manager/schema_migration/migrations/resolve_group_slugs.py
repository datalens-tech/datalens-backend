from __future__ import annotations

from datetime import datetime
import logging
import os
from typing import TYPE_CHECKING

from dl_api_commons.base_models import RequestContextInfo
from dl_constants import RLSSubjectType
from dl_core.us_manager.schema_migration.base import Migration
from dl_rls.exc import RLSError
from dl_rls.models import RLS_FAILED_USER_NAME_PREFIX
from dl_rls.subject_resolver import BaseSubjectResolver
from dl_rls.utils import is_slug
from dl_utils.aio import await_sync

if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)

VERSION = datetime(2026, 5, 16, 12, 0, 0)
NAME = "Resolve RLS group slugs to real subject ids"


def _has_unresolved_group_slug(items: list[dict]) -> bool:
    return any(
        item["subject"]["subject_type"] == RLSSubjectType.group.name
        and is_slug(
            group_id=item["subject"]["subject_id"],
            group_name=item["subject"]["subject_name"],
        )
        for item in items
    )


def _resolve_group_slugs_in_items(
    items: list[dict],
    subject_resolver: BaseSubjectResolver,
    rci: RequestContextInfo,
) -> None:
    """
    Resolve RLS group slugs in ``items`` to real group ids, mutating in place.
    For every item whose ``subject`` is a group still identified by a slug
    (``subject_id == subject_name`` stripped of the ``@group:`` prefix), the
    ``subject_resolver`` is asked for the real id. The ``subject`` dict is
    then mutated:
    * on success — ``subject_id`` is overwritten with the resolved id;
    * if the slug cannot be resolved (resolver returns ``None``) —
      ``subject_type`` becomes ``RLSSubjectType.notfound`` and ``subject_name``
      is prefixed with ``RLS_FAILED_USER_NAME_PREFIX``.
    If the resolver lacks slug-resolution support (``NotImplementedError``) the
    item is logged and left as-is; every other exception propagates so the
    migration fails and can be retried (e.g. transient service errors). A group
    that is definitively unresolvable is signalled by the resolver returning
    ``None`` (handled below as the notfound case). Within a single call each
    distinct slug is resolved at most once (cached locally). Non-group items, and
    groups that already carry a real id, are skipped.
    "In place" means the function returns ``None`` and edits the very same
    ``subject`` dicts the caller passed in; callers should not re-bind
    ``items`` and can observe the changes immediately afterwards.
    Example::
        items = [
            {"subject": {"subject_type": "group",
                         "subject_id":   "admins",
                         "subject_name": "@group:admins"}},
            {"subject": {"subject_type": "group",
                         "subject_id":   "ghosts",
                         "subject_name": "@group:ghosts"}},
            {"subject": {"subject_type": "user",
                         "subject_id":   "uid-42",
                         "subject_name": "alice"}},
        ]
        _resolve_group_slugs_in_items(items, resolver, rci)
        # items is now (assuming 'admins' resolves to 'gid-1' and
        # 'ghosts' cannot be resolved):
        # [
        #   {"subject": {"subject_type": "group",
        #                "subject_id":   "gid-1",
        #                "subject_name": "@group:admins"}},
        #   {"subject": {"subject_type": "notfound",
        #                "subject_id":   "ghosts",
        #                "subject_name": RLS_FAILED_USER_NAME_PREFIX + "@group:ghosts"}},
        #   {"subject": {"subject_type": "user",
        #                "subject_id":   "uid-42",
        #                "subject_name": "alice"}},  # untouched
        # ]
    """
    resolved_cache: dict[str, str | None] = {}
    for item in items:
        subject = item["subject"]
        if subject["subject_type"] != RLSSubjectType.group.name:
            continue
        slug = subject["subject_name"]
        if not is_slug(group_id=subject["subject_id"], group_name=slug):
            continue
        if slug in resolved_cache:
            real_id = resolved_cache[slug]
        else:
            try:
                real_id = subject_resolver.resolve_group_slug(slug, rci)
            except NotImplementedError:
                LOGGER.warning("Group slug resolution not supported by current resolver, slug: %r", slug)
                continue
            resolved_cache[slug] = real_id
        if real_id is not None:
            subject["subject_id"] = real_id
        else:
            subject["subject_type"] = RLSSubjectType.notfound.name
            subject["subject_name"] = RLS_FAILED_USER_NAME_PREFIX + slug


def migrate_resolve_group_slugs_up(
    entry: dict,
    services_registry: ServicesRegistry | None = None,
) -> dict:
    items = entry.get("data", {}).get("rls") or []
    if not _has_unresolved_group_slug(items):
        return entry
    if services_registry is None:
        msg = "RLS slug-resolution migration: no services_registry available"
        LOGGER.warning(msg)
        raise RLSError(msg)

    inst_specific_sr = services_registry.get_installation_specific_service_registry_opt()
    if inst_specific_sr is None:
        msg = "RLS slug-resolution migration: installation-specific service registry unavailable"
        LOGGER.warning(msg)
        raise RLSError(msg)
    try:
        subject_resolver = await_sync(inst_specific_sr.get_subject_resolver())
    except NotImplementedError:
        LOGGER.warning("RLS slug-resolution migration: subject resolver unavailable")
        raise
    # resolve inplace. see docstring
    _resolve_group_slugs_in_items(items, subject_resolver, services_registry.rci)
    return entry


async def migrate_resolve_group_slugs_up_async(
    entry: dict,
    services_registry: ServicesRegistry | None = None,
) -> dict:
    items = entry.get("data", {}).get("rls") or []
    if not _has_unresolved_group_slug(items):
        return entry
    if services_registry is None:
        msg = "RLS slug-resolution migration: no services_registry available"
        LOGGER.warning(msg)
        raise RLSError(msg)

    inst_specific_sr = services_registry.get_installation_specific_service_registry_opt()
    if inst_specific_sr is None:
        msg = "RLS slug-resolution migration: installation-specific service registry unavailable"
        LOGGER.warning(msg)
        raise RLSError(msg)

    try:
        subject_resolver = await inst_specific_sr.get_subject_resolver()
    except NotImplementedError:
        LOGGER.warning("RLS slug-resolution migration: subject resolver unavailable")
        raise

    rci = services_registry.rci
    await services_registry.get_compute_executor().execute(
        lambda: _resolve_group_slugs_in_items(items, subject_resolver, rci)
    )
    return entry


def migrate_resolve_group_slugs_down(
    entry: dict,
    services_registry: ServicesRegistry | None = None,
) -> dict:
    return entry


async def migrate_resolve_group_slugs_down_async(
    entry: dict,
    services_registry: ServicesRegistry | None = None,
) -> dict:
    return entry


RESOLVE_GROUP_SLUGS_MIGRATION = Migration(
    version=VERSION,
    name=NAME,
    up_function=migrate_resolve_group_slugs_up,
    down_function=migrate_resolve_group_slugs_down,
    await_up_function=migrate_resolve_group_slugs_up_async,
    await_down_function=migrate_resolve_group_slugs_down_async,
    downgrade_only=(
        os.getenv("DISABLE_RLS_MIGRATION", "1") == "1"
    ),  # TODO: BI-7466 tmp kostyl' poka iam ne oknet group permission
)
