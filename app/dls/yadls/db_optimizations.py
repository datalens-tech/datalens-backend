# coding: utf8
"""
...
"""


from __future__ import annotations

from . import db_walkers


QUERY_JOINER = (
    "\n"
    "union all"
    "\n"
)

CHECK_PERMS_SQL_NODE = """
select 'node' as type_, row_to_json(sq_node) as data_ from (
  SELECT
    dls_node_config.id as node_config_id, dls_node_config.node_identifier as identifier,
    dls_node_config.scope, dls_node_config.realm,
    dls_node_config.ctime, dls_node_config.mtime, dls_node_config.meta
  FROM dls_node_config
  WHERE dls_node_config.node_identifier = %(node)s
  LIMIT 2
) sq_node
"""

CHECK_PERMS_SQL_SUBJECT = """
select 'subject' as type_, row_to_json(sq_subject) as data_ from (
  SELECT
    dls_subject.id, dls_subject.meta, dls_subject.ctime, dls_subject.mtime,
    dls_subject.kind, dls_subject.name, dls_subject.node_config_id,
    dls_subject.active, dls_subject.source
  FROM dls_subject
  WHERE dls_subject.name = %(subject)s
   LIMIT 2
) sq_subject
"""


CHECK_PERMS_SQL_NODE_PERM = """
select 'node_perm' as type_, row_to_json(sq_node_perm) as data_ from (
  SELECT
    dls_subject.id, dls_subject.kind, dls_subject.name,
    dls_grant.node_config_id, dls_grant.perm_kind, dls_grant.active AS grant_active, dls_grant.state AS grant_state,
    dls_subject.meta, dls_subject.ctime, dls_subject.mtime,
    dls_grant.meta AS grant_meta, dls_grant.ctime AS grant_ctime, dls_grant.mtime AS grant_mtime
  FROM dls_subject
  JOIN dls_grant ON dls_subject.id = dls_grant.subject_id
  JOIN dls_node_config ON dls_node_config.id = dls_grant.node_config_id
  WHERE dls_node_config.node_identifier = %(node)s
    AND dls_grant.active = true
) sq_node_perm
"""


CHECK_PERMS_SQL_SUBJECT_GROUPS = """
select 'subject_group' as type_, row_to_json(sq_subject_groups) as data_ from (
{}
) sq_subject_groups
""".format(db_walkers.SQL_EFFECTIVE_GROUPS_DATA_BY_NAME)


CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE_BASETPL = """
select 'subject_group' as type_, row_to_json(groups) as data_
from jsonb_to_recordset((
  select
  case when exists(
    select key from dls_data
    where key = 'cache__effective_groups__' || %(subject)s
  ) then (
    select meta->'groups' from dls_data  where key = 'cache__effective_groups__' || %(subject)s
  ) else (
    select jsonb_agg(sq) from (
{}
    ) sq
  ) end
)) as groups(name text);
"""


CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE = CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE_BASETPL.format(
    db_walkers.SQL_EFFECTIVE_GROUPS_DATA_BY_NAME)
CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE_BY_ID = CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE_BASETPL.format(
    db_walkers.SQL_EFFECTIVE_GROUPS_DATA_BY_ID)


CHECK_PERMS_SQL = QUERY_JOINER.join((
    CHECK_PERMS_SQL_NODE,
    CHECK_PERMS_SQL_SUBJECT,
    CHECK_PERMS_SQL_NODE_PERM,
    CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE,
))
