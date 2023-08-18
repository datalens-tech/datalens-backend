# coding: utf8
'''
Recursive SQL walking utils.
'''


from __future__ import annotations

from .db import group_members_m2m, BULK_ENTITY_LIMIT


__all__ = (
    'SQL_EFFECTIVE_GROUPS_W',
    'sql_effective_groups',
    'SQL_EFFECTIVE_GROUPS_EX_W',
    'sql_effective_groups_ex',
    'SQL_EFFECTIVE_MEMBERS_W',
    'sql_effective_members',
    'SQL_EFFECTIVE_MEMBERS_EX_W',
    'sql_effective_members_ex',
)


def _sql_recurse_common(
        template, table_name='_rec_walk', edges_table=group_members_m2m,
        target_clause='%s', only_with=False, columns_s='*', entity_limit=BULK_ENTITY_LIMIT):
    edges_table = getattr(edges_table, '__tablename__', edges_table)
    result = template.format(
        table_name=table_name, edges_table=edges_table, target_clause=target_clause,
    )
    if not only_with:
        result = '{base}\nselect {columns_s} from {table_name} limit {entity_limit}'.format(
            base=result, columns_s=columns_s, table_name=table_name, entity_limit=entity_limit)
    return result


SQL_EFFECTIVE_GROUPS_W = '''
with recursive {table_name} as (
  select group_id, member_id
  from {edges_table}
  where member_id = {target_clause}
  union
  select edge.group_id, edge.member_id
  from {edges_table} edge
  inner join {table_name} edge_edge
  on edge.member_id = edge_edge.group_id
)
'''


def sql_effective_groups(table_name='effective_groups', columns_s='group_id', **kwargs):
    '''
    Make a SQL statement that takes one parameter (subject id) and returns the
    (group_id,) of recursive groups of that subject.
    (where 'member_id' is the nearest member to the group)
    '''
    return _sql_recurse_common(table_name=table_name, template=SQL_EFFECTIVE_GROUPS_W, columns_s=columns_s, **kwargs)


SQL_EFFECTIVE_GROUPS_EX_W = '''
with recursive {table_name} as (
  select group_id, member_id,
  2 as depth,
  array[group_id, member_id] as path
  from {edges_table}
  where member_id = {target_clause}
  union
  select edge.group_id, edge.member_id,
  edge_edge.depth + 1 as depth,
  array[edge.group_id] || edge_edge.path as path
  from {edges_table} edge
  inner join {table_name} edge_edge
  on edge.member_id = edge_edge.group_id
  where edge.group_id != all ( edge_edge.path )
)
'''


def sql_effective_groups_ex(table_name='effective_groups_ex', **kwargs):
    '''
    Make a SQL statement that takes one parameter (subject id) and returns the
    (group_id, member_id, depth, path) of recursive groups of that subject.
    '''
    return _sql_recurse_common(table_name=table_name, template=SQL_EFFECTIVE_GROUPS_EX_W, **kwargs)


SQL_EFFECTIVE_GROUPS_BULK_W = '''
with recursive {table_name} as (
  select member_id as user_id, member_id as group_member_id, group_id
  from {edges_table}
  where member_id in {target_clause}
  union
  select previous_step.user_id, edge.member_id, edge.group_id
  from {edges_table} edge
  inner join {table_name} previous_step
  on edge.member_id = previous_step.group_id
)
'''


def sql_effective_groups_bulk(table_name='effective_groups_bulk', **kwargs):
    '''
    Make a SQL statement that takes one parameter (tuple of subject ids) and returns the
    (user_id, group_member_id, group_id) of recursive memberships of the subjects.
    '''
    return _sql_recurse_common(table_name=table_name, template=SQL_EFFECTIVE_GROUPS_BULK_W, **kwargs)


SQL_EFFECTIVE_MEMBERS_W = '''
with recursive {table_name} as (
  select group_id, member_id
  from {edges_table}
  where group_id = {target_clause}
  union
  select edge.group_id, edge.member_id
  from {edges_table} edge
  inner join {table_name} edge_edge on edge.group_id = edge_edge.member_id
)
'''


def sql_effective_members(table_name='effective_members', columns_s='member_id', **kwargs):
    '''
    Make a SQL statement that takes one parameter (group id) and returns the
    (member_id) of recursive members of that group.
    '''
    return _sql_recurse_common(table_name=table_name, template=SQL_EFFECTIVE_MEMBERS_W, columns_s=columns_s, **kwargs)


SQL_EFFECTIVE_MEMBERS_EX_W = '''
with recursive {table_name} as (
  select group_id, member_id,
  2 as depth,
  array[group_id, member_id] as path
  from {edges_table}
  where group_id = {target_clause}
  union
  select edge.group_id, edge.member_id,
  edge_edge.depth + 1,
  edge_edge.path || array[edge.member_id]
  from {edges_table} edge
  inner join {table_name} edge_edge
  on edge.group_id = edge_edge.member_id
  where edge.member_id != all ( edge_edge.path )
)
'''


def sql_effective_members_ex(table_name='effective_members_ex', **kwargs):
    '''
    Make a SQL statement that takes one parameter (group id) and returns the
    (group_id, member_id, depth, path) of recursive members of that group
    (where `group_id` is the nearest group to that member).
    '''
    return _sql_recurse_common(table_name=table_name, template=SQL_EFFECTIVE_MEMBERS_EX_W, **kwargs)


SQL_EFFECTIVE_GROUPS_DATA_BASETPL = '''
  SELECT "group".name
  FROM dls_subject "group"
  WHERE "group".id IN (

{}

  )
'''


SQL_EFFECTIVE_GROUPS_DATA_BY_NAME = SQL_EFFECTIVE_GROUPS_DATA_BASETPL.format(
    sql_effective_groups(target_clause='(select id from dls_subject where name = %(subject)s)'))

SQL_EFFECTIVE_GROUPS_DATA_BY_ID = SQL_EFFECTIVE_GROUPS_DATA_BASETPL.format(
    sql_effective_groups(target_clause='%(subject_id)s'))
