SQL:

    insert into dls_data (key, data, meta)
    values (
      'cache__effective_groups__' || %(subject)s,
      '',
      (select row_to_json(full_data) from (
        select null as timestamp,
        array_to_json(array_agg(sq_subject_groups)) as groups from (

          -- inner query data
          SELECT dls_subject.name
          FROM dls_subject
          WHERE dls_subject.id IN (
            with recursive effective_groups as (
              select group_id, member_id
              from dls_group_members_m2m
              where member_id = (select id from dls_subject where name = %(subject)s)
              union
              select edge.group_id, edge.member_id
              from dls_group_members_m2m edge
              inner join effective_groups edge_edge
              on edge.member_id = edge_edge.group_id
            )
            select group_id from effective_groups limit 128000)

        ) sq_subject_groups
      ) full_data)
    )
    on conflict (key) do update
      set
        meta = excluded.meta
      where
        dls_data.meta != excluded.meta
    ;

Note: `array_to_json(array_agg(X))` and `jsonb_agg(X)` might be equivalent.


Resulting row:

    key  | cache__effective_groups__…
    meta | {"groups": [{"name": "group:…"}, …], "timestamp": null}

`timestamp` is not currently used (but might be useful for time-based invalidation).


Unwrapping to get the “inner query data”:

    select *
    from jsonb_to_recordset(
      (select meta->'groups' from dls_data
        where key = 'cache__effective_groups__' || %(subject)s))
      as sq(name text);


Full cached get logic for a cost of one extra key lookup:

    select * from jsonb_to_recordset((
      select
      case when exists(
        select key from dls_data
        where key = 'cache__effective_groups__' || %(subject)s
      ) then (
        select meta->'groups' from dls_data  where key = 'cache__effective_groups__' || %(subject)s
      ) else (
        select jsonb_agg(sq) from (

          -- inner query data
          SELECT dls_subject.name
          FROM dls_subject
          WHERE dls_subject.id IN (
            with recursive effective_groups as (
              select group_id, member_id
              from dls_group_members_m2m
              where member_id = (select id from dls_subject where name = %(subject)s)
              union
              select edge.group_id, edge.member_id
              from dls_group_members_m2m edge
              inner join effective_groups edge_edge
              on edge.member_id = edge_edge.group_id
            )
            select group_id from effective_groups limit 128000)

        ) sq
      ) end
    )) as sq(name text, timestamp float);


Bulk SQL:

    insert into dls_data (key, data, meta) (
    select
      'cache__effective_groups__' || "user"."name" as key,
      '' as data,
      json_build_object(
        'groups', jsonb_agg(
          json_build_object('name', "group"."name")
        )
      ) as meta
      from (
          with recursive effective_groups as (
            select member_id as user_id, member_id as group_member_id, group_id
            from dls_group_members_m2m
            where member_id in (select id from dls_subject where name in %(subjects)s)
            union
            select previous_step.user_id, edge.member_id, edge.group_id
            from dls_group_members_m2m edge
            inner join effective_groups previous_step
            on edge.member_id = previous_step.group_id
          )
          select user_id, group_member_id, group_id from effective_groups limit 128000
        ) membership
        join dls_subject "user" on membership.user_id = "user".id
        join dls_subject "pre_group" on membership.group_member_id = "pre_group".id
        join dls_subject "group" on membership.group_id = "group".id
      group by "user"."name"
    ) on conflict (key) do update
      set
        meta = excluded.meta
      where
        dls_data.meta != excluded.meta;
