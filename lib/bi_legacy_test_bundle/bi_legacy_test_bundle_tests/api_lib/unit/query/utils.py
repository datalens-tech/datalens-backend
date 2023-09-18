from typing import Sequence

from dl_query_processing.compilation.primitives import (
    JoinedFromObject, FromObject, SubqueryFromObject, FromColumn,
)


def joined_from_from_avatar_ids(
        from_ids: Sequence[str], base_avatars: dict[str, FromObject],
        cols_by_query: dict[str, Sequence[str]]
) -> JoinedFromObject:
    froms: list[FromObject] = []
    for from_id in from_ids:
        from_obj: FromObject
        if from_id in base_avatars:
            from_obj = base_avatars[from_id]
        else:
            from_obj = SubqueryFromObject(
                id=from_id, query_id=from_id, alias=from_id,
                columns=tuple(
                    FromColumn(id=col_name, name=col_name)
                    for col_name in cols_by_query[from_id]
                )
            )
        froms.append(from_obj)

    return JoinedFromObject(root_from_id=from_ids[0], froms=froms)
