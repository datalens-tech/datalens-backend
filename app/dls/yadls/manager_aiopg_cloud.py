""" An extra layer that foregoes regular synchronization and pulls subjects on-demand """
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from .cloud_manager import CloudManagerAPI
from .exceptions import NotFound
from .manager_aiopg_base import ensure_db_writing
from .manager_aiopg import DLSPG
from .utils import flatten_permissions

if TYPE_CHECKING:
    from . import db as db_base


class DLSPGC(DLSPG):

    async def _subject_names_to_api_datas_by_iam(
            self, subject_names: Sequence[str], require: bool = True,
    ) -> dict[str, dict[str, Any]]:
        prefix = 'user:'
        users = [
            name.removeprefix(prefix)
            for name in subject_names
            if name.startswith(prefix)
        ]

        if require and len(users) != len(subject_names):
            missing = [name for name in subject_names if not name.startswith(prefix)]
            raise NotFound(f"Some of the subjects were not found: {missing}")

        if not users:
            return {}
        result: dict[str, dict[str, Any]] = {}
        iam_token = (self.context or {}).get('iam_token')
        request_id = (self.context or {}).get('request_id')
        cloud_manager = CloudManagerAPI(request_id=request_id)
        async with cloud_manager:
            infos = await cloud_manager.subject_ids_to_infos(users, iam_token=iam_token)
            result.update({
                prefix + subj.id: cloud_manager.subject_info_to_dls_data(subj)
                for subj in infos.values()})
        if require:
            missing = [name for name in subject_names if name not in result]
            if missing:
                raise NotFound(f"Some of the subjects were not found: {missing}")
        return result

    async def subject_names_to_infos(self, subject_names, subject_objs=None) -> dict[str, dict[str, Any]]:
        # Check IAM first, then DB.
        # As this is rendering-API, the data must be as fresh as possible,
        # and it shouldn't raise on missing items.
        result = await self._subject_names_to_api_datas_by_iam(subject_names, require=False)
        others = [name for name in subject_names if name not in result]
        if others:
            others_res = await super().subject_names_to_infos(others, subject_objs=subject_objs)
            result.update(others_res)
        return result

    @ensure_db_writing  # maybe `ensure_db_tx`?
    async def _upsert_subject_infos(self, subject_infos: Sequence[dict[str, Any]]) -> Sequence[db_base.Subject]:
        names = [subj['name'] for subj in subject_infos]
        results: list[db_base.Subject] = []
        for name in names:
            # TODO: optimize, synchronize better.
            subj = await self._get_subject_node_async(name, expected_kind='user', autocreate=True)
            results.append(subj)
        return results

    async def _ensure_subject_objs_in_db(self, subject_names: Sequence[str]) -> Sequence[db_base.Subject]:
        # Check DB first, then IAM.
        # As this is, essentially, validation,
        # most cases should be entirely in DB.
        subject_objs = await self._get_subjects_nodes_async(subject_names, require=False)
        subject_objs_by_name = {subj.name: subj for subj in subject_objs}
        missing = [name for name in subject_names if name not in subject_objs_by_name]
        if not missing:
            return subject_objs
        iam_datas = await self._subject_names_to_api_datas_by_iam(missing, require=True)
        assert iam_datas  # nonempty missing + require => nonempty results
        iam_subject_objs = await self._upsert_subject_infos(list(iam_datas.values()))
        return list(subject_objs) + list(iam_subject_objs)

    async def _modify_node_permissions_async(
            self, node, requester, diff, autocreate_subjects=True,
            clear_all=False, default_comment=None, action=None,
            force_editable=None, extras=None, subject_objs=None,
    ) -> dict[str, Any]:

        all_subjects = list(set(
            str(subject_name)
            for perms in diff.values()
            for diff_item in flatten_permissions(perms)
            for subject_name in self.diff_item_to_subjects(diff_item)))
        if subject_objs is None:
            subject_objs = await self._ensure_subject_objs_in_db(all_subjects)

        return await super()._modify_node_permissions_async(
            node=node, requester=requester, diff=diff, autocreate_subjects=False,
            clear_all=clear_all, default_comment=default_comment, action=action,
            force_editable=force_editable, extras=extras, subject_objs=subject_objs)
