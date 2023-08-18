""" ... """

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Type, TypeVar, Union, Optional, Any

from bi_utils.utils import DotDict
from bi_core.base_models import (
    BaseAttrsDataModel,
    EntryLocation, PathEntryLocation, WorkbookEntryLocation,
)
from bi_api_commons.base_models import RequestContextInfo

if TYPE_CHECKING:
    from bi_core.us_manager.us_manager import USManagerBase
    from bi_core.us_manager.us_manager_sync import SyncUSManager

LOGGER = logging.getLogger(__name__)


_USENTRY_TV = TypeVar('_USENTRY_TV', bound='USEntry')


class USEntry:
    DataModel: ClassVar[Union[Type[BaseAttrsDataModel], None]] = None
    dir_name = None
    class_version = (0, 0)

    uuid: Optional[str] = None
    _data = None
    entry_key: Optional[EntryLocation] = None
    scope: Optional[str] = None
    type_: Optional[str] = None
    is_locked = None
    is_favorite = None
    permissions_mode = None
    initial_permissions = None
    permissions = None
    hidden: bool
    links = None

    _stored_in_db = False
    _us_resp: Optional[dict] = None
    _lock = None
    _us_manager: Optional[USManagerBase]

    @classmethod
    def _class_generator(cls, us_resp):  # type: ignore  # TODO: fix
        return cls

    # TODO FIX: Remove after removing 0007__metrika_counter_creation_date.py
    @classmethod
    def _init_from_us_resp(cls, us_resp, us_manager=None):  # type: ignore  # TODO: fix
        init_params = dict(
            uuid=us_resp['entryId'],
            data=us_resp.get('data'),
            entry_key=us_resp['key'],
            type_=us_resp['type'],
            meta=us_resp['meta'],
            is_locked=us_resp.get('isLocked'),
            is_favorite=us_resp.get('isFavorite'),
            permissions=us_resp.get('permissions') or {},
            links=us_resp.get('links') or {},
            hidden=us_resp['hidden'],
            us_manager=us_manager,
            data_strict=False,
        )
        real_cls = cls._class_generator(us_resp)  # XXX: also done in `from_db`.
        obj = real_cls(**init_params)
        obj._stored_in_db = True
        obj._us_resp = us_resp
        return obj

    @classmethod
    def create_from_dict(
            cls: Type[_USENTRY_TV],
            data_dict: Union[dict, BaseAttrsDataModel],
            ds_key: Union[EntryLocation, str, None] = None,
            type_: Optional[str] = None,
            meta: Any = None,
            *,
            us_manager: SyncUSManager,
            **kwargs: Any,
    ) -> _USENTRY_TV:
        from bi_core.us_manager.us_manager import USManagerBase

        if not isinstance(us_manager, USManagerBase):
            raise TypeError(f"us_manager must be USManagerBase, not {type(us_manager)}")

        if not (
                # dict
                isinstance(data_dict, dict) and cls.DataModel is None
                # otherwise types must match
                or type(data_dict) is cls.DataModel
        ):
            raise TypeError(f'Invalid object type for data_dict: {type(data_dict)}')

        effective_entry_key: Optional[EntryLocation]

        if ds_key is None:
            effective_entry_key = None
        elif isinstance(ds_key, str):
            effective_entry_key = PathEntryLocation(ds_key)
        elif isinstance(ds_key, EntryLocation):
            effective_entry_key = ds_key
        else:
            raise AssertionError(f"Unexpected type of entry key: {type(ds_key)}")

        obj = cls(
            data=data_dict,  # type: ignore  # TODO: fix
            entry_key=effective_entry_key,
            type_=type_,
            meta=meta,
            us_manager=us_manager,
            **kwargs
        )
        return obj

    def __init__(self, uuid: str = None, data: dict = None, entry_key: Optional[EntryLocation] = None,
                 type_: str = None,
                 meta: dict = None, is_locked: bool = None, is_favorite: bool = None,
                 permissions_mode: str = None, initial_permissions: str = None,
                 permissions: dict = None, links: dict = None, hidden: bool = False, data_strict: bool = True,
                 *, us_manager: USManagerBase):
        if entry_key is not None:
            assert isinstance(entry_key, EntryLocation), f"Unexpected type of entry key: {type(entry_key)}"

        self._us_manager = us_manager
        self.uuid = uuid
        self._data = self._load_data(data, strict=data_strict) if data is not None else None
        self.entry_key = entry_key
        self.type_ = type_
        self.meta = dict(meta or {})
        self.is_locked = is_locked
        self.is_favorite = is_favorite
        self.permissions_mode = permissions_mode
        self.initial_permissions = initial_permissions
        self.permissions = permissions
        self.links = links
        self.hidden = hidden

    @property
    def us_manager(self) -> USManagerBase:
        assert self._us_manager is not None
        return self._us_manager

    @property
    def _context(self) -> RequestContextInfo:
        return self._us_manager.bi_context  # type: ignore  # TODO: fix

    def on_updated(self):  # type: ignore  # TODO: fix
        """Post-update actions go here"""

    def _load_data(
            self, data: dict, strict: bool = True
    ) -> Union[BaseAttrsDataModel, DotDict]:
        from bi_core.us_manager.us_manager import USManagerBase

        if self.DataModel is None:
            return DotDict(data)
        elif issubclass(self.DataModel, BaseAttrsDataModel):
            if isinstance(data, self.DataModel):
                return data

            schema = USManagerBase.get_load_storage_schema(self.DataModel)
            if schema is None:
                return DotDict(data)

            data = schema.load(data)
            return data  # type: ignore  # TODO: fix
        else:
            raise TypeError(f"Unexpected data type ({type(self.DataModel)}) for entry class {type(self)}")

    def has_data(self) -> bool:
        return self._data is not None

    @property
    def data(self):  # type: ignore  # TODO: fix
        assert self._data is not None
        return self._data

    @data.setter
    def data(self, value):  # type: ignore  # TODO: fix
        self._data = value

    @property
    def raw_us_key(self) -> Optional[str]:
        """
        For backward compatibility.
        Returns private US key if workbook location is used and actual key if path location is used.
        Must be removed after total migration to workbooks.
        """
        if self._us_resp:
            raw_key = self._us_resp.get("key")
            if raw_key is not None:
                assert isinstance(raw_key, str)
                return raw_key
        return None

    @property
    def location_short_string(self) -> Optional[str]:
        loc = self.entry_key
        if loc:
            return loc.to_short_string()
        return None

    def as_dict(self, short: bool = False) -> dict:
        if not short:
            if isinstance(self.data, BaseAttrsDataModel):
                # TODO FIX: implement private fields filtering in schema/USManagerBase
                data_dict = {}
            else:
                data_dict = self.data
            data = {k: v for k, v in data_dict.items()}
        else:
            data = {
                'type': self.type_,
                'created_by': self.created_by,
                'created_at': self.created_at,
                'is_locked': self.is_locked,
            }
        ret = {
            'id': self.uuid,
            'key': self.location_short_string,
            'is_favorite': self.is_favorite,
            **data
        }
        if isinstance(self.entry_key, WorkbookEntryLocation):
            ret.update(
                workbook_id=self.entry_key.workbook_id,
                name=self.entry_key.entry_name,
                key=self.raw_us_key,
            )

        return ret

    @property
    def created_by(self):  # type: ignore  # TODO: fix
        return self._us_resp.get('createdBy') if isinstance(self._us_resp, dict) else None  # type: ignore  # TODO: fix

    @property
    def created_at(self):  # type: ignore  # TODO: fix
        return self._us_resp.get('createdAt') if isinstance(self._us_resp, dict) else None  # type: ignore  # TODO: fix

    @property
    def updated_at(self):  # type: ignore  # TODO: fix
        return self._us_resp.get('updatedAt') if isinstance(self._us_resp, dict) else None  # type: ignore  # TODO: fix

    @property
    def revision_id(self):  # type: ignore  # TODO: fix
        return self._us_resp.get('revId') if isinstance(self._us_resp, dict) else None  # type: ignore  # TODO: fix

    @property
    def folder_id(self):  # type: ignore  # TODO: fix
        return self._us_resp.get('tenantId') if isinstance(self._us_resp, dict) else None  # type: ignore  # TODO: fix

    @property
    def version(self):  # type: ignore  # TODO: fix
        if self._stored_in_db:
            return (self.meta.get('version', 0), self.meta.get('version_minor', 0))
        else:
            return self.class_version

    @version.setter
    def version(self, version: tuple[int, int]):  # type: ignore  # TODO: fix
        self.meta['version'] = version[0]
        self.meta['version_minor'] = version[1]

    def validate(self) -> None:
        """
        Fast data validation method. Should not be called in constructor.
        It's to instantiate invalid object.
        Purposes:
         * application-level validation after create/update by end user
         * making decision if entry can be used
        Think about extracting to separate layer
        """
        pass


class USMigrationEntry(USEntry):
    def __init__(
            self, uuid: str = None, data: dict = None, entry_key: Optional[EntryLocation] = None,
            type_: str = None, meta: dict = None,
            is_locked: bool = None, is_favorite: bool = None, permissions_mode: str = None,
            initial_permissions: str = None, permissions: dict = None,
            links: dict = None, hidden: bool = False, data_strict: bool = True, *, us_manager: USManagerBase,
            unversioned_data: Optional[dict[str, Any]]
    ):
        super().__init__(
            uuid=uuid, data=data, entry_key=entry_key,
            type_=type_, meta=meta, is_locked=is_locked, is_favorite=is_favorite,
            permissions_mode=permissions_mode,
            initial_permissions=initial_permissions,
            permissions=permissions,
            links=links,
            hidden=hidden,
            data_strict=data_strict,
            us_manager=us_manager,
        )

        self._unversioned_data = DotDict(unversioned_data) if unversioned_data is not None else None

    @property
    def unversioned_data(self) -> DotDict:
        return self._unversioned_data  # type: ignore  # TODO: fix
