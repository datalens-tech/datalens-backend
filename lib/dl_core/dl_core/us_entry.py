""" ... """

from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
    Type,
    TypeVar,
    Union,
)

from dl_api_commons.base_models import RequestContextInfo
from dl_core.base_models import (
    BaseAttrsDataModel,
    EntryLocation,
    PathEntryLocation,
    WorkbookEntryLocation,
)
from dl_utils.utils import DotDict


if TYPE_CHECKING:
    from dl_core.us_manager.us_manager import USManagerBase
    from dl_core.us_manager.us_manager_sync import SyncUSManager

LOGGER = logging.getLogger(__name__)


_USENTRY_TV = TypeVar("_USENTRY_TV", bound="USEntry")


class USEntry:
    DataModel: ClassVar[Union[Type[BaseAttrsDataModel], None]] = None
    dir_name = None

    uuid: Optional[str] = None
    _data = None
    entry_key: Optional[EntryLocation] = None
    scope: Optional[str] = None
    type_: Optional[str] = None
    is_locked: Optional[bool] = None
    is_favorite: Optional[bool] = None
    permissions_mode: Optional[str] = None
    initial_permissions: Optional[str] = None
    permissions: Optional[dict[str, bool]] = None
    hidden: bool
    links: Optional[dict] = None
    migrated: bool = False

    _stored_in_db: bool = False
    _us_resp: Optional[dict] = None
    _lock: Optional[str] = None
    _us_manager: Optional[USManagerBase]

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
        from dl_core.us_manager.us_manager import USManagerBase

        if not isinstance(us_manager, USManagerBase):
            raise TypeError(f"us_manager must be USManagerBase, not {type(us_manager)}")

        if not (
            # dict
            isinstance(data_dict, dict)
            and cls.DataModel is None
            # otherwise types must match
            or type(data_dict) is cls.DataModel
        ):
            raise TypeError(f"Invalid object type for data_dict: {type(data_dict)}")

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
            **kwargs,
        )
        return obj

    def __init__(
        self,
        uuid: Optional[str] = None,
        data: Optional[dict] = None,
        entry_key: Optional[EntryLocation] = None,
        type_: Optional[str] = None,
        meta: Optional[dict] = None,
        is_locked: Optional[bool] = None,
        is_favorite: Optional[bool] = None,
        permissions_mode: Optional[str] = None,
        initial_permissions: Optional[str] = None,
        permissions: Optional[dict[str, bool]] = None,
        links: Optional[dict] = None,
        hidden: bool = False,
        data_strict: bool = True,
        migrated: bool = False,
        *,
        us_manager: USManagerBase,
    ):
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
        self.migrated = migrated

    @property
    def us_manager(self) -> USManagerBase:
        assert self._us_manager is not None
        return self._us_manager

    @property
    def stored_in_db(self) -> bool:
        return self._stored_in_db

    @stored_in_db.setter
    def stored_in_db(self, value: bool) -> None:
        self._stored_in_db = value

    @property
    def lock(self) -> Optional[str]:
        return self._lock

    @lock.setter
    def lock(self, value: Optional[str]) -> None:
        self._lock = value

    @property
    def _context(self) -> RequestContextInfo:
        assert self._us_manager is not None
        return self._us_manager.bi_context

    def on_updated(self) -> None:
        """Post-update actions go here"""

    def _load_data(self, data: dict, strict: bool = True) -> Union[BaseAttrsDataModel, DotDict]:
        from dl_core.us_manager.us_manager import USManagerBase

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
                "type": self.type_,
                "created_by": self.created_by,
                "created_at": self.created_at,
                "is_locked": self.is_locked,
            }
        ret = {"id": self.uuid, "key": self.location_short_string, "is_favorite": self.is_favorite, **data}
        if isinstance(self.entry_key, WorkbookEntryLocation):
            ret.update(
                workbook_id=self.entry_key.workbook_id,
                name=self.entry_key.entry_name,
                key=self.raw_us_key,
            )

        return ret

    @property
    def created_by(self) -> Optional[str]:
        return self._us_resp.get("createdBy") if isinstance(self._us_resp, dict) else None

    @property
    def created_at(self) -> Optional[str]:
        return self._us_resp.get("createdAt") if isinstance(self._us_resp, dict) else None

    @property
    def updated_at(self) -> Optional[str]:
        return self._us_resp.get("updatedAt") if isinstance(self._us_resp, dict) else None

    @property
    def revision_id(self) -> Optional[str]:
        return self._us_resp.get("revId") if isinstance(self._us_resp, dict) else None

    @property
    def raw_tenant_id(self) -> Optional[str]:
        if not isinstance(self._us_resp, dict):
            raise ValueError("Can not get tenantId from US entry without ._us_resp.")

        may_be_tenant_id = self._us_resp.get("tenantId")

        if may_be_tenant_id is None or isinstance(may_be_tenant_id, str):
            return may_be_tenant_id
        raise ValueError(
            f"Gon unexpected type of tenantId in US entry {self.scope}/{self.type_}/{self.uuid}: {may_be_tenant_id!r}"
        )

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
        self,
        uuid: Optional[str] = None,
        data: Optional[dict] = None,
        entry_key: Optional[EntryLocation] = None,
        type_: Optional[str] = None,
        meta: Optional[dict] = None,
        is_locked: Optional[bool] = None,
        is_favorite: Optional[bool] = None,
        permissions_mode: Optional[str] = None,
        initial_permissions: Optional[str] = None,
        permissions: Optional[dict] = None,
        links: Optional[dict] = None,
        hidden: bool = False,
        data_strict: bool = True,
        *,
        us_manager: USManagerBase,
        unversioned_data: Optional[dict[str, Any]],
    ):
        super().__init__(
            uuid=uuid,
            data=data,
            entry_key=entry_key,
            type_=type_,
            meta=meta,
            is_locked=is_locked,
            is_favorite=is_favorite,
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
        assert self._unversioned_data is not None
        return self._unversioned_data
