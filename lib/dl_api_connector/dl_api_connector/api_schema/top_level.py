from __future__ import annotations

import copy
from copy import deepcopy
import itertools
import logging
import os
from typing import (
    Any,
    ClassVar,
    Generic,
    Iterable,
    Optional,
    Sequence,
    TypeVar,
    Union,
    final,
)

import marshmallow
from marshmallow import (
    missing,
    post_dump,
    post_load,
    pre_load,
)
from marshmallow import Schema
from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.extras import (
    FieldExtra,
    SchemaKWArgs,
)
from dl_constants.enums import (
    CreateMode,
    EditMode,
    ExportMode,
    ImportMode,
    OperationsMode,
)
from dl_core import exc as bi_core_exc
from dl_core.base_models import (
    EntryLocation,
    PathEntryLocation,
    WorkbookEntryLocation,
)
from dl_core.us_entry import USEntry
from dl_core.us_manager.us_manager import USManagerBase


LOGGER = logging.getLogger(__name__)

_TARGET_OBJECT_TV = TypeVar("_TARGET_OBJECT_TV")


class BaseTopLevelSchema(Schema, Generic[_TARGET_OBJECT_TV]):
    CTX_KEY_EDITABLE_OBJECT: ClassVar[str] = "editable_object"
    CTX_KEY_OPERATIONS_MODE: ClassVar[str] = "operations_mode"

    TARGET_CLS: ClassVar[type]

    def __init__(
        self,
        *,
        only: Optional[Sequence[str]] = None,
        exclude: Sequence[str] = (),
        many: bool = False,
        context: Optional[dict] = None,
        load_only: Sequence[str] = (),
        dump_only: Sequence[str] = (),
        partial: Union[Sequence[str], bool] = False,
        unknown: Optional[str] = None,
    ) -> None:
        refined_kwargs = self._refine_init_kwargs(
            dict(
                only=only,
                partial=partial,
                exclude=exclude,
                load_only=load_only,
                dump_only=dump_only,
            ),
            # Context is not bind to self here
            None if context is None else context.get(self.CTX_KEY_OPERATIONS_MODE),
        )

        super().__init__(many=many, context=context, unknown=unknown, **refined_kwargs)

    @staticmethod
    def get_field_extra(f: ma_fields.Field) -> Optional[FieldExtra]:
        return f.metadata.get("bi_extra", None)

    @property
    def operations_mode(self) -> Optional[CreateMode | ImportMode]:
        return self.context.get(self.CTX_KEY_OPERATIONS_MODE)

    @classmethod
    def all_fields_dict(cls) -> dict[str, ma_fields.Field]:
        return cls._declared_fields

    @classmethod
    def all_fields_with_extra_info(cls) -> Iterable[tuple[str, ma_fields.Field, FieldExtra]]:
        for field_name, field in cls.all_fields_dict().items():
            extra = cls.get_field_extra(field)
            if extra is not None:
                yield field_name, field, extra

    def _refine_init_kwargs(self, kw_args: SchemaKWArgs, operations_mode: Optional[OperationsMode]) -> SchemaKWArgs:
        if operations_mode is None:
            return kw_args

        ret = copy.deepcopy(kw_args)

        if isinstance(operations_mode, EditMode) and kw_args["only"] is None:
            editable_fields = []
            for field_name, _field, field_extra in self.all_fields_with_extra_info():
                if (isinstance(field_extra.editable, bool) and field_extra.editable is True) or (
                    isinstance(field_extra.editable, Sequence) and operations_mode in field_extra.editable
                ):
                    editable_fields.append(field_name)

            # To even not accept non-editable fields
            ret["only"] = editable_fields
            # To reset required flags & prevent fallback to `missing=`
            ret["partial"] = editable_fields
        else:
            # Handling extra partials only in non-edit mode
            extra_partial = []
            for field_name, _field, field_extra in self.all_fields_with_extra_info():
                if operations_mode in field_extra.partial_in:
                    extra_partial.append(field_name)
            # Do not override partials if was passed via schema constructor
            # TODO CONSIDER: May be append to constructor partials if present?
            if kw_args["partial"] is False and extra_partial:
                ret["partial"] = tuple(extra_partial)

        # Handling extra excludes in every case
        extra_exclude = []
        for field_name, _field, field_extra in self.all_fields_with_extra_info():
            if operations_mode in field_extra.exclude_in:
                extra_exclude.append(field_name)

        if extra_exclude:
            ret["exclude"] = tuple(
                set(
                    itertools.chain(
                        kw_args["exclude"],
                        extra_exclude,
                    )
                )
            )

        return ret

    def create_object(self, data: dict[str, Any]) -> _TARGET_OBJECT_TV:
        raise NotImplementedError()

    def update_object(self, obj: _TARGET_OBJECT_TV, data: dict[str, Any]) -> _TARGET_OBJECT_TV:
        raise NotImplementedError()

    @post_load(pass_many=False)
    def post_load(self, data: dict[str, Any], **_: Any) -> Union[_TARGET_OBJECT_TV, dict]:
        if isinstance(self.operations_mode, EditMode):
            editable_object = self.context.get(self.CTX_KEY_EDITABLE_OBJECT)
            if editable_object is None:
                return data
            assert isinstance(editable_object, self.TARGET_CLS)
            return self.update_object(editable_object, data)
        if isinstance(self.operations_mode, CreateMode | ImportMode):
            return self.create_object(data)
        raise ValueError(f"Can not perform load. Unknown operations mode: {self.operations_mode!r}")

    def get_allowed_unknown_fields(self) -> set[str]:
        """
        Should return set of fields that does not exist at schema, but it is ok.
        This fields will be removed from data in pre_load hook.
        Main purpose: create a transition period for frontend to adopt to schema changes.
        """
        return set()

    @final
    def delete_unknown_fields(self, data: dict[str, Any]) -> dict[str, Any]:
        LOGGER.info(
            "Got unknown fields for schema %s/%s. Unknown fields will be removed.",
            type(self).__qualname__,
            self.operations_mode,
        )

        cleaned_data = {}
        for field_name, field_value in data.items():
            if field_name in self.fields and not self.fields[field_name].dump_only:
                cleaned_data[field_name] = field_value

        return cleaned_data

    @final
    def handle_unknown_fields(self, data: dict[str, Any]) -> dict[str, Any]:
        refined_data = {}
        allowed_unknown_data = {}
        disallowed_unknown_data = {}

        allowed_unknown_fields = self.get_allowed_unknown_fields()

        for field_name, field_value in data.items():
            if field_name in self.fields:
                refined_data[field_name] = field_value
            else:
                if field_name in allowed_unknown_fields:
                    allowed_unknown_data[field_name] = field_value
                else:
                    disallowed_unknown_data[field_name] = field_value

        # Passing disallowed unknown fields to schema to get correct error messages
        refined_data.update(disallowed_unknown_data)

        if allowed_unknown_data or disallowed_unknown_data:
            sorted_all_unknown_keys = list(
                sorted(
                    itertools.chain(
                        allowed_unknown_data.keys(),
                        disallowed_unknown_data.keys(),
                    )
                )
            )
            sorted_allowed_unknown_keys = list(sorted(allowed_unknown_data.keys()))
            sorted_disallowed_unknown_keys = list(sorted(disallowed_unknown_data.keys()))

            LOGGER.info(
                "Got unknown fields for schema %s/%s. Allowed: %s. Disallowed: %s",
                type(self).__qualname__,
                self.operations_mode,
                ",".join(sorted_allowed_unknown_keys),
                ",".join(sorted_disallowed_unknown_keys),
                extra=dict(
                    schema_unk_fields=sorted_all_unknown_keys,
                    schema_unk_fields_allowed=sorted_allowed_unknown_keys,
                    schema_unk_fields_disallowed=sorted_disallowed_unknown_keys,
                ),
            )

        return refined_data

    @pre_load(pass_many=False)
    def pre_load(self, data: dict[str, Any], **_: Any) -> dict[str, Any]:
        all_data_keys = list(sorted(data.keys()))
        LOGGER.debug(
            "Got fields for schema %s/%s: %s",
            type(self).__qualname__,
            self.operations_mode,
            ",".join(all_data_keys),
            extra=dict(
                schema_input_keys=all_data_keys,
            ),
        )

        if isinstance(self.operations_mode, ImportMode):
            return self.delete_unknown_fields(data)

        return self.handle_unknown_fields(data)


_US_ENTRY_TV = TypeVar("_US_ENTRY_TV", bound=USEntry)


class USEntryAnnotationMixin(Schema):
    description = ma_fields.String(
        required=False,
        allow_none=True,
        load_default="",
        dump_default="",
        bi_extra=FieldExtra(editable=True),
        attribute="annotation.description",
    )


class USEntryBaseSchema(BaseTopLevelSchema[_US_ENTRY_TV], USEntryAnnotationMixin):
    CTX_KEY_USM: ClassVar[str] = "us_manager"

    id = ma_fields.String(dump_only=True, attribute="uuid")
    key = ma_fields.String(dump_only=True, attribute="raw_us_key")

    dir_path = ma_fields.String(
        allow_none=False,
        load_only=True,
        load_default="Connection",
        bi_extra=FieldExtra(exclude_in=[CreateMode.test]),
        attribute="entry_key.dir_path",
    )
    workbook_id = ma_fields.String(
        required=False,
        allow_none=True,
        load_default=None,
        bi_extra=FieldExtra(exclude_in=[CreateMode.test]),
        attribute="entry_key.workbook_id",
    )
    name = ma_fields.String(
        required=True,
        allow_none=False,
        bi_extra=FieldExtra(exclude_in=[CreateMode.test]),
        attribute="entry_key.entry_name",
    )

    @classmethod
    def fieldnames_with_extra_export_fake_info(cls) -> Iterable[str]:
        for field_name, field in cls.all_fields_dict().items():
            extra = cls.get_field_extra(field)
            if extra is not None and extra.export_fake is True:
                yield field_name

    @post_dump(pass_many=False, pass_original=True)
    def post_dump(self, data: dict[str, Any], obj: _US_ENTRY_TV, **_: Any) -> dict[str, Any]:
        if not isinstance(self.operations_mode, ExportMode):
            return data

        data = deepcopy(data)
        for secret_field in self.fieldnames_with_extra_export_fake_info():
            if getattr(obj.data, secret_field, None) is None:
                export_value = None
            else:
                export_value = "******"
            data[secret_field] = export_value
        return self.delete_unknown_fields(data)

    @property
    def us_manager(self) -> USManagerBase:
        return self.context[self.CTX_KEY_USM]

    def default_create_from_dict_kwargs(self, data: dict[str, Any]) -> dict[str, Any]:
        entry_name = data["entry_key"]["entry_name"]
        entry_dir_path = data["entry_key"]["dir_path"]
        entry_wb_id = data["entry_key"]["workbook_id"]

        if entry_wb_id is None and entry_dir_path is None:
            raise marshmallow.ValidationError("dir_path can not be null if workbook_id is null", field_name="dir_path")

        return dict(
            ds_key=resolve_entry_loc_from_api_req_body(
                name=entry_name,
                dir_path=entry_dir_path,
                workbook_id=entry_wb_id,
            ),
            us_manager=self.us_manager,
            annotation=data["annotation"],
        )

    # TODO FIX: Find a way to specify return type
    def create_data_model(self, data_attributes: dict[str, Any]) -> Any:
        raise NotImplementedError()

    @final
    def validate_object(self, obj: _US_ENTRY_TV) -> None:
        try:
            obj.validate()
        except bi_core_exc.EntryValidationError as err:
            if err.model_field:
                cause_attribute = f"data.{err.model_field}"
                cause_field_name = (
                    next(
                        map(
                            lambda field: field.name,
                            filter(lambda field: field.attribute == cause_attribute, self.fields.values()),
                        ),
                        None,
                    )
                    or marshmallow.exceptions.SCHEMA
                )
            else:
                cause_field_name = marshmallow.exceptions.SCHEMA

            raise marshmallow.ValidationError(err.message, field_name=cause_field_name) from err

    @final
    def create_object(self, data: dict[str, Any]) -> _US_ENTRY_TV:
        data_attributes = data.get("data", {})

        obj = self.TARGET_CLS.create_from_dict(  # type: ignore  # TODO: fix
            data_dict=self.create_data_model(data_attributes),
            entry_op_mode=self.operations_mode,
            **self.default_create_from_dict_kwargs(data),
        )
        self.validate_object(obj)
        return obj

    @final
    def update_object(self, obj: _US_ENTRY_TV, data: dict[str, Any]) -> _US_ENTRY_TV:
        obj.entry_op_mode = self.operations_mode

        # Assumed that only data of USEntry can be modified with schema
        assert not (data.keys() - {"data"})

        for data_field_name, data_field_value in data.get("data", {}).items():
            if data_field_value is missing:
                continue

            if isinstance(obj.data, dict):
                obj.data[data_field_name] = obj.data[data_field_value]
            else:
                setattr(obj.data, data_field_name, data_field_value)

        self.validate_object(obj)
        return obj


def resolve_entry_loc_from_api_req_body(
    *,
    name: str,
    dir_path: Optional[str],
    workbook_id: Optional[str],
) -> EntryLocation:
    assert name is not None, "name can not be None"

    if workbook_id is None:
        assert dir_path is not None, "dir_path can not be None"
        return PathEntryLocation(os.path.join(dir_path, name))
    else:
        return WorkbookEntryLocation(
            workbook_id=workbook_id,
            entry_name=name,
        )
