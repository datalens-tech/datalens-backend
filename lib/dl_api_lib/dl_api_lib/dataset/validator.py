from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import (
    TYPE_CHECKING,
    Collection,
    Generator,
    Optional,
    Sequence,
)
import uuid

import attr

from dl_api_lib import (
    exc,
    utils,
)
from dl_api_lib.dataset.base_wrapper import DatasetBaseWrapper
from dl_api_lib.dataset.component_abstraction import (
    DatasetComponentAbstraction,
    DatasetComponentRef,
)
from dl_api_lib.dataset.utils import check_permissions_for_origin_sources
from dl_api_lib.enums import (
    FILTERS_BY_TYPE,
    DatasetAction,
    DatasetSettingName,
    USPermissionKind,
)
from dl_api_lib.query.formalization.block_formalizer import BlockFormalizer
from dl_api_lib.query.formalization.legend_formalizer import ValidationLegendFormalizer
from dl_api_lib.query.formalization.raw_specs import RawQuerySpecUnion
from dl_api_lib.request_model.data import (
    Action,
    AddField,
    AddUpdateObligatoryFilter,
    AvatarActionBase,
    DeleteField,
    DeleteObligatoryFilter,
    FieldAction,
    ObligatoryFilterActionBase,
    ObligatoryFilterBase,
    RelationActionBase,
    ReplaceConnection,
    ReplaceConnectionAction,
    SourceActionBase,
    UpdateField,
    UpdateSettingAction,
)
from dl_app_tools.profiling_base import generic_profiler
from dl_constants.enums import (
    AggregationFunction,
    BinaryJoinOperator,
    CalcMode,
    ComponentErrorLevel,
    ComponentType,
    ConnectionType,
    DataSourceRole,
    DataSourceType,
    ManagedBy,
    TopLevelComponentId,
)
from dl_core.base_models import (
    DefaultConnectionRef,
    DefaultWhereClause,
)
from dl_core.connectors.base.data_source_migration import get_data_source_migrator
from dl_core.constants import DatasetConstraints
from dl_core.data_source.base import DataSource
from dl_core.data_source.collection import DataSourceCollection
from dl_core.data_source.sql import BaseSQLDataSource
from dl_core.data_source.type_mapping import get_data_source_class
from dl_core.db import (
    SchemaColumn,
    SchemaInfo,
)
import dl_core.exc as common_exc
from dl_core.fields import (
    BIField,
    DirectCalculationSpec,
    FormulaCalculationSpec,
    create_calc_spec_from,
    del_calc_spec_kwargs_from,
)
from dl_core.multisource import (
    AvatarRelation,
    BinaryCondition,
    ConditionPartDirect,
    SourceAvatar,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_formula.core import exc as formula_exc
from dl_formula.core.message_ctx import (
    FormulaErrorCtx,
    MessageLevel,
)
from dl_query_processing.compilation.helpers import single_formula_comp_query_for_validation
import dl_query_processing.exc
from dl_query_processing.legend.block_legend import BlockSpec
from dl_utils.utils import enum_not_none


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class ErrorInfo:
    code: Sequence[str] = attr.ib(kw_only=True)
    message: str = attr.ib(kw_only=True)
    details: dict = attr.ib(kw_only=True, factory=dict)
    level: ComponentErrorLevel = attr.ib(kw_only=True, default=ComponentErrorLevel.error)


@attr.s(frozen=True)
class FormulaErrorInfo(ErrorInfo):  # noqa
    row: Optional[int] = attr.ib(kw_only=True)
    column: Optional[int] = attr.ib(kw_only=True)
    token: Optional[str] = attr.ib(kw_only=True)


_FORMULA_TO_CORE_ERR_LEVEL_MAP = {
    MessageLevel.ERROR: ComponentErrorLevel.error,
    MessageLevel.WARNING: ComponentErrorLevel.warning,
}


def comp_err_level_from_formula_err_level(formula_level: MessageLevel) -> ComponentErrorLevel:
    return _FORMULA_TO_CORE_ERR_LEVEL_MAP[formula_level]


def field_not_none(field: Optional[BIField]) -> BIField:
    assert field is not None
    return field


class DatasetValidator(DatasetBaseWrapper):
    """
    An extension of DatasetView that allows "smooth" updates of the DS configuration
    and provides validation methods.
    """

    # TODO: refactor the class's structure (separate into several classes: one per action)
    _validation_mode = True

    def __init__(
        self,
        ds: Dataset,
        us_manager: USManagerBase,
        is_data_api: bool = False,
    ):
        super().__init__(ds=ds, us_manager=us_manager)
        self._is_data_api = is_data_api

        self._ds_ca = DatasetComponentAbstraction(dataset=self._ds, us_entry_buffer=self._us_manager.get_entry_buffer())
        self._remapped_source_ids: dict[str, str] = {}
        self._affected_components: set[DatasetComponentRef] = set()

    def _reload_formalized_specs(self, block_spec: Optional[BlockSpec] = None) -> None:
        # Ignore incoming spec.
        raw_query_spec_union = RawQuerySpecUnion()
        legend = ValidationLegendFormalizer(dataset=self._ds).make_legend(raw_query_spec_union=raw_query_spec_union)
        block_legend = BlockFormalizer(dataset=self._ds).make_block_legend(
            raw_query_spec_union=raw_query_spec_union, legend=legend
        )
        block_spec = block_legend.blocks[0]
        super()._reload_formalized_specs(block_spec=block_spec)

    @property
    def _sync_us_manager(self) -> SyncUSManager:
        us_manager = self._us_manager
        if isinstance(us_manager, SyncUSManager):
            return us_manager
        raise TypeError(
            f"Unsupported type of USManager ({type(us_manager).__qualname__}). " f"Only SyncUSManager supported."
        )

    def _get_action_order_index(self, action: Action) -> int:
        # Fields with calc_mode=parameter should be added first so that sources can reference them
        if (
            isinstance(action, FieldAction)
            and action.action == DatasetAction.add_field
            and isinstance(action.field, UpdateField)
            and action.field.calc_mode == CalcMode.parameter
        ):
            return 0

        return 100

    def _sort_action_batch(self, action_batch: Sequence[Action]) -> Sequence[Action]:
        return sorted(action_batch, key=self._get_action_order_index)

    @generic_profiler("validator-apply-action-batch")
    def apply_batch(self, action_batch: Sequence[Action]) -> None:
        self.update_unpatched_fields()  # TODO: remove after all fields have been patched
        sorted_action_batch = self._sort_action_batch(action_batch)
        try:
            for action_patch in sorted_action_batch:
                self.apply_action(
                    item_data=action_patch,
                )
        except common_exc.DatasetConfigurationError as err:
            LOGGER.info("Error occurred during applying action: %r", err)
            raise exc.DLValidationFatal(f"Invalid action: {err}") from err

        self._check_for_orphaned_component_errors()

    def apply_action(self, item_data: Action, by: Optional[ManagedBy] = ManagedBy.user) -> None:
        """Apply update to the dataset configuration"""

        action = DatasetAction.remap_legacy(item_data.action)
        if self._is_data_api and action not in DatasetAction.get_actions_whitelist_for_data_api():
            raise exc.DatasetActionNotAllowedError(f"Action {action.value} is not allowed.")

        extra = dict(
            dataset_action_name=action.name,
            dataset_action_data=item_data,
        )
        LOGGER.info(f"Going to apply action {action.name}", extra=extra)

        # TODO: ManagedBy is not present in any schema. Why is this check needed here?
        if by is not None and (item_data.managed_by or ManagedBy.user) != by:
            raise exc.DLValidationFatal(f"Item cannot be managed by {by.name}")

        if isinstance(item_data, FieldAction):
            self.apply_field_action(
                action=action,
                field_data=item_data.field,
                order_index=item_data.order_index or 0,
                by=by,
            )
        elif isinstance(item_data, SourceActionBase):
            self.apply_source_action(action=action, source_data=item_data.source, by=by)
        elif isinstance(item_data, AvatarActionBase):
            self.apply_source_avatar_action(
                action=action,
                source_avatar_data=item_data.source_avatar,
                by=by,
                disable_fields_update=item_data.disable_fields_update,
            )
        elif isinstance(item_data, RelationActionBase):
            self.apply_avatar_relation_action(action=action, avatar_relation_data=item_data.avatar_relation, by=by)
        elif isinstance(item_data, ReplaceConnectionAction):
            self.apply_connection_action(action=action, connection_data=item_data.connection, by=by)
        elif isinstance(item_data, ObligatoryFilterActionBase):
            self.apply_obligatory_filter_action(action=action, filter_data=item_data.obligatory_filter, by=by)
        elif isinstance(item_data, UpdateSettingAction):
            self.apply_setting_action(action=action, setting=item_data.setting, by=by)

        self.update_validity_of_affected_components()

    def _check_for_orphaned_component_errors(self) -> None:
        missing_components: list[DatasetComponentRef] = []
        for err_pack in self._ds.error_registry.items:
            component_ref = DatasetComponentRef(component_type=err_pack.type, component_id=err_pack.id)
            component = self._ds_ca.get_component(component_ref=component_ref)
            if component is None:
                missing_components.append(component_ref)
        if missing_components:
            LOGGER.warning(
                "Nonexistent components are referenced in error registry",
                extra={
                    "dataset_id": self._ds.uuid,
                    "components": [
                        {"type": component_ref.component_type, "id": component_ref.component_id}
                        for component_ref in missing_components
                    ],
                },
            )

    def validate_result_schema_length(self, field_cnt_diff: int = 0) -> None:
        if self._is_data_api:
            return

        # only one limit related record in error registry + remove potentially invalid errors
        self._ds.error_registry.remove_errors(id=self._ds.result_schema.id, code=exc.TooManyFieldsError.err_code)

        new_result_schema_len = len(self._ds.result_schema) + field_cnt_diff
        if new_result_schema_len > DatasetConstraints.FIELD_COUNT_LIMIT_HARD and field_cnt_diff > 0:
            raise dl_query_processing.exc.DatasetTooManyFieldsFatal()
        if new_result_schema_len > DatasetConstraints.FIELD_COUNT_LIMIT_SOFT:
            self._ds.error_registry.add_error(
                id=self._ds.result_schema.id,
                type=ComponentType.result_schema,
                message="Too many fields in the result schema",
                code=exc.TooManyFieldsError.err_code,
            )

    def get_dependent_fields(self, field: Optional[BIField]) -> list[BIField]:
        """Return list of fields directly dependent on the given one"""

        if field is None:
            return []

        result = []
        for other_field in self._ds.result_schema:
            if other_field.depends_on(field):
                result.append(other_field)

        return result

    def _get_field_errors(self, field: BIField) -> list[FormulaErrorCtx]:
        errors = self.formula_compiler.get_field_errors(field=field)
        if not self._has_sources:
            errors.append(
                FormulaErrorCtx(
                    message="Data source is not set for the dataset",
                    code=tuple(common_exc.DataSourceNotFound.err_code),
                    level=MessageLevel.ERROR,
                )
            )
            return errors
        try:
            formula_info = self.formula_compiler.compile_field_formula(field=field, collect_errors=True)
            assert self._column_reg is not None
            formula_comp_query = single_formula_comp_query_for_validation(
                formula=formula_info,
                ds_accessor=self._ds_accessor,
                column_reg=self._column_reg,
            )
            formula_comp_multi_query = self.process_compiled_query(compiled_query=formula_comp_query)
            multi_translator = self.make_multi_query_translator()
            errors += multi_translator.collect_errors(compiled_multi_query=formula_comp_multi_query)
        except formula_exc.FormulaError:
            pass
        return errors

    def _get_relation_errors(self, relation: AvatarRelation) -> list[FormulaErrorCtx]:
        # TODO: switch to ComponentError items
        errors: list[FormulaErrorCtx] = []

        # Validate expressions
        try:
            formula_info = self.formula_compiler.compile_relation_formula(relation=relation, collect_errors=True)
            assert self._column_reg is not None
            formula_comp_query = single_formula_comp_query_for_validation(
                formula=formula_info, ds_accessor=self._ds_accessor, column_reg=self._column_reg
            )
            formula_comp_multi_query = self.process_compiled_query(compiled_query=formula_comp_query)
            multi_translator = self.make_multi_query_translator()
            errors = multi_translator.collect_errors(compiled_multi_query=formula_comp_multi_query)
        except formula_exc.FormulaError as err:
            errors += err.errors

        # Validate join_type
        left_coll = self._get_data_source_coll_strict(
            source_id=self._ds_accessor.get_avatar_strict(avatar_id=relation.left_avatar_id).source_id
        )
        right_coll = self._get_data_source_coll_strict(
            source_id=self._ds_accessor.get_avatar_strict(avatar_id=relation.right_avatar_id).source_id
        )

        if not left_coll.supports_join_type(relation.join_type) or not right_coll.supports_join_type(
            relation.join_type
        ):
            errors.append(
                FormulaErrorCtx(
                    message=common_exc.AvatarRelationJoinTypeError.default_message,
                    code=common_exc.AvatarRelationJoinTypeError.err_code,  # type: ignore  # TODO: fix
                    level=MessageLevel.ERROR,
                )
            )

        return errors

    def _get_formula_errors(self, formula: str, feature_errors: bool = True) -> list[FormulaErrorCtx]:
        errors = self.formula_compiler.get_formula_errors(formula=formula)
        try:
            formula_info = self.formula_compiler.compile_text_formula(formula=formula, collect_errors=True)
            assert self._column_reg is not None
            formula_comp_query = single_formula_comp_query_for_validation(
                formula=formula_info, ds_accessor=self._ds_accessor, column_reg=self._column_reg
            )
            formula_comp_multi_query = self.process_compiled_query(compiled_query=formula_comp_query)
            multi_translator = self.make_multi_query_translator()
            errors += multi_translator.collect_errors(
                compiled_multi_query=formula_comp_multi_query, feature_errors=feature_errors
            )
        except formula_exc.FormulaError:
            pass
        return errors

    def _automanage_calc_mode_attrs(self, field: BIField) -> BIField:
        updated_field = field
        if updated_field.calc_mode == CalcMode.direct:
            if updated_field.avatar_id is None:
                root_avatar = self._ds_accessor.get_root_avatar_strict()
                updated_field = updated_field.clone(avatar_id=root_avatar.id)
            col = self._get_raw_schema_column(field=updated_field)
            if col is not None:
                updated_field = updated_field.clone(
                    has_auto_aggregation=col.has_auto_aggregation,
                    lock_aggregation=col.lock_aggregation,
                )
            else:
                updated_field = updated_field.clone(
                    has_auto_aggregation=False,
                    lock_aggregation=False,
                )
        elif updated_field.calc_mode == CalcMode.formula:
            if updated_field.formula:
                updated_field = updated_field.clone(
                    guid_formula=BIField.rename_in_formula(
                        formula=updated_field.formula, key_map=self._ds.result_schema.titles_to_guids
                    )
                )
            elif updated_field.guid_formula:
                updated_field = updated_field.clone(
                    formula=BIField.rename_in_formula(
                        formula=updated_field.guid_formula, key_map=self._ds.result_schema.guids_to_titles
                    )
                )

        return updated_field

    def _get_autoupdated_field(self, field: BIField, explicitly_updated: bool = False) -> BIField:
        updated_field = field
        updated_field = self._automanage_calc_mode_attrs(updated_field)

        self.formula_compiler.update_field(updated_field)
        updated_field = updated_field.clone(
            data_type=self.formula_compiler.get_field_final_data_type(updated_field),
            initial_data_type=self.formula_compiler.get_field_initial_data_type(updated_field),
            has_auto_aggregation=self.formula_compiler.field_has_auto_aggregation(updated_field),
            type=self.formula_compiler.get_field_type(updated_field),
        )
        field_is_new = field.initial_data_type is None

        # Auto-manage cast
        if field_is_new and updated_field.cast is None:
            LOGGER.info(
                f"Automatically initializing cast in {field.title} "
                f"as {enum_not_none(updated_field.initial_data_type).name}"
            )
            updated_field = updated_field.clone(cast=updated_field.initial_data_type)
        elif not field_is_new and updated_field.initial_data_type != field.initial_data_type:
            # autofix data type if derived type has changed
            if updated_field.cast != updated_field.initial_data_type:
                LOGGER.info(
                    f"Automatically setting cast in {field.title} "
                    f"to {enum_not_none(updated_field.initial_data_type).name}"
                )
                updated_field = updated_field.clone(cast=updated_field.initial_data_type)
                # the change of cast might result in new data_type
                updated_field = self._get_autoupdated_field(updated_field)

        # Auto-manage has_auto_aggregation
        if (
            explicitly_updated
            and updated_field.has_auto_aggregation
            and updated_field.aggregation != AggregationFunction.none
        ):
            # Field is autoaggregated and also has an explicit aggregation.
            # We can drop the additional aggregation if this is the field
            # being updated by the user (`explicitly_updated`) explicitly.
            updated_field = updated_field.clone(
                aggregation=AggregationFunction.none,
            )

        return updated_field

    def _update_dependent_fields(
        self,
        old_field: Optional[BIField],
        new_field: Optional[BIField],
        visited_guids: frozenset[str],
    ) -> None:
        """Automatically update all fields dependent on the given one"""
        dep_field_ids = set()
        dep_fields = []
        for dep_field in self.get_dependent_fields(old_field) + self.get_dependent_fields(new_field):
            if dep_field.guid not in dep_field_ids:
                dep_fields.append(dep_field)
                dep_field_ids.add(dep_field.guid)

        # Handle renaming and guid changing
        if new_field is not None and old_field is not None:
            if new_field.title != old_field.title:
                for i, dep_field in enumerate(dep_fields):
                    assert isinstance(dep_field.calc_spec, FormulaCalculationSpec)
                    dep_fields[i] = dep_field.clone(
                        formula=BIField.rename_in_formula(dep_field.formula, key_map={old_field.title: new_field.title})
                    )
            if new_field.guid != old_field.guid:
                for i, dep_field in enumerate(dep_fields):
                    assert isinstance(dep_field.calc_spec, FormulaCalculationSpec)
                    dep_fields[i] = dep_field.clone(
                        guid_formula=BIField.rename_in_formula(
                            dep_field.guid_formula, key_map={old_field.guid: new_field.guid}
                        )
                    )

        # Update the fields
        for dep_field in dep_fields:
            self._update_field(
                old_field=self.get_field_by_id(dep_field.guid),
                new_field=dep_field,
                recursive=True,
                visited_guids=visited_guids,
            )

    def _update_field(
        self,
        old_field: Optional[BIField],
        new_field: Optional[BIField],
        order_index: Optional[int] = None,
        recursive: bool = False,
        visited_guids: frozenset[str] = frozenset(),
        explicitly_updated: bool = False,
    ) -> None:
        """Update/delete/add field and apply autofixes."""

        latest_field = field_not_none(new_field or old_field)
        field_id = latest_field.guid
        if field_id in visited_guids:
            # recursion detected
            return
        visited_guids |= frozenset([field_id])

        component_ref = DatasetComponentRef(component_type=ComponentType.field, component_id=field_id)

        # Clear old errors
        self._ds.error_registry.remove_errors(id=field_id)

        # Remove expression and dependency caches for field
        self.formula_compiler.uncache_field(latest_field)

        if new_field is None and old_field is not None:
            # Field is being deleted
            self.formula_compiler.unregister_field(old_field)
            self._ds.result_schema.remove(field_id=old_field.guid)
        elif old_field is None and new_field is not None:
            # Field is being created
            if order_index is None:
                order_index = len(self._ds.result_schema)
            self.formula_compiler.register_field(new_field)
            new_field = self._get_autoupdated_field(new_field, explicitly_updated=explicitly_updated)
            self.formula_compiler.update_field(new_field)
            self._ds.result_schema.add(idx=order_index, field=new_field)
        else:
            assert new_field is not None
            # Field is being updated
            new_field = self._get_autoupdated_field(new_field, explicitly_updated=explicitly_updated)
            if new_field != old_field:
                self.formula_compiler.update_field(new_field)
                field_idx = self._ds.result_schema.index(field_id=field_id)
                self._ds.result_schema.update_field(idx=field_idx, field=new_field)

        self._reload_formalized_specs()
        if recursive and new_field != old_field:
            self._update_dependent_fields(
                old_field,
                new_field,
                visited_guids=visited_guids,
            )

        if new_field is not None and old_field is not None and new_field.cast != old_field.cast:
            oblig_filter = self._ds_accessor.get_obligatory_filter_opt(field_guid=old_field.guid)
            if oblig_filter is not None:
                allowed_filters = FILTERS_BY_TYPE[enum_not_none(new_field.cast)]
                for default_filter in oblig_filter.default_filters:
                    if default_filter.operation not in allowed_filters:
                        self._ds.error_registry.add_error(
                            # FIXME: Filter errors will be accumulated here
                            #  (new ones being added without the old ones being removed).
                            #  This kind of ruins the idempotency of this method
                            id=oblig_filter.id,
                            type=ComponentType.obligatory_filter,
                            message=f"Filtration type {default_filter.operation.name} "
                            f"is not allowed for field type {enum_not_none(new_field.cast).name}",
                            code=common_exc.DatasetConfigurationError.err_code,  # TODO: add specific err_code
                            details=dict(
                                filter_type=default_filter.operation.name,
                                field_type=enum_not_none(new_field.cast).name,
                            ),
                        )
                        of_component_ref = DatasetComponentRef(
                            component_type=ComponentType.obligatory_filter, component_id=oblig_filter.id
                        )
                        self.add_affected_component(of_component_ref)

        if new_field is not None:
            for error in self._get_field_errors(field=new_field):
                self._ds.error_registry.add_error(
                    id=field_id,
                    type=ComponentType.field,
                    message=error.message,
                    code=error.code,
                    details={"token": error.token, "row": error.coords[0], "column": error.coords[1]},
                )
        self.add_affected_component(component_ref)
        old_title = old_field.title if old_field is not None else None
        new_title = new_field.title if new_field is not None else None
        self.perform_component_title_validation(component_ref=component_ref, old_title=old_title, new_title=new_title)

        # Update field dependencies in dataset
        field_dep_mgr = self._ds_dep_mgr_factory.get_field_shallow_inter_dependency_manager()
        if new_field is None:
            field_dep_mgr.clear_field_direct_references(dep_field_id=field_id)  # type: ignore  # TODO: fix
        else:
            field_dep_mgr.set_field_direct_references(
                dep_field_id=field_id, ref_field_ids=self.formula_compiler.get_referenced_fields(new_field)
            )

    def _get_unpatched_field_ids(self) -> list[str]:
        """Return fields that required patching of properties (e.g. cast, initial_data_type, etc.)"""
        return [
            f.guid
            for f in self._ds.result_schema
            if (f.type is None or f.initial_data_type is None or f.cast is None or f.data_type is None)
        ]

    def update_unpatched_fields(self) -> None:
        """Patch all fields that require patching (non-recursive)"""
        for field_id in self._get_unpatched_field_ids():
            field = self.get_field_by_id(field_id)
            self._update_field(field, field, recursive=False)

    def _get_raw_schema_column(self, field: BIField) -> Optional[SchemaColumn]:
        avatar_id = field.avatar_id
        if avatar_id is None:
            LOGGER.error("Empty avatar_id for field %s, cannot check auto aggregation", field.guid)
            return None
        avatar = self._ds_accessor.get_avatar_opt(avatar_id=avatar_id)
        if avatar is None:
            LOGGER.warning(
                "Avatar not found for id %s from field %s, cannot check auto aggregation", avatar_id, field.guid
            )
            return None
        dsrc_coll = self._get_data_source_coll_strict(source_id=avatar.source_id)
        raw_schema = dsrc_coll.get_cached_raw_schema()
        try:
            return [c for c in raw_schema if c.name == field.source][0]  # type: ignore  # TODO: fix
        except IndexError:
            LOGGER.warning("No field with source %s found in raw_schema", field.source)
            return None

    @generic_profiler("validator-apply-field-action")
    def apply_field_action(
        self,
        action: DatasetAction,
        field_data: UpdateField,
        order_index: Optional[int] = None,
        by: Optional[ManagedBy] = ManagedBy.user,
    ) -> None:
        """Apply update to the result schema configuration"""

        field_data_dict = field_data.as_dict()
        field_data_dict = {k: v for k, v in field_data_dict.items() if v is not None}
        field_id = field_data_dict.pop("guid", None) or self._id_generator.make_field_id(field_data_dict.get("title"))
        new_field_id = field_data_dict.pop("new_id", None)
        order_index = order_index or 0  # added to the beginning by default for usability reasons
        component_ref = DatasetComponentRef(component_type=ComponentType.field, component_id=field_id)
        strict = field_data_dict.pop("strict", False)

        update_field_id = new_field_id is not None and action == DatasetAction.update_field
        if update_field_id:
            if not self._id_validator.is_valid(new_field_id):
                raise exc.DLValidationFatal("Field ID must be [a-z0-9_\\-]{1,36}")
            new_component_ref = DatasetComponentRef(component_type=ComponentType.field, component_id=new_field_id)
            self.perform_component_id_validation(component_ref=new_component_ref)
            # non strict option is used only for old charts. not needed for id update
            strict = True

        old_field: Optional[BIField] = None
        new_field: Optional[BIField] = None

        if action == DatasetAction.add_field:
            self.perform_component_id_validation(component_ref=component_ref)
            self.validate_result_schema_length(field_cnt_diff=1)

        if action in (DatasetAction.add_field, DatasetAction.update_field):
            if "formula" in field_data_dict and "guid_formula" not in field_data_dict:
                field_data_dict["guid_formula"] = ""
            elif "guid_formula" in field_data_dict and "formula" not in field_data_dict:
                field_data_dict["formula"] = ""

            if self._is_data_api:
                # forbidden fields to be mutated via Data API
                for field_name in ("value_constraint", "template_enabled"):
                    if field_name in field_data_dict:
                        del field_data_dict[field_name]

        if action in (DatasetAction.update_field, DatasetAction.delete_field):
            try:
                old_field = self.get_field_by_id(field_id)
            except common_exc.FieldNotFound as err:
                err_msg = f"Field with ID {field_id} doesn't exist"
                LOGGER.error(err_msg)
                if strict:
                    raise exc.DLValidationFatal(err_msg) from err
                return

            self._ds_ca.validate_component_can_be_managed(component_ref=component_ref, by=by)
            LOGGER.info(f"Field {field_id} old data", extra=old_field._asdict())

        if action == DatasetAction.add_field or update_field_id:
            checked_field_id = field_id if not update_field_id else new_field_id
            try:
                self.get_field_by_id(checked_field_id)
            except common_exc.FieldNotFound:
                pass
            else:
                err_msg = "Field with ID {} already exists".format(checked_field_id)
                LOGGER.error(err_msg)
                if strict:
                    raise exc.DLValidationFatal(err_msg)
                return

        if action == DatasetAction.update_field:
            # Update field id
            assert old_field is not None
            if update_field_id:
                index = self._ds.result_schema.index(field_id)
                self.apply_field_action(
                    action=DatasetAction.delete_field,
                    field_data=DeleteField(guid=field_id),
                    by=by,
                )
                add_field_data = dict(
                    guid=new_field_id,
                    title=old_field.title,
                    type=old_field.type,
                    aggregation=old_field.aggregation,
                    cast=old_field.cast,
                    has_auto_aggregation=old_field.has_auto_aggregation,
                    lock_aggregation=old_field.lock_aggregation,
                    hidden=old_field.hidden,
                    calc_spec=old_field.calc_spec,
                )
                self.apply_field_action(
                    action=DatasetAction.add_field,
                    field_data=AddField(**add_field_data),
                    by=by,
                    order_index=index,
                )
                self._ds.rename_field_id_usages(old_id=field_id, new_id=new_field_id)
                return

            # Update existing field
            new_field = old_field.clone(**field_data_dict)
            if new_field == old_field:
                # Nothing changed, so just quit
                return

        elif action == DatasetAction.add_field:
            # add new field
            # TODO: use create_result_schema_field from dataset instead
            field_data_dict["hidden"] = field_data_dict.get("hidden", False)
            field_data_dict["aggregation"] = field_data_dict.get("aggregation", AggregationFunction.none)
            if "calc_spec" not in field_data_dict:
                default_calc_mode = CalcMode.formula if field_data_dict.get("formula") else CalcMode.direct
                field_data_dict["calc_mode"] = field_data_dict.get("calc_mode", default_calc_mode)
                field_data_dict["calc_spec"] = field_data_dict.get("calc_spec", create_calc_spec_from(field_data_dict))
            field_data_dict = del_calc_spec_kwargs_from(field_data_dict)

            if (
                isinstance(field_data_dict["calc_spec"], DirectCalculationSpec)
                and not field_data_dict["calc_spec"].avatar_id
            ):
                # TODO: remove this after old charts are somehow updated
                root_avatar = self._ds_accessor.get_root_avatar_strict()
                field_data_dict["calc_spec"] = field_data_dict["calc_spec"].clone(avatar_id=root_avatar.id)

            new_field = BIField.make(guid=field_id, **field_data_dict)

        elif action == DatasetAction.clone_field:
            # clone existing field under a new title
            from_field_id = field_data_dict["from_guid"]
            from_field = self.get_field_by_id(from_field_id)
            if by != ManagedBy.user:
                raise common_exc.DatasetConfigurationError("Non-user-managed fields cannot be cloned")

            cast = field_data_dict.get("cast")
            cast = cast if cast is not None else from_field.cast
            aggregation = field_data_dict.get("aggregation")
            aggregation = aggregation if aggregation is not None else from_field.aggregation

            # It has the same type as the original field if both have aggregations or both don't
            same_type_as_original = (
                from_field.aggregation != AggregationFunction.none
                and aggregation != AggregationFunction.none
                or from_field.aggregation == AggregationFunction.none
                and aggregation == AggregationFunction.none
            )

            add_field_data = dict(
                guid=field_id,
                title=field_data_dict["title"],
                type=from_field.type if same_type_as_original else None,
                aggregation=aggregation,
                cast=cast,
                has_auto_aggregation=from_field.has_auto_aggregation,
                lock_aggregation=from_field.lock_aggregation,
                hidden=from_field.hidden,
                calc_spec=from_field.calc_spec,
            )
            self.apply_field_action(
                action=DatasetAction.add_field,
                field_data=AddField(**add_field_data),
                by=ManagedBy.user,
                order_index=order_index,
            )
            return

        elif action == DatasetAction.delete_field:
            # delete field

            # Check if field obligatory filter is set up
            if self._ds_accessor.get_obligatory_filter_opt(field_guid=field_id) is not None:
                raise common_exc.DatasetConfigurationError(
                    f'Obligatory filter present for field "{field_id}". It should be deleted first.'
                )

        self._update_field(
            old_field=old_field,
            new_field=new_field,
            order_index=order_index,
            recursive=True,
            explicitly_updated=True,
        )

        # errors can be cleared or fixed by now, so revalidate the result schema length
        if action in (DatasetAction.add_field, DatasetAction.delete_field):
            self.validate_result_schema_length()

    def _update_direct_fields_for_updated_raw_schema(
        self,
        old_raw_schema: Sequence[SchemaColumn],
        new_raw_schema: Optional[Sequence[SchemaColumn]],
        avatar_ids: list[str],
        do_delete_fields: bool = False,
        force_update_fields: bool = False,
    ) -> None:
        old_title_by_name = {col.name: col.title for col in (old_raw_schema or ())}
        new_name_by_title = {col.title: col.name for col in (new_raw_schema or ())}

        old_column_by_name = {col.name: col for col in (old_raw_schema or ())}
        new_column_by_name = {col.name: col for col in (new_raw_schema or ())}

        def apply_field_changes() -> None:
            # Before updating all of them, uncache all affected fields to avoid loading incorrect dependencies
            for old_field, new_field in updated_fields:
                self.formula_compiler.uncache_field(field_not_none(old_field or new_field))
            # Now move on to updating
            updated_ids: list[str] = []
            by_id: dict[str, tuple[Optional[BIField], Optional[BIField]]] = {}
            for old_field, new_field in updated_fields:
                self._update_field(old_field, new_field, recursive=False)
                field_id = field_not_none(old_field or new_field).guid
                if field_id not in by_id:
                    by_id[field_id] = (old_field, new_field)
                    updated_ids.append(field_id)

            # Now update dependent fields
            for field_id in updated_ids:
                new_field, old_field = by_id[field_id]
                self._update_dependent_fields(
                    old_field=old_field,
                    new_field=new_field,
                    visited_guids=frozenset(),
                )

            updated_fields.clear()

        field_cnt_diff = 0
        fields_by_avatar_and_source = set()
        updated_fields: list[tuple[Optional[BIField], Optional[BIField]]] = []
        for old_field in self._ds.result_schema:
            if old_field.calc_mode != CalcMode.direct or old_field.avatar_id not in avatar_ids:
                continue

            # try to match them to the new raw schema by title from the original raw schema
            original_title = old_title_by_name.get(old_field.source)
            new_field = old_field
            if original_title in new_name_by_title:
                if new_name_by_title[original_title] != old_field.source:
                    LOGGER.info(
                        f'Replacing source column "{original_title}" '
                        f'with "{new_name_by_title[original_title]}" in field {old_field.guid}'
                    )
                    # match by title
                    new_field = new_field.clone(source=new_name_by_title[original_title])

            else:
                # Column is no longer present
                if do_delete_fields:  # Source was deleted
                    # In this case delete the fields
                    new_field = None
                    field_cnt_diff -= 1

            # re-validate ALL fields that belong to the specified avatars
            # and that have changed in some way
            latest_field = field_not_none(new_field or old_field)
            old_column = old_column_by_name.get(latest_field.source)
            new_column = new_column_by_name.get(latest_field.source)
            if force_update_fields or (new_field != old_field) or (new_column != old_column):
                updated_fields.append((old_field, new_field))

            fields_by_avatar_and_source.add((latest_field.avatar_id, latest_field.source))

        apply_field_changes()

        # generate new fields for new columns
        if new_raw_schema:
            for avatar_id in avatar_ids:
                for new_col in new_raw_schema:
                    if (avatar_id, new_col.name) not in fields_by_avatar_and_source:
                        field_cnt_diff += 1
                        new_field = BIField.make(
                            **self._ds.create_result_schema_field(
                                column=new_col,
                                field_id_generator=self._id_generator,
                                avatar_id=avatar_id,
                            )
                        )
                        updated_fields.append((None, new_field))
        self.validate_result_schema_length(field_cnt_diff)

        apply_field_changes()

    def add_affected_component(self, component_ref: DatasetComponentRef) -> None:
        """
        Register component as affected by current update action.
        This registry is used for updating validity status of components
        """
        self._affected_components.add(component_ref)

    def update_validity_of_affected_components(self, clear: bool = True) -> None:
        """
        Iterate over components registered in ``self._affected_components``
        and update their ``valid`` attribute based on the presence or absence of errors
        in the dataset's error registry.
        """
        for component_ref in self._affected_components:
            self._ds_ca.update_component_validity(
                component_ref=component_ref, valid=not self._ds.error_registry.get_pack(id=component_ref.component_id)
            )
        if clear:
            self._affected_components.clear()

    def perform_component_id_validation(self, component_ref: DatasetComponentRef) -> None:
        if component_ref.component_id in (toplevel_id.value for toplevel_id in TopLevelComponentId):
            raise exc.DLValidationFatal(f"Cannot use a reserved ID - {component_ref.component_id}")

        # TODO validate component id itself: length, allowed symbols, etc.

        # TODO bring this back when external-api defaults are patched
        # other_types = {component_type for component_type in ComponentType} - {component_ref.component_type}
        #
        # for other_type in other_types:
        #     for other_component in self._ds_ca.iter_dataset_components_by_type(other_type):
        #         other_component_id = other_component.guid if isinstance(other_component, BIField) else other_component.id
        #
        #         if component_ref.component_id == other_component_id:
        #             raise exc.DLValidationFatal(
        #                 f'Component IDs are not unique: '
        #                 f'found {component_ref.component_type.name} and {other_type.name} '
        #                 f'with ID {other_component_id}'
        #             )

    def perform_component_title_validation(
        self, component_ref: DatasetComponentRef, old_title: Optional[str] = None, new_title: Optional[str] = None
    ) -> None:
        """
        Validate title change for the given component.
        If a conflict for this title is found among some of the components,
        then register errors for all of them (if not registered yet).
        If a conflict was present for the old title, but is not there for the new one,
        then remove errors.

        Return list of affected component IDs

        Supports all component types with titles: data_source (collection), source_avatar, field
        """
        old_title_conflict_ids = set()
        new_title_conflict_ids = set()
        exc_cls = {
            ComponentType.data_source: common_exc.DataSourceTitleConflict,
            ComponentType.source_avatar: common_exc.SourceAvatarTitleConflict,
            ComponentType.field: common_exc.FieldTitleConflict,
        }[component_ref.component_type]

        for other_component in self._ds_ca.iter_dataset_components_by_type(component_type=component_ref.component_type):
            if not isinstance(other_component, (DataSourceCollection, SourceAvatar, BIField)):
                raise TypeError(f"Component type {type(other_component)} is not supported here")

            other_component_id = other_component.guid if isinstance(other_component, BIField) else other_component.id
            if other_component_id == component_ref.component_id:
                continue
            if old_title is not None and other_component.title == old_title:
                old_title_conflict_ids.add(other_component_id)
            if new_title is not None and other_component.title == new_title:
                new_title_conflict_ids.add(other_component_id)

        if old_title_conflict_ids:
            old_title_conflict_ids.add(component_ref.component_id)
        if new_title_conflict_ids:
            new_title_conflict_ids.add(component_ref.component_id)

        for other_component_id in old_title_conflict_ids | new_title_conflict_ids:
            other_component_ref = DatasetComponentRef(
                component_type=component_ref.component_type,
                component_id=other_component_id,
            )
            if other_component_id in new_title_conflict_ids:
                # new conflict
                pack = self._ds.error_registry.get_pack(id=other_component_id)
                if pack is None or not pack.get_errors(code=exc_cls.err_code):
                    self._ds.error_registry.add_error(
                        id=other_component_id,
                        type=component_ref.component_type,
                        message=exc_cls.default_message,
                        code=exc_cls.err_code,
                        details={"title": new_title, "other_id": other_component_id},
                    )
            else:
                # old conflict, now resolved, so remove error
                self._ds.error_registry.remove_errors(id=other_component_id, code_prefix=exc_cls.err_code)

            self.add_affected_component(other_component_ref)

    def _are_schemas_identical(
        self,
        old_raw_schema: Optional[Sequence[SchemaColumn]],
        new_raw_schema: Optional[Sequence[SchemaColumn]],
    ) -> bool:
        if old_raw_schema is None and new_raw_schema is None:
            return True
        if old_raw_schema is None or new_raw_schema is None:
            return False
        assert old_raw_schema is not None and new_raw_schema is not None

        if type(old_raw_schema) is not type(new_raw_schema):
            old_raw_schema = tuple(old_raw_schema)
            new_raw_schema = tuple(new_raw_schema)
        return new_raw_schema == old_raw_schema

    def refresh_data_source(
        self,
        source_id: str,
        old_raw_schema: Optional[Sequence[SchemaColumn]],
        force_update_fields: bool = False,
    ) -> None:
        """
        Patch data source with parameters from the database.
        If any errors are encountered, then register them in the dataset.
        """

        sr = self._service_registry

        @contextmanager
        def source_error_ctx() -> Generator[None, None, None]:
            try:
                yield
            except common_exc.DLBaseException as err:
                LOGGER.exception(f"Failed to check source {source_id}. Assuming it doesn't exist")
                self._ds.error_registry.add_error(
                    id=source_id,
                    type=ComponentType.data_source,
                    message=err.message,
                    code=err.err_code,
                    details=err.details,
                )

        dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
        origin_dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)

        def conn_executor_factory_func() -> SyncConnExecutorBase:
            conn_executor = sr.get_conn_executor_factory().get_sync_conn_executor(conn=origin_dsrc.connection)
            return conn_executor

        @generic_profiler("validator-get-source-exists")
        def get_source_exists() -> bool:
            exists = False  # noqa
            with source_error_ctx():
                exists = origin_dsrc.source_exists(conn_executor_factory=conn_executor_factory_func)
            return exists

        @generic_profiler("validator-get-db-info")
        def get_db_version(exists: bool) -> Optional[str]:
            db_version = None
            if exists:
                with source_error_ctx():
                    db_version = origin_dsrc.get_db_info(conn_executor_factory=conn_executor_factory_func).version

                LOGGER.info(f"Got version {db_version} for data source {source_id}")
            return db_version

        @generic_profiler("validator-get-schema")
        def get_schema_info(exists: bool) -> Optional[SchemaInfo]:
            with source_error_ctx():
                if exists:
                    schema_info = origin_dsrc.get_schema_info(conn_executor_factory=conn_executor_factory_func)
                    # TODO FIX: BI-2355 Ensure that dsrc.get_schema_info() do not suppress exceptions and remove
                    if not schema_info.schema:  # TODO: remove when get_schema starts raising errors
                        raise common_exc.DLBaseException(message="Failed to load table schema")
                    return schema_info
                else:
                    params = dict()
                    if isinstance(origin_dsrc, BaseSQLDataSource):
                        table_def = origin_dsrc.get_table_definition()
                        params["table_definition"] = str(table_def)

                    raise common_exc.SourceDoesNotExist(
                        db_message="",
                        query="",
                        params=params,
                    )
            return None

        exists = get_source_exists()
        LOGGER.info(f"Data source {source_id} exists: {exists}")

        new_raw_schema_info = get_schema_info(exists=exists)

        new_raw_schema = new_raw_schema_info.schema if new_raw_schema_info is not None else []
        new_idx_info_set = new_raw_schema_info.indexes if new_raw_schema_info is not None else None

        LOGGER.info(f"Got raw_schema with {len(new_raw_schema)} columns for data source {source_id}")

        db_version = get_db_version(exists=exists)
        self._ds_editor.update_data_source(
            source_id=source_id,
            role=DataSourceRole.origin,
            raw_schema=new_raw_schema,
            db_version=db_version,
            index_info_set=new_idx_info_set,
        )

        self._reload_sources()

        avatar_ids = [avatar.id for avatar in self._ds_accessor.get_avatar_list(source_id=source_id)]
        new_direct_result_fields = self._ds.result_schema.get_direct_fields_for_avatars(avatar_ids)
        if (
            not self._are_schemas_identical(new_raw_schema, old_raw_schema)
            or len(new_raw_schema) != len(new_direct_result_fields)
            or force_update_fields
        ):
            # try to match old and new schemas against each other
            # and update result_schema fields accordingly
            self._update_direct_fields_for_updated_raw_schema(
                old_raw_schema=old_raw_schema,  # type: ignore  # TODO: fix
                new_raw_schema=new_raw_schema,
                avatar_ids=avatar_ids,
                do_delete_fields=False,
                force_update_fields=force_update_fields,
            )

    def validate_source_already_exists(self, source_id: str, source_data: dict) -> bool:
        """
        Check if an identical source already exists.
        If it does, then register the ID as remapped, return ``True``.
        Otherwise return ``False``.
        """

        existing_id = self._ds.find_data_source_configuration(
            connection_id=source_data.get("connection_id"),
            created_from=source_data["source_type"],
            parameters=source_data.get("parameters"),
            # Currently ignoring: `title`
        )
        if existing_id:
            # such source already exists, don't add its copy to the dataset
            # remap all following actions to use the existing source's ID
            LOGGER.info("Such a source already exists, so remapping ID %s to %s", source_id, existing_id)
            self._remapped_source_ids[source_id] = existing_id
            return True

        return False

    @generic_profiler("validator-apply-source-action")
    def apply_source_action(
        self,
        action: DatasetAction,
        source_data: dict,
        by: Optional[ManagedBy] = ManagedBy.user,
        ignore_source_ids: Optional[Collection[str]] = None,
    ) -> None:
        """Apply update to the data source configuration"""

        if action != DatasetAction.refresh_source:
            # any source update requires sufficient permissions on the connection,
            # but you still can refresh fields
            source_ids = set(self._ds_accessor.get_data_source_id_list()) - set(ignore_source_ids or ())
            check_permissions_for_origin_sources(
                dataset=self._ds,
                source_ids=source_ids,
                permission_kind=USPermissionKind.read,
                us_entry_buffer=self._us_manager.get_entry_buffer(),
            )

        source_data = source_data.copy()
        source_id = source_data.pop("id") or str(uuid.uuid4())
        component_ref = DatasetComponentRef(component_type=ComponentType.data_source, component_id=source_id)
        old_title: Optional[str] = None
        new_title: Optional[str] = None

        if source_id in self._remapped_source_ids:
            LOGGER.warning("Source %s has not been added, so action %s will be ignored", source_id, action.name)
            return

        if action == DatasetAction.add_source:
            if self.validate_source_already_exists(source_id=source_id, source_data=source_data):
                return
            self.perform_component_id_validation(component_ref)

        def add_source(title: str) -> None:
            connection_id = source_data.get("connection_id")
            if connection_id is None:
                raise exc.DLValidationFatal("source connection_id cannot be None")

            connection_ref = DefaultConnectionRef(conn_id=connection_id)
            self._sync_us_manager.ensure_entry_preloaded(connection_ref)
            self._ds_editor.add_data_source(
                source_id=source_id,
                role=DataSourceRole.origin,
                connection_id=connection_id,
                created_from=source_data["source_type"],
                title=title,
                parameters=source_data["parameters"],
            )

            # need to check permissions again in case added source refers to an unchecked connection
            # this can only happen when the first source is being added,
            # because we don't support more than one connection in a single ds (see `source_can_be_added`)
            existing_source_id = self._ds.get_single_data_source_id(ignore_source_ids=[source_id])
            if existing_source_id is None:  # dataset is empty
                check_permissions_for_origin_sources(
                    dataset=self._ds,
                    source_ids=[source_id],
                    permission_kind=USPermissionKind.read,
                    us_entry_buffer=self._us_manager.get_entry_buffer(),
                )

        if action in (DatasetAction.update_source, DatasetAction.delete_source):
            dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
            old_title = dsrc_coll.title
            self._ds_ca.validate_component_can_be_managed(component_ref=component_ref, by=by)

        if action in (DatasetAction.update_source, DatasetAction.add_source):
            existing_source_id = source_id if action == DatasetAction.update_source else None
            if not self._capabilities.source_can_be_added(
                connection_id=source_data.get("connection_id"),
                created_from=source_data["source_type"],
                ignore_source_ids=ignore_source_ids or [existing_source_id],  # type: ignore  # TODO: fix
            ):
                raise exc.DLValidationFatal("Source cannot be added to dataset")

        # we have verified that the action is valid, so we can proceed to modifying the dataset

        # clear old errors
        self._ds.error_registry.remove_errors(id=source_id)

        if action == DatasetAction.add_source:
            for field in self._ds.error_registry.for_type(ComponentType.field):
                self._ds.error_registry.remove_errors(id=field.id, code=common_exc.DataSourceNotFound.err_code)
            add_source(title=source_data.get("title") or None)  # type: ignore  # TODO: fix
            self.refresh_data_source(source_id=source_id, old_raw_schema=None)
            new_title = source_data["title"]
            self.add_affected_component(component_ref)

        elif action == DatasetAction.update_source:
            self.add_affected_component(component_ref)
            parameters = source_data.get("parameters") or {}

            dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
            origin_dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            old_raw_schema = origin_dsrc.saved_raw_schema
            new_title = source_data.get("title") or dsrc_coll.title
            self._ds_editor.update_data_source(
                source_id=source_id,
                created_from=source_data.get("source_type"),
                role=DataSourceRole.origin,
                connection_id=source_data.get("connection_id"),
                **parameters,
            )
            if new_title != old_title:
                self._ds_editor.update_data_source_collection(source_id=source_id, title=new_title)

            if (set(source_data) - {"title", "id", "source_type", "parameters"}) or parameters:
                # something besides the title was updated
                self.refresh_data_source(source_id=source_id, old_raw_schema=old_raw_schema)

        elif action == DatasetAction.delete_source:
            self._ds_editor.remove_data_source_collection(source_id=source_id)
            self._reload_sources()

        elif action == DatasetAction.refresh_source:
            dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
            origin_dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            new_title = dsrc_coll.title
            assert origin_dsrc is not None
            self.refresh_data_source(
                source_id=source_id,
                old_raw_schema=origin_dsrc.saved_raw_schema,
                # Just in case some of the fields were erroneously marked as invalid,
                # force-update them if `force_update_fields` is true
                force_update_fields=source_data.get("force_update_fields", False),
            )
            self.add_affected_component(component_ref)

        # check title against conflicts
        self.perform_component_title_validation(component_ref=component_ref, old_title=old_title, new_title=new_title)

    @generic_profiler("validator-apply-avatar-action")
    def apply_source_avatar_action(
        self,
        action: DatasetAction,
        source_avatar_data: dict,
        by: Optional[ManagedBy] = ManagedBy.user,
        disable_fields_update: bool = False,
    ) -> None:
        """Apply update to the data source configuration"""

        source_avatar_data = source_avatar_data.copy()
        avatar_id = source_avatar_data.pop("id")
        source_id = source_avatar_data.get("source_id")
        component_ref = DatasetComponentRef(component_type=ComponentType.source_avatar, component_id=avatar_id)
        old_avatar: Optional[SourceAvatar] = None
        old_raw_schema = None
        new_raw_schema = None
        old_title: Optional[str] = None
        new_title: Optional[str] = None
        do_delete_fields = False
        if source_id is not None:
            source_id = self._remapped_source_ids.get(source_id, source_id)

        update_fields = False

        if action in (DatasetAction.update_source_avatar, DatasetAction.delete_source_avatar):
            old_avatar = self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)
            old_dsrc_coll = self._get_data_source_coll_strict(source_id=old_avatar.source_id)
            old_raw_schema = old_dsrc_coll.get_cached_raw_schema(role=DataSourceRole.origin)
            old_title = old_avatar.title
            self._ds_ca.validate_component_can_be_managed(component_ref=component_ref, by=by)

        # clear old errors
        self._ds.error_registry.remove_errors(id=avatar_id)

        if action == DatasetAction.add_source_avatar:
            self.perform_component_id_validation(component_ref)
            new_title = source_avatar_data["title"]
            self._ds_editor.add_avatar(
                avatar_id=avatar_id,
                source_id=source_id,
                title=new_title,  # type: ignore  # TODO: fix
            )
            new_dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
            new_raw_schema = new_dsrc_coll.get_cached_raw_schema(role=DataSourceRole.origin)
            self.formula_compiler.register_avatar(avatar_id=avatar_id, source_id=source_id)
            update_fields = True

        if action == DatasetAction.update_source_avatar:
            new_title = source_avatar_data.get("title") or old_avatar.title  # type: ignore  # TODO: fix
            if new_title != old_title:
                self._ds_editor.update_avatar(avatar_id=avatar_id, title=new_title)
            if source_id is not None and source_id != old_avatar.source_id:  # type: ignore  # TODO: fix
                new_dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
                self._ds_editor.update_avatar(avatar_id=avatar_id, source_id=source_id)
                new_raw_schema = new_dsrc_coll.get_cached_raw_schema(role=DataSourceRole.origin)
                self.formula_compiler.unregister_avatar(avatar_id=avatar_id)
                self.formula_compiler.register_avatar(avatar_id=avatar_id, source_id=source_id)
                update_fields = True

        elif action == DatasetAction.delete_source_avatar:
            # clean up relations
            left_relations = self._ds_accessor.get_avatar_relation_list(right_avatar_id=avatar_id)
            for relation in left_relations:
                self._ds_editor.remove_avatar_relation(relation_id=relation.id)

            # remove avatar
            self._ds_editor.remove_avatar(avatar_id=avatar_id)
            self.formula_compiler.unregister_avatar(avatar_id=avatar_id)
            update_fields = True
            do_delete_fields = True

        # update result schema fields
        if update_fields and not disable_fields_update:
            self._update_direct_fields_for_updated_raw_schema(
                old_raw_schema=old_raw_schema,  # type: ignore  # TODO: fix
                new_raw_schema=new_raw_schema,
                avatar_ids=[avatar_id],
                do_delete_fields=do_delete_fields,
            )

        # check title against conflicts
        self.perform_component_title_validation(component_ref=component_ref, old_title=old_title, new_title=new_title)

        self.load_exbuilders()

    def _try_generate_conditions_from_columns(
        self,
        left_avatar_id: str,
        right_avatar_id: str,
    ) -> list[BinaryCondition]:
        """Automatically generate condition for first pair of matching field names"""

        left_columns = self._get_data_source_strict(
            source_id=self._ds_accessor.get_avatar_strict(avatar_id=left_avatar_id).source_id,
            role=DataSourceRole.origin,
        ).saved_raw_schema
        right_columns = self._get_data_source_strict(
            source_id=self._ds_accessor.get_avatar_strict(avatar_id=right_avatar_id).source_id,
            role=DataSourceRole.origin,
        ).saved_raw_schema

        if not left_columns or not right_columns:
            if right_columns and not left_columns:
                which = "left"
            elif left_columns and not right_columns:
                which = "right"
            else:
                which = "both"
            LOGGER.warning(f"Can't generate condition: schema for {which} is missing")
            return []

        left_by_name = {col.name: col for col in left_columns}
        right_by_name = {col.name: col for col in right_columns}
        right_by_lower_name = {col.name.lower(): col for col in right_columns}
        column_match = None
        for left_col in sorted(left_by_name):
            # first try to match by exact name,
            # and then - ignore case
            if left_col in right_by_name:
                column_match = (left_by_name[left_col], right_by_name[left_col])
                break
            if left_col.lower() in right_by_lower_name:
                column_match = (left_by_name[left_col], right_by_lower_name[left_col.lower()])
                break
        if column_match:
            return [
                BinaryCondition(
                    left_part=ConditionPartDirect(source=column_match[0].name),
                    right_part=ConditionPartDirect(source=column_match[1].name),
                    operator=BinaryJoinOperator.eq,
                )
            ]
        else:
            return []

    @generic_profiler("validator-apply-relation-action")
    def apply_avatar_relation_action(
        self, action: DatasetAction, avatar_relation_data: dict, by: Optional[ManagedBy] = ManagedBy.user
    ) -> None:
        """Apply update to the data source configuration"""

        avatar_relation_data = avatar_relation_data.copy()
        relation_id = avatar_relation_data.pop("id")
        component_ref = DatasetComponentRef(component_type=ComponentType.avatar_relation, component_id=relation_id)

        if by is not None and action in (DatasetAction.update_avatar_relation, DatasetAction.delete_avatar_relation):
            self._ds_ca.validate_component_can_be_managed(component_ref=component_ref, by=by)

        # clear old errors
        self._ds.error_registry.remove_errors(id=relation_id)

        if action == DatasetAction.add_avatar_relation:
            self.perform_component_id_validation(component_ref)
            conditions = avatar_relation_data.get("conditions")
            left_avatar_id = avatar_relation_data["left_avatar_id"]
            right_avatar_id = avatar_relation_data["right_avatar_id"]
            if not conditions:
                conditions = self._try_generate_conditions_from_columns(
                    left_avatar_id=left_avatar_id, right_avatar_id=right_avatar_id
                )
            self._ds_editor.add_avatar_relation(
                relation_id=relation_id,
                left_avatar_id=left_avatar_id,
                right_avatar_id=right_avatar_id,
                conditions=conditions,
                join_type=avatar_relation_data.get("join_type"),
                required=avatar_relation_data.get("required", False),
            )

        if action == DatasetAction.update_avatar_relation:
            self._ds_editor.update_avatar_relation(
                relation_id=relation_id,
                conditions=avatar_relation_data.get("conditions"),
                join_type=avatar_relation_data.get("join_type"),
                required=avatar_relation_data.get("required", False),
            )

        if action == DatasetAction.delete_avatar_relation:
            self._ds_editor.remove_avatar_relation(relation_id=relation_id)

        if action in (DatasetAction.add_avatar_relation, DatasetAction.update_avatar_relation):
            errors = self._get_relation_errors(
                relation=self._ds_accessor.get_avatar_relation_strict(relation_id=relation_id),
            )
            for error in errors:
                self._ds.error_registry.add_error(
                    id=relation_id,
                    type=ComponentType.avatar_relation,
                    message=error.message,
                    code=error.code,
                    details={},
                )
            self.add_affected_component(component_ref)

        self.load_exbuilders()

    def _migrate_source_parameters(
        self,
        old_connection: Optional[ConnectionBase],
        new_connection: ConnectionBase,
        dsrc: DataSource,
    ) -> tuple[dict, DataSourceType]:
        old_conn_type: ConnectionType
        if old_connection is not None:
            old_conn_type = old_connection.conn_type
        else:
            old_conn_type = dsrc.conn_type

        src_migrator = get_data_source_migrator(conn_type=old_conn_type)
        dst_migrator = get_data_source_migrator(conn_type=new_connection.conn_type)

        migration_dtos = src_migrator.export_migration_dtos(
            data_source_spec=dsrc.spec,
        )
        try:
            new_dsrc_spec = dst_migrator.import_migration_dtos(
                migration_dtos=migration_dtos,
                connection_ref=new_connection.conn_ref,  # type: ignore  # 2024-01-24 # TODO: Argument "connection_ref" to "import_migration_dtos" of "DataSourceMigrator" has incompatible type "ConnectionRef | None"; expected "ConnectionRef"  [arg-type]
            )
        except common_exc.DataSourceMigrationImpossible:
            # Failed to migrate anything.
            # Let's just go with an empty default data source
            return {}, new_connection.source_type  # type: ignore  # 2024-01-24 # TODO: Incompatible return value type (got "tuple[dict[Any, Any], DataSourceType | None]", expected "tuple[dict[Any, Any], DataSourceType]")  [return-value]

        new_dsrc_cls = get_data_source_class(new_dsrc_spec.source_type)
        new_dsrc_dummy = new_dsrc_cls(
            id="",
            spec=new_dsrc_spec,
            us_entry_buffer=self._us_manager.get_entry_buffer(),
            dataset_parameter_values={},
            dataset_template_enabled=False,
        )
        new_dsrc_parameters = new_dsrc_dummy.get_parameters()
        return new_dsrc_parameters, new_dsrc_spec.source_type

    @generic_profiler("validator-apply-connection-action")
    def apply_connection_action(
        self, action: DatasetAction, connection_data: ReplaceConnection, by: Optional[ManagedBy] = ManagedBy.user
    ) -> None:
        assert action == DatasetAction.replace_connection
        old_connection: Optional[ConnectionBase]
        try:
            old_connection_ref = DefaultConnectionRef(conn_id=connection_data.id)
            self._sync_us_manager.ensure_entry_preloaded(old_connection_ref)
            old_connection = self._sync_us_manager.get_loaded_us_connection(old_connection_ref)
        except (common_exc.ReferencedUSEntryNotFound, common_exc.ReferencedUSEntryAccessDenied) as e:
            LOGGER.info(f'Failed to get the old connection, reason: "{e}"')
            old_connection = None

        new_connection_ref = DefaultConnectionRef(conn_id=connection_data.new_id)
        self._sync_us_manager.ensure_entry_preloaded(new_connection_ref)
        new_connection = self._sync_us_manager.get_loaded_us_connection(new_connection_ref)
        utils.need_permission_on_entry(new_connection, USPermissionKind.read)

        updates = []
        ignore_source_ids = []  # exclude all the sources that are going to be updated from compatibility checks
        for source_id in self._ds_accessor.get_data_source_id_list():
            dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
            dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            connection_ref = dsrc.connection_ref
            if not isinstance(connection_ref, DefaultConnectionRef):
                raise TypeError(f"Unexpected connection_ref type: {type(connection_ref)}")
            if connection_ref.conn_id == connection_data.id:
                parameters, new_source_type = self._migrate_source_parameters(
                    old_connection=old_connection,
                    new_connection=new_connection,
                    dsrc=dsrc,
                )

                # migrate parameters for new source type
                updates.append(
                    dict(
                        id=dsrc_coll.id,
                        connection_id=connection_data.new_id,
                        source_type=new_source_type,
                        parameters=parameters,
                    )
                )
                ignore_source_ids.append(dsrc_coll.id)

        LOGGER.info(f"Found {len(updates)} sources to update")
        for dsrc_data in updates:
            LOGGER.info(
                f'Going to update source {dsrc_data["id"]} as {dsrc_data["source_type"].name} '  # type: ignore  # 2024-01-24 # TODO: "object" has no attribute "name"  [attr-defined]
                f"for connection {connection_data.new_id}"
            )
            self.apply_source_action(
                action=DatasetAction.update_source,
                source_data=dsrc_data,
                by=by,
                ignore_source_ids=ignore_source_ids,
            )

    @generic_profiler("validator-apply-obligatory-filter-action")
    def apply_obligatory_filter_action(
        self,
        action: DatasetAction,
        filter_data: ObligatoryFilterBase,
        by: Optional[ManagedBy] = ManagedBy.user,
    ) -> None:
        assert isinstance(filter_data, ObligatoryFilterBase)
        component_ref = DatasetComponentRef(component_type=ComponentType.obligatory_filter, component_id=filter_data.id)

        if action in (DatasetAction.add_obligatory_filter, DatasetAction.update_obligatory_filter):
            assert isinstance(filter_data, AddUpdateObligatoryFilter)
            if action == DatasetAction.update_obligatory_filter:
                filter_object = self._ds_accessor.get_obligatory_filter_strict(obfilter_id=filter_data.id)
                field_guid = filter_object.field_guid
            else:
                assert isinstance(filter_data.field_guid, str)
                self.perform_component_id_validation(component_ref)
                field_guid = filter_data.field_guid

            field = self.get_field_by_id(field_guid)  # Validating field guid
            default_where_clauses = []
            allowed_filter_types = FILTERS_BY_TYPE[enum_not_none(field.cast)]
            for default_filter in filter_data.default_filters:
                clause = DefaultWhereClause(operation=default_filter.operation, values=default_filter.values)
                if clause.operation not in allowed_filter_types:
                    raise common_exc.DatasetConfigurationError(
                        f"Filtration type {clause.operation.name} is not allowed "
                        f"for field type {enum_not_none(field.cast).name}"
                    )
                default_where_clauses.append(clause)

            if action == DatasetAction.add_obligatory_filter:
                valid = filter_data.valid if filter_data.valid is not None else True
                assert valid is not None
                self._ds_editor.add_obligatory_filter(
                    obfilter_id=filter_data.id,
                    field_guid=field_guid,
                    default_filters=default_where_clauses,
                    managed_by=by,
                    valid=valid,
                )
            else:
                self._ds_editor.update_obligatory_filter(
                    obfilter_id=filter_data.id,
                    default_filters=default_where_clauses,
                    valid=filter_data.valid,
                )

        if action == DatasetAction.delete_obligatory_filter:
            assert isinstance(filter_data, DeleteObligatoryFilter)
            self._ds_editor.remove_obligatory_filter(obfilter_id=filter_data.id)

    @generic_profiler("validator-apply-setting-action")
    def apply_setting_action(
        self,
        action: DatasetAction,
        setting: UpdateSettingAction.Setting,
        by: ManagedBy | None = ManagedBy.user,
    ) -> None:
        if action == DatasetAction.update_setting:
            if setting.name == DatasetSettingName.load_preview_by_default:
                self._ds_editor.set_load_preview_by_default(setting.value)
            elif setting.name == DatasetSettingName.template_enabled:
                self._ds_editor.set_template_enabled(setting.value)
            elif setting.name == DatasetSettingName.data_export_forbidden:
                self._ds_editor.set_data_export_forbidden(setting.value)
            else:
                raise NotImplementedError(f"Not implemented setting name: {setting.name}")
        else:
            raise NotImplementedError(f"Not implemented setting action: {action}")

    @generic_profiler("validator-get-single-formula-errors")
    def get_single_formula_errors(self, formula: str, feature_errors: bool = False) -> list[ErrorInfo]:
        if not self._has_sources:
            errors = [FormulaErrorCtx(message="No data sources have been defined", level=MessageLevel.ERROR)]

        else:
            formula_errors = self._get_formula_errors(formula, feature_errors=feature_errors)
            if len(formula_errors) == 1:
                # Hide error for empty formulas
                if formula_errors[0].is_sub_error(code=formula_exc.ParseEmptyFormulaError.default_code):
                    formula_errors = []

            errors = [
                FormulaErrorInfo(  # type: ignore  # TODO: fix
                    code=err.code,
                    message=err.message,
                    level=comp_err_level_from_formula_err_level(err.level),
                    row=err.coords[0],
                    column=err.coords[1],
                    token=err.token,
                )
                for err in formula_errors
            ]

        return errors  # type: ignore  # TODO: fix

    def collect_nonexistent_connection_errors(self) -> None:
        for source_id in self._ds_accessor.get_data_source_id_list():
            dsrc_coll = self._get_data_source_coll_strict(source_id=source_id)
            dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            try:
                self._sync_us_manager.ensure_entry_preloaded(dsrc.connection_ref)
            except common_exc.ReferencedUSEntryNotFound:
                self._ds.error_registry.add_error(
                    id=dsrc_coll.id,
                    type=ComponentType.data_source,
                    message=f"Connection for {dsrc_coll.title} not found",
                    code=common_exc.ReferencedUSEntryNotFound.err_code,
                    details={},
                )

    def _find_phantom_error_refs(self) -> list[DatasetComponentRef]:
        all_error_refs = [
            DatasetComponentRef(component_type=item.type, component_id=item.id)
            for item in self._ds.error_registry.items
        ]
        phantom_refs: list[DatasetComponentRef] = []
        for component_ref in all_error_refs:
            if self._ds_ca.get_component(component_ref=component_ref) is None:
                phantom_refs.append(component_ref)

        return phantom_refs

    def _remove_phantom_errors_for_refs(self, phantom_refs: list[DatasetComponentRef]) -> None:
        for ref in phantom_refs:
            self._ds.error_registry.remove_errors(id=ref.component_id)

    def find_and_remove_phantom_error_refs(self) -> None:
        phantom_error_refs = self._find_phantom_error_refs()
        if phantom_error_refs:
            LOGGER.warning(
                f"Detected phantom errors during dataset validation: {phantom_error_refs},"
                " removing from component errors"
            )

        self._remove_phantom_errors_for_refs(phantom_error_refs)
