from typing import Optional, Sequence

import attr

from bi_external_api.attrs_model_mapper import Processor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.converter_exc_composer import ConversionErrHandlingContext
from bi_external_api.converter.data_source import (
    convert_external_dsrc_to_add_action,
    convert_internal_datasource_to_external_data_source,
)
from bi_external_api.converter.dataset_field import DatasetFieldConverter
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import datasets


@attr.s
class DatasetConverter:
    _wb_context: WorkbookContext = attr.ib()
    _converter_ctx: ConverterContext = attr.ib()

    @classmethod
    def fill_defaults_for_avatar_configs(cls, ext_dataset: ext.Dataset) -> ext.Dataset:
        if ext_dataset.avatars is None:
            single_source: ext.DataSource

            if len(ext_dataset.sources) == 1:
                single_source = ext_dataset.sources[0]
            else:
                raise ValueError("Avatars config can not be defined only if dataset contains single source")

            avatars_config = ext.AvatarsConfig(
                definitions=(
                    ext.AvatarDef(id=single_source.id, source_id=single_source.id, title=single_source.title),
                ),
                root=single_source.id,
                joins=()
            )

            return attr.evolve(
                ext_dataset,
                avatars=avatars_config
            )

        return ext_dataset

    @attr.s(frozen=True)
    class FieldDefaulter(Processor[ext.DatasetField]):
        single_avatar_id: Optional[str] = attr.ib()

        def _should_process(self, meta: FieldMeta) -> bool:
            return issubclass(meta.clz, ext.DatasetField)

        def _process_single_object(self, obj: ext.DatasetField, meta: FieldMeta) -> ext.DatasetField:
            accumulator = obj

            orig_calc_spec = obj.calc_spec

            if isinstance(orig_calc_spec, ext.DirectCS):
                if orig_calc_spec.avatar_id is None and self.single_avatar_id is not None:
                    accumulator = attr.evolve(
                        accumulator,
                        calc_spec=attr.evolve(
                            accumulator.calc_spec,
                            avatar_id=self.single_avatar_id
                        ),
                    )
            if accumulator.aggregation is None:
                accumulator = attr.evolve(accumulator, aggregation=ext.Aggregation.none)
            return accumulator

    @classmethod
    def fill_defaults(cls, ext_dataset: ext.Dataset) -> ext.Dataset:
        normalized_ext_dataset = cls.fill_defaults_for_avatar_configs(ext_dataset)

        avatars_config = normalized_ext_dataset.strict_avatars

        normalized_ext_dataset = cls.FieldDefaulter(
            single_avatar_id=avatars_config.definitions[0].id if len(avatars_config.definitions) == 1 else None
        ).process(normalized_ext_dataset)
        return normalized_ext_dataset

    def convert_public_dataset_to_actions(
            self,
            dataset: ext.Dataset,
            generate_direct_fields: bool = False,
    ) -> Sequence[datasets.Action]:
        """
        Generates actions sequence that should create a dataset from scratch according to public definition.
        """
        actions: list[datasets.Action] = []

        for ext_source in dataset.sources:
            actions.append(convert_external_dsrc_to_add_action(ext_source, wb_context=self._wb_context))

        for avatar_def in dataset.strict_avatars.definitions:
            avatar = datasets.Avatar(
                id=avatar_def.id,
                is_root=(avatar_def.id == dataset.strict_avatars.root),
                source_id=avatar_def.source_id,
                title=avatar_def.title,
            )

            actions.append(datasets.ActionAvatarAdd(
                source_avatar=avatar,
                disable_fields_update=not generate_direct_fields,
            ))

        with ConversionErrHandlingContext(current_path=["fields"]).cm() as exc_hdr:
            with exc_hdr.postpone_error_in_current_path():
                actions.extend(DatasetFieldConverter.convert_ext_fields_to_actions(dataset.fields))

        return actions

    def convert_internal_dataset_to_public_dataset(
        self,
        internal_dataset: datasets.Dataset,
    ) -> ext.Dataset:
        """
        Converts internal representation of dataset into public dataset definition.
        """
        sources = [
            convert_internal_datasource_to_external_data_source(dsrc, wb_context=self._wb_context) for dsrc
            in internal_dataset.sources
        ]

        fields = tuple(
            DatasetFieldConverter.convert_int_field_to_ext_field(int_field, self._converter_ctx)
            for int_field in internal_dataset.result_schema
        )

        root_avatar_id = next(
            int_avatar.id
            for int_avatar in internal_dataset.source_avatars
            if int_avatar.is_root
        )
        avatars = tuple(
            ext.AvatarDef(
                id=int_avatar.id,
                title=int_avatar.title,
                source_id=int_avatar.source_id,
            )
            for int_avatar in internal_dataset.source_avatars
        )
        # TODO FIX: Add support when models for join conditions will be ready
        assert len(avatars) == 1, "Multi-avatars not yet supported"

        avatars_config = ext.AvatarsConfig(
            definitions=avatars,
            joins=(),
            root=root_avatar_id
        )

        return ext.Dataset(
            fields=fields,
            avatars=avatars_config,
            sources=sources,
        )
