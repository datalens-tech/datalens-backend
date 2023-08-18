from typing import Optional, ClassVar

import attr

from bi_external_api.converter.converter_exc import NotSupportedYet, MalformedEntryConfig
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import charts


class MeasureColoringSpecConverter:
    THRESHOLD_MODE_AUTO: ClassVar[str] = "auto"
    THRESHOLD_MODE_MANUAL: ClassVar[str] = "manual"

    GRAD_MODE_2: ClassVar[str] = "2-point"
    GRAD_MODE_3: ClassVar[str] = "3-point"

    @attr.s(auto_attribs=True)
    class MeaningIntFields:
        gradient_mode: Optional[str]
        palette: Optional[str]
        thresholds_mode: Optional[str]
        left_threshold: Optional[str]
        middle_threshold: Optional[str]
        right_threshold: Optional[str]

        @staticmethod
        def convert_to_float_strict(val: Optional[str]) -> float:
            if val is None:
                raise MalformedEntryConfig("Got NULL for float value")
            try:
                return float(val)
            except Exception:
                raise MalformedEntryConfig(f"Got unexpected value for float field: {val!r}")

        @property
        def left_threshold_float_strict(self) -> float:
            return self.convert_to_float_strict(self.left_threshold)

        @property
        def middle_threshold_float_strict(self) -> float:
            return self.convert_to_float_strict(self.middle_threshold)

        @property
        def right_threshold_float_strict(self) -> float:
            return self.convert_to_float_strict(self.right_threshold)

        @classmethod
        def from_color_config(cls, config: charts.ColorConfig) -> "MeasureColoringSpecConverter.MeaningIntFields":
            return cls(
                gradient_mode=config.gradientMode,
                palette=config.gradientPalette,
                thresholds_mode=config.thresholdsMode,
                left_threshold=config.leftThreshold,
                middle_threshold=config.middleThreshold,
                right_threshold=config.rightThreshold,
            )

        def apply_to_color_config(self, config: charts.ColorConfig) -> charts.ColorConfig:
            return attr.evolve(
                config,
                gradientPalette=self.palette,
                gradientMode=self.gradient_mode,
                thresholdsMode=self.thresholds_mode,
                leftThreshold=self.left_threshold,
                middleThreshold=self.middle_threshold,
                rightThreshold=self.right_threshold,
            )

    @classmethod
    def get_threshold_mode(cls, *, grad_spec_presented: bool) -> str:
        return cls.THRESHOLD_MODE_MANUAL if grad_spec_presented else cls.THRESHOLD_MODE_AUTO

    @classmethod
    def convert_ext_to_int(
            cls,
            ext_spec: Optional[ext.MeasureColoringSpec],
            existing_config: Optional[charts.ColorConfig] = None,
    ) -> charts.ColorConfig:
        target_int_config: charts.ColorConfig = (
            existing_config
            if existing_config is not None
            else charts.ColorConfig(
                coloredByMeasure=True,
            )
        )

        meaning_fields: "MeasureColoringSpecConverter.MeaningIntFields"
        # Processing
        if ext_spec is None:
            meaning_fields = cls.MeaningIntFields(
                gradient_mode=None,
                palette=None,
                thresholds_mode=None,
                left_threshold=None,
                middle_threshold=None,
                right_threshold=None,
            )
        elif isinstance(ext_spec, ext.Gradient2):
            meaning_fields = cls.MeaningIntFields(
                palette=ext_spec.palette,
                gradient_mode=cls.GRAD_MODE_2,
                thresholds_mode=cls.get_threshold_mode(grad_spec_presented=(ext_spec.thresholds is not None)),
                #
                left_threshold=str(ext_spec.thresholds.left) if ext_spec.thresholds is not None else None,
                middle_threshold=None,
                right_threshold=str(ext_spec.thresholds.right) if ext_spec.thresholds is not None else None,
            )
        elif isinstance(ext_spec, ext.Gradient3):
            meaning_fields = cls.MeaningIntFields(
                palette=ext_spec.palette,
                gradient_mode=cls.GRAD_MODE_3,
                thresholds_mode=cls.get_threshold_mode(grad_spec_presented=(ext_spec.thresholds is not None)),
                #
                left_threshold=str(ext_spec.thresholds.left) if ext_spec.thresholds is not None else None,
                middle_threshold=str(ext_spec.thresholds.middle) if ext_spec.thresholds is not None else None,
                right_threshold=str(ext_spec.thresholds.right) if ext_spec.thresholds is not None else None,
            )
        else:
            raise NotSupportedYet(f"Unsupported measure color spec kind: {ext_spec.kind}")

        return meaning_fields.apply_to_color_config(target_int_config)

    @classmethod
    def convert_int_to_ext(cls, int_config: Optional[charts.ColorConfig]) -> ext.MeasureColoringSpec:
        # By default charts does not store any information in colors config
        # And apply 2-points gradient
        if int_config is None or int_config.gradientMode is None:
            return ext.Gradient2(palette=None)

        gradient_mode = int_config.gradientMode
        meaning_fields = cls.MeaningIntFields.from_color_config(int_config)

        if gradient_mode == cls.GRAD_MODE_2:
            return ext.Gradient2(
                palette=meaning_fields.palette,
                thresholds=ext.Thresholds2(
                    left=meaning_fields.left_threshold_float_strict,
                    right=meaning_fields.right_threshold_float_strict,
                ) if meaning_fields.thresholds_mode == cls.THRESHOLD_MODE_MANUAL else None
            )
        elif gradient_mode == cls.GRAD_MODE_3:
            return ext.Gradient3(
                palette=meaning_fields.palette,
                thresholds=ext.Thresholds3(
                    left=meaning_fields.left_threshold_float_strict,
                    middle=meaning_fields.middle_threshold_float_strict,
                    right=meaning_fields.right_threshold_float_strict,
                ) if meaning_fields.thresholds_mode == cls.THRESHOLD_MODE_MANUAL else None
            )
        else:
            raise MalformedEntryConfig(f"Unexpected gradientMode in chart: {gradient_mode!r}")
