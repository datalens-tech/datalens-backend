from __future__ import annotations

import abc
from typing import Dict, Sequence, Tuple

import attr

from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.slicing.env import SlicerEnvironment
from bi_formula.slicing.slicer import FormulaSlicer
from bi_formula.slicing.schema import (
    SliceSchema, WindowFunctionLevelBoundary, TopLevelBoundary,
    NonFieldsBoundary, LevelBoundary, NestedLevelTaggedBoundary,
)
from bi_formula.core.tag import LevelTag

from bi_query_processing.legacy_pipeline.planning.primitives import (
    SlicerType, SlicerConfiguration, TaggedSlicerConfiguration, SlicingPlan
)
from bi_query_processing.utils.name_gen import NameGen


@attr.s
class SlicerFactoryBase(abc.ABC):
    _boundaries: Dict[Tuple[int, SlicerConfiguration, str], LevelBoundary] = attr.ib(init=False, factory=dict)
    _slicers: Dict[Tuple[SlicingPlan, str], FormulaSlicer] = attr.ib(init=False, factory=dict)

    @abc.abstractmethod
    def _make_boundary(
            self, level_idx: int, slicer_config: SlicerConfiguration, iteration_id: str
    ) -> LevelBoundary:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_slicer(self, slicing_plan: SlicingPlan, iteration_id: str) -> FormulaSlicer:
        raise NotImplementedError

    def get_boundary(
            self, level_idx: int, slicer_config: SlicerConfiguration, iteration_id: str,
    ) -> LevelBoundary:
        key = (level_idx, slicer_config, iteration_id)
        if key not in self._boundaries:
            self._boundaries[key] = self._make_boundary(
                level_idx=level_idx, slicer_config=slicer_config,
                iteration_id=iteration_id,
            )
        return self._boundaries[key]

    def get_slicer(self, slicing_plan: SlicingPlan, iteration_id: str) -> FormulaSlicer:
        key = (slicing_plan, iteration_id)
        if key not in self._slicers:
            self._slicers[key] = self._make_slicer(
                slicing_plan=slicing_plan, iteration_id=iteration_id,
            )
        return self._slicers[key]


@attr.s
class SlicerFactory(SlicerFactoryBase):
    _inspect_env: InspectionEnvironment = attr.ib(kw_only=True)
    _level_names: Sequence[str] = attr.ib(kw_only=True, default='abcdefghijklmnopqrstuvwxyz')
    _name_gen: NameGen = attr.ib(init=False, factory=NameGen)
    _slicer_env_cache: Dict[str, SlicerEnvironment] = attr.ib(init=False, factory=dict)

    def _make_boundary(
            self, level_idx: int, slicer_config: SlicerConfiguration, iteration_id: str
    ) -> LevelBoundary:
        name = f'{self._level_names[level_idx]}_{iteration_id}'
        if slicer_config.slicer_type == SlicerType.top:
            return TopLevelBoundary(name=name, name_gen=self._name_gen)
        if slicer_config.slicer_type == SlicerType.window:
            return WindowFunctionLevelBoundary(name=name, name_gen=self._name_gen)
        if slicer_config.slicer_type == SlicerType.fields:
            return NonFieldsBoundary(name=name, name_gen=self._name_gen)
        if slicer_config.slicer_type == SlicerType.level_tagged:
            assert isinstance(slicer_config, TaggedSlicerConfiguration)
            assert isinstance(slicer_config.tag, LevelTag)
            return NestedLevelTaggedBoundary(name=name, name_gen=self._name_gen, tag=slicer_config.tag)
        raise ValueError(f'Invalid boundary specifier: {slicer_config}')

    def _get_slicer_env(self, iteration_id: str) -> SlicerEnvironment:
        if iteration_id not in self._slicer_env_cache:
            self._slicer_env_cache[iteration_id] = SlicerEnvironment(inspect_env=self._inspect_env)
        return self._slicer_env_cache[iteration_id]

    def _make_slicer(self, slicing_plan: SlicingPlan, iteration_id: str) -> FormulaSlicer:
        return FormulaSlicer(
            slice_schema=SliceSchema(
                levels=[
                    self.get_boundary(
                        level_idx=level_idx, slicer_config=slicer_config,
                        iteration_id=iteration_id,
                    )
                    for level_idx, slicer_config in enumerate(slicing_plan.slicer_configs)
                ]
            ),
            env=self._get_slicer_env(iteration_id=iteration_id),
        )
