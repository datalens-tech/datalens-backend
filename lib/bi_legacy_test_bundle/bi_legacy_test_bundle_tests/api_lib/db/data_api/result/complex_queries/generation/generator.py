from __future__ import annotations

import random
from typing import Any, Sequence

import attr

from dl_constants.enums import WhereClauseOperation


@attr.s
class AutoGeneratorSettings:
    dimensions: Sequence[str] = attr.ib(kw_only=True)
    dates: frozenset[str] = attr.ib(kw_only=True, factory=frozenset)
    filters: dict[str, tuple[WhereClauseOperation, list[str]]] = attr.ib(kw_only=True)
    dimension_cnts: Sequence[int] = attr.ib(kw_only=True)
    aggregations: Sequence[str] = attr.ib(kw_only=True)
    formula_cnts: Sequence[int] = attr.ib(kw_only=True)
    measure_base_expressions: Sequence[str] = attr.ib(kw_only=True)
    filter_probability: float = attr.ib(kw_only=True)
    bfb_probability: float = attr.ib(kw_only=True)
    lookup_probability: float = attr.ib(kw_only=True)


@attr.s(frozen=True)
class FormulaTemplate:
    template_str: str = attr.ib()
    placeholder: str = attr.ib(kw_only=True)

    def wrap(self, nested_formula: str) -> str:
        return self.template_str.format(**{self.placeholder: nested_formula})


@attr.s(frozen=True)
class FormulaRecursionState:
    agg_level: int = attr.ib(kw_only=True)
    effective_dims: list[str] = attr.ib(kw_only=True)
    remaining_dims: list[str] = attr.ib(kw_only=True)
    available_filters: frozenset[str] = attr.ib(kw_only=True, factory=frozenset)
    top_level: bool = attr.ib(kw_only=True, default=False)

    def clone(self, **kwargs: Any) -> FormulaRecursionState:
        return attr.evolve(self, **kwargs)

    def pop_agg_level(self) -> FormulaRecursionState:
        assert self.agg_level > 0
        return self.clone(top_level=False, agg_level=self.agg_level - 1)

    def pop_dimension(self) -> tuple[FormulaRecursionState, str]:
        assert self.remaining_dims
        chosen_dim = random.choice(self.remaining_dims)
        new_effective_dims = self.effective_dims + [chosen_dim]
        new_remaining_dims = [dim for dim in self.remaining_dims if dim != chosen_dim]
        new_state = self.clone(
            remaining_dims=new_remaining_dims,
            effective_dims=new_effective_dims,
        )
        return new_state, chosen_dim

    def pop_bfb_field(self) -> tuple[FormulaRecursionState, str]:
        bfb_field = next(iter(self.available_filters))
        new_available_filters = self.available_filters - frozenset((bfb_field,))
        return self.clone(available_filters=new_available_filters), bfb_field


@attr.s
class TestSettings:
    base_dimensions: Sequence[str] = attr.ib(kw_only=True)
    measure_formulas: Sequence[str] = attr.ib(kw_only=True)
    filters: dict[str, tuple[WhereClauseOperation, list[str]]] = attr.ib(kw_only=True, factory=dict)

    def serialize(self) -> dict:
        return {
            'base_dimensions': list(self.base_dimensions),
            'measure_formulas': list(self.measure_formulas),
            'filters': {
                name: {'op': op.name, 'values': values}
                for name, (op, values) in self.filters.items()
            },
        }

    @classmethod
    def deserialize(cls, data: dict) -> TestSettings:
        return cls(
            base_dimensions=data['base_dimensions'],
            measure_formulas=data['measure_formulas'],
            filters={
                name: (WhereClauseOperation[params['op']], params['values'])
                for name, params in data['filters'].items()
            }
        )


@attr.s
class LODTestAutoGenerator:
    settings: AutoGeneratorSettings = attr.ib(kw_only=True)

    def _match_probability(self, value: float) -> bool:
        return random.random() <= value

    def _generate_formula_iteration_generic_func(
            self, recursion_state: FormulaRecursionState,
            func_name: str, add_args: Sequence[str] = (),
            agg_level: bool = False,
            lod: bool = False,
            bfb: bool = False,
            igdim: bool = False,
    ) -> tuple[FormulaTemplate, FormulaRecursionState]:
        top_level = recursion_state.top_level

        if agg_level:
            recursion_state = recursion_state.pop_agg_level()

        lod_str: str = ''
        if lod and not top_level:
            recursion_state, chosen_dim = recursion_state.pop_dimension()
            lod_str = f' INCLUDE [{chosen_dim}]'

        bfb_str: str = ''
        if bfb and recursion_state.available_filters:
            if self._match_probability(self.settings.bfb_probability):
                recursion_state, bfb_field = recursion_state.pop_bfb_field()
                bfb_str = f' BEFORE FILTER BY [{bfb_field}]'

        add_args_str = (', ' + ', '.join(add_args)) if add_args else ''
        formula_tmpl = FormulaTemplate(
            f'{func_name}({{nested_formula}}{add_args_str}{lod_str}{bfb_str})',
            placeholder='nested_formula',
        )
        return formula_tmpl, recursion_state

    def _generate_formula_iteration_agg(
            self, recursion_state: FormulaRecursionState,
    ) -> tuple[FormulaTemplate, FormulaRecursionState]:
        return self._generate_formula_iteration_generic_func(
            recursion_state=recursion_state,
            func_name=random.choice(self.settings.aggregations),
            agg_level=True, lod=True, bfb=True,
        )

    def _generate_formula_iteration_lookup(
            self, recursion_state: FormulaRecursionState,
    ) -> tuple[FormulaTemplate, FormulaRecursionState]:
        date_dims = list(set(recursion_state.effective_dims) & self.settings.dates)
        assert date_dims
        return self._generate_formula_iteration_generic_func(
            recursion_state=recursion_state,
            func_name='AGO', add_args=[f'[{random.choice(date_dims)}]'],
            bfb=True,
        )

    def _generate_formula_iteration(
            self, recursion_state: FormulaRecursionState,
    ) -> tuple[FormulaTemplate, FormulaRecursionState]:
        date_dims = set(recursion_state.effective_dims) & self.settings.dates
        if date_dims and recursion_state.agg_level > 0 and self._match_probability(self.settings.lookup_probability):
            return self._generate_formula_iteration_lookup(recursion_state=recursion_state)

        return self._generate_formula_iteration_agg(recursion_state=recursion_state)

    def generate_formula(self, recursion_state: FormulaRecursionState) -> str:
        if recursion_state.agg_level == 0:
            return random.choice(self.settings.measure_base_expressions)

        formula_tmpl, child_recursion_state = self._generate_formula_iteration(recursion_state=recursion_state)
        nested_formula = self.generate_formula(recursion_state=child_recursion_state)
        formula = formula_tmpl.wrap(nested_formula)
        return formula

    def generate_test_settings(self) -> TestSettings:
        dimension_cnt = random.choice(self.settings.dimension_cnts)
        base_dimensions = random.sample(self.settings.dimensions, k=dimension_cnt)
        effective_dims = [dim for dim in self.settings.dimensions if dim in base_dimensions]
        remaining_dims = [dim for dim in self.settings.dimensions if dim not in base_dimensions]
        formula_cnt = random.choice(self.settings.formula_cnts)

        # Generate filters
        chosen_filters: dict[str, tuple[WhereClauseOperation, list[str]]] = {}
        for name, params in self.settings.filters.items():
            if random.random() <= self.settings.filter_probability:
                chosen_filters[name] = params

        recursion_state = FormulaRecursionState(
            effective_dims=effective_dims,
            remaining_dims=remaining_dims,
            available_filters=frozenset(chosen_filters.keys()),
            top_level=True,
            agg_level=random.randrange(1, len(remaining_dims) + 1),
        )

        measure_formulas = [
            self.generate_formula(recursion_state=recursion_state)
            for i in range(formula_cnt)
        ]

        test_settings = TestSettings(
            base_dimensions=base_dimensions,
            measure_formulas=measure_formulas,
            filters=chosen_filters,
        )
        return test_settings

    def generate_setting_list(self, test_cnt: int) -> list[TestSettings]:
        return [self.generate_test_settings() for i in range(test_cnt)]
