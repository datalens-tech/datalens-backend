import enum
from typing import Sequence, Optional, Any

import attr
from bi_external_api.attrs_model_mapper import Processor
from bi_external_api.attrs_model_mapper.base import AttribDescriptor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.attrs_model_mapper.utils import unwrap_container_stack_with_single_type
from bi_external_api.domain.utils import ensure_tuple, ensure_tuple_of_tuples


class ModelTags(enum.Enum):
    the_x = enum.auto()


@attr.s(frozen=True, kw_only=True)
class Point:
    name: Optional[str] = attr.ib(default=None)
    x: int = attr.ib(metadata=AttribDescriptor(tags=frozenset({ModelTags.the_x})).to_meta())
    y: int = attr.ib()


@attr.s(frozen=True, kw_only=True)
class Polygon:
    name: Optional[str] = attr.ib(default=None)
    points: Sequence[Point] = attr.ib(converter=ensure_tuple)


@attr.s(frozen=True)
class BigModel:
    name: str = attr.ib()

    main_polygon: Polygon = attr.ib()
    optional_point: Optional[Point] = attr.ib()

    list_of_lists_of_point: Sequence[Sequence[Point]] = attr.ib(converter=ensure_tuple_of_tuples)
    list_of_polygons: Sequence[Polygon] = attr.ib(converter=ensure_tuple)


INITIAL_INSTANCE = BigModel(
    name="Ololo",

    main_polygon=Polygon(name="always has been", points=[
        Point(x=2, y=2, name="Edge of the Earth"),
        Point(x=3, y=3)
    ]),
    optional_point=None,

    list_of_lists_of_point=[
        [Point(x=1, y=1)],
        [Point(x=1, y=1), Point(x=2, y=2), Point(x=3, y=3)],
    ],
    list_of_polygons=[]
)


def test_unwrap_container_stack_with_single_type():
    assert unwrap_container_stack_with_single_type(list[Optional[str]]) == ((list, Optional), str)
    assert unwrap_container_stack_with_single_type(Polygon) == ((), Polygon)
    assert unwrap_container_stack_with_single_type(list[list[Point]]) == ((list, list), Point)


def test_field_meta_pop_container():
    container_stack, _ = unwrap_container_stack_with_single_type(list[Point])
    fm = FieldMeta(Point, "foobar", container_stack, None)

    container, sub_fm = fm.pop_container()
    assert container == list
    assert sub_fm == FieldMeta(Point, "foobar", (), None)

    sub_container, sub_sub_fm = sub_fm.pop_container()
    assert sub_container is None
    assert sub_sub_fm == sub_fm


def test_no_changes():
    class EqualityProcessor(Processor[Any]):
        def _should_process(self, meta: FieldMeta) -> bool:
            return True

        def _process_single_object(self, obj: Any, meta: FieldMeta) -> Optional[Any]:
            return obj

    processor = EqualityProcessor()
    result = processor.process(INITIAL_INSTANCE)

    assert result is INITIAL_INSTANCE


def test_change_all_names():
    def process_string(i: Optional[str]) -> str:
        if i is None:
            return "N/A"
        return f"The great {i}"

    class NamePrependProcessor(Processor[str]):
        def _should_process(self, meta: FieldMeta) -> bool:
            return meta.clz == str and meta.attrib_name == "name"

        def _process_single_object(self, obj: Optional[str], meta: FieldMeta) -> Optional[str]:
            return process_string(obj)

    processor = NamePrependProcessor()
    result = processor.process(INITIAL_INSTANCE)

    def rename_point(p: Optional[Point]) -> Optional[Point]:
        if p is None:
            return None
        return attr.evolve(p, name=process_string(p.name))

    def rename_polygon(poly: Optional[Polygon]) -> Optional[Polygon]:
        if poly is None:
            return None
        return attr.evolve(poly, name=process_string(poly.name), points=[rename_point(p) for p in poly.points])

    assert result == attr.evolve(
        INITIAL_INSTANCE,
        name=process_string(INITIAL_INSTANCE.name),

        main_polygon=rename_polygon(INITIAL_INSTANCE.main_polygon),
        optional_point=rename_point(INITIAL_INSTANCE.optional_point),

        list_of_lists_of_point=[
            [rename_point(p) for p in lp]
            for lp in INITIAL_INSTANCE.list_of_lists_of_point
        ],
        list_of_polygons=[
            rename_polygon(poly)
            for poly in INITIAL_INSTANCE.list_of_polygons
        ],
    )


def test_change_by_tag():
    new_x_value = 100500

    class TagProcessor(Processor[str]):
        def _should_process(self, meta: FieldMeta) -> bool:
            if meta.attrib_descriptor is not None:
                return ModelTags.the_x in meta.attrib_descriptor.tags

            return False

        def _process_single_object(self, obj: Optional[int], meta: FieldMeta) -> Optional[int]:
            return new_x_value

    processor = TagProcessor()
    result = processor.process(INITIAL_INSTANCE)

    def process_point(p: Optional[Point]) -> Optional[Point]:
        if p is None:
            return None
        return attr.evolve(p, x=100500)

    def process_polygon(poly: Optional[Polygon]) -> Optional[Polygon]:
        if poly is None:
            return None
        return attr.evolve(poly, points=[process_point(p) for p in poly.points])

    assert result == attr.evolve(
        INITIAL_INSTANCE,

        main_polygon=process_polygon(INITIAL_INSTANCE.main_polygon),
        optional_point=process_point(INITIAL_INSTANCE.optional_point),

        list_of_lists_of_point=[
            [process_point(p) for p in lp]
            for lp in INITIAL_INSTANCE.list_of_lists_of_point
        ],
        list_of_polygons=[
            process_polygon(poly)
            for poly in INITIAL_INSTANCE.list_of_polygons
        ],
    )
