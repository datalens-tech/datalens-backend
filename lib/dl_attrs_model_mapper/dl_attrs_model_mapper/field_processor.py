import abc
from typing import (
    Any,
    Generic,
    Optional,
    Sequence,
    Type,
    TypeVar,
    cast,
)

import attr

from dl_attrs_model_mapper.base import AttribDescriptor
from dl_attrs_model_mapper.utils import (
    is_sequence,
    is_str_mapping,
    unwrap_container_stack_with_single_type,
)


_TARGET_TV = TypeVar("_TARGET_TV")
_PROCESSING_OBJECT_TV = TypeVar("_PROCESSING_OBJECT_TV")


@attr.s(frozen=True)
class FieldMeta:
    clz: Type = attr.ib()
    attrib_name: str = attr.ib()
    container_stack: Sequence[Any] = attr.ib()
    attrib_descriptor: Optional[AttribDescriptor] = attr.ib()

    def pop_container(self) -> tuple[Any, "FieldMeta"]:
        if len(self.container_stack) == 0:
            return None, self
        return self.container_stack[0], attr.evolve(self, container_stack=self.container_stack[1:])


# TODO FIX: Split into planing & execution
class Processor(Generic[_PROCESSING_OBJECT_TV]):
    """
    This generic is intended for creation of processor class,
     with a `.process` method which recursively (w.r.t. class structure defined by attrs.ib)
     processes all attributes.

    `.process` checks attributes values and if `._should_process` evaluates true for attr meta
     and if `._process_single_object` returns a new value
     and evolves instance with replaced attr value

    Parametrized by:
        _PROCESSING_OBJECT_TV: type of the attribute value which should be processed
    """

    @abc.abstractmethod
    def _should_process(self, meta: FieldMeta) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def _process_single_object(self, obj: _PROCESSING_OBJECT_TV, meta: FieldMeta) -> Optional[_PROCESSING_OBJECT_TV]:
        raise NotImplementedError()

    @classmethod
    def _create_field_meta(cls, attr_ib: attr.Attribute) -> FieldMeta:
        container_stack, effective_type = unwrap_container_stack_with_single_type(attr_ib.type)
        return FieldMeta(
            clz=effective_type,
            attrib_name=attr_ib.name,
            container_stack=container_stack,
            attrib_descriptor=AttribDescriptor.from_attrib(attr_ib),
        )

    @classmethod
    def _get_changes_key(cls, attr_ib: attr.Attribute) -> str:
        return attr_ib.name.removeprefix("_")

    def _process_attr_ib_value(
        self,
        value: Any,
        meta: FieldMeta,
        do_processing: bool,
    ) -> Any:
        container_type, target_meta = meta.pop_container()

        if container_type is None:
            if do_processing:
                return self._process_single_object(value, target_meta)
            else:
                effective_nested_type = target_meta.clz
                if isinstance(effective_nested_type, type) and attr.has(effective_nested_type):
                    if value is not None:
                        # Recursively call entry point for nested object
                        return self.process(value)

                return value

        elif container_type is Optional:
            return self._process_attr_ib_value(value, target_meta, do_processing)

        elif is_sequence(container_type):
            if value is None:
                return None

            values_sequence = cast(Sequence, value)

            changed = False
            processed_values_sequence = []

            for single_value in values_sequence:
                processed_single_value = self._process_attr_ib_value(single_value, target_meta, do_processing)
                processed_values_sequence.append(processed_single_value)
                if processed_single_value is not single_value:
                    changed = True

            if changed:
                return tuple(processed_values_sequence)
            return value

        elif is_str_mapping(container_type):
            if do_processing:
                raise NotImplementedError("Processing of string mappings is not yet supported")
            return value

        raise AssertionError(f"Can not process container type {container_type}")

    def process(self, target: _TARGET_TV) -> _TARGET_TV:
        assert attr.has(type(target))
        changes = {}

        for attr_ib in attr.fields(type(target)):
            current_value = getattr(target, attr_ib.name)

            meta = self._create_field_meta(attr_ib)

            should_process = self._should_process(meta)
            processed_value = self._process_attr_ib_value(current_value, meta, should_process)

            if processed_value is not current_value:
                changes[self._get_changes_key(attr_ib)] = processed_value

        if changes:
            return attr.evolve(target, **changes)
        else:
            return target
