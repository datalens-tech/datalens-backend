from __future__ import annotations

from typing import (
    AbstractSet,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from marshmallow.decorators import (
    POST_DUMP,
    POST_LOAD,
    PRE_DUMP,
    PRE_LOAD,
)
from marshmallow.error_store import ErrorStore
from marshmallow.exceptions import ValidationError
from marshmallow_oneofschema import OneOfSchema


class OneOfSchemaWithDumpLoadHooks(OneOfSchema):  # TODO: Move to bi_model_tools
    """
    OneOfSchema has disabled all pre/post_dump/load hooks by default so we add the following copy-paste from marshmallow
    to be able to use those hooks

    See https://github.com/marshmallow-code/marshmallow-oneofschema/issues/4
    """

    def dump(
        self, obj: Any, *, many: Optional[bool] = None, **kwargs: Any
    ) -> Union[Mapping[str, Any], Iterable[Mapping[str, Any]]]:
        many = self.many if many is None else bool(many)

        if self._has_processors(PRE_DUMP):
            processed_obj = self._invoke_dump_processors(PRE_DUMP, obj, many=many, original_data=obj)
        else:
            processed_obj = obj

        result = super().dump(processed_obj, many=many, **kwargs)

        if self._has_processors(POST_DUMP):
            result = self._invoke_dump_processors(POST_DUMP, result, many=many, original_data=obj)

        return result

    def load(
        self,
        data: Union[Mapping[str, Any], Iterable[Mapping[str, Any]]],
        *,
        many: Optional[bool] = None,
        partial: Optional[Union[bool, Sequence[str], AbstractSet[str]]] = None,
        unknown: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        error_store = ErrorStore()
        errors: Dict[str, List[str]] = {}
        many = self.many if many is None else bool(many)
        unknown = unknown or self.unknown
        if partial is None:
            partial = self.partial
        processed_data = data
        result = None
        # Run preprocessors
        if self._has_processors(PRE_LOAD):
            try:
                processed_data = self._invoke_load_processors(
                    PRE_LOAD, data, many=many, original_data=data, partial=partial
                )
            except ValidationError as err:
                errors = err.normalized_messages()
                result = None
        if not errors:
            # Deserialize data
            try:
                result = super().load(processed_data, many=many, partial=partial, unknown=unknown, **kwargs)
            except ValidationError as error:
                error_store.store_error(error.messages)
                result = error.valid_data
            errors = error_store.errors
            # Run post processors
            if not errors and self._has_processors(POST_LOAD):
                try:
                    result = self._invoke_load_processors(
                        POST_LOAD,
                        result,
                        many=many,
                        original_data=data,
                        partial=partial,
                    )
                except ValidationError as err:
                    errors = err.normalized_messages()
        if errors:
            exc = ValidationError(errors, data=data, valid_data=result)
            self.handle_error(exc, data, many=many, partial=partial)
            raise exc

        return result
