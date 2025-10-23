import typing


JsonSerializablePrimitive = str | int | float | bool | None
JsonSerializableMapping = typing.Mapping[str, "JsonSerializable"]
JsonSerializableSequence = typing.Sequence["JsonSerializable"]
JsonSerializable = JsonSerializablePrimitive | JsonSerializableMapping | JsonSerializableSequence
