from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq


def test_FrozenMappingStrToStrOrStrSeq():
    mapping_data = {
        "avro": ("lancaster", "manchester",),
        "boeing": "B-17 Flying Fortress",
        "tupolev": ("tu-2",),
    }

    container = FrozenMappingStrToStrOrStrSeq(mapping_data)

    # Keys
    assert container.keys() == mapping_data.keys()
    # Dict conversion
    assert dict(container) == mapping_data
    # Items
    assert container.items() == mapping_data.items()
    # []
    assert {k: container[k] for k in mapping_data.keys()} == mapping_data
    # Get
    assert {k: container.get(k) for k in mapping_data.keys()} == mapping_data

    # Len
    assert len(container) == len(mapping_data)

    # Equality
    assert FrozenMappingStrToStrOrStrSeq(mapping_data) == FrozenMappingStrToStrOrStrSeq(mapping_data)
    assert FrozenMappingStrToStrOrStrSeq(mapping_data) != FrozenMappingStrToStrOrStrSeq({"a": "1"})

    # Hash
    assert hash(FrozenMappingStrToStrOrStrSeq(mapping_data)) == hash(FrozenMappingStrToStrOrStrSeq(mapping_data))
    assert hash(FrozenMappingStrToStrOrStrSeq(mapping_data)) != hash(FrozenMappingStrToStrOrStrSeq({"a": "1"}))
