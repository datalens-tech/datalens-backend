from bi_core.components.ids import ID_VALID_SYMBOLS, ID_LENGTH, make_readable_field_id


def test_make_readable_field_id():
    assert make_readable_field_id(title="hello!:*",
                                  valid_symbols=ID_VALID_SYMBOLS,
                                  max_length=ID_LENGTH) == "hello"
    assert make_readable_field_id(title=" René François Lacôte   123",
                                  valid_symbols=ID_VALID_SYMBOLS,
                                  max_length=ID_LENGTH) == "rene_francois_lacote_123"
    assert make_readable_field_id(title="Проверка",
                                  valid_symbols=ID_VALID_SYMBOLS,
                                  max_length=ID_LENGTH) == "proverka"

