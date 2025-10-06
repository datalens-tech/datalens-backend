import dl_pydantic


def test_model_diff() -> None:
    class Model(dl_pydantic.BaseModel):
        a: int
        b: int

    model_a = Model(a=1, b=2)
    model_b = Model(a=1, b=3)

    diff = model_a.model_deepdiff(model_b)
    assert diff == {"values_changed": {"root.b": {"new_value": 3, "old_value": 2}}}

    diff = model_a.model_deepdiff(model_a, exclude_paths=["b"])
    assert diff == {}


def test_model_diff_with_nested_model() -> None:
    class Model(dl_pydantic.BaseModel):
        class Nested(dl_pydantic.BaseModel):
            c: int
            d: int

        a: int
        b: int
        nested: Nested

    model_a = Model(a=1, b=2, nested=Model.Nested(c=3, d=4))
    model_b = Model(a=1, b=2, nested=Model.Nested(c=3, d=5))

    diff = model_a.model_deepdiff(model_b)
    assert diff == {"values_changed": {"root.nested.d": {"new_value": 5, "old_value": 4}}}
