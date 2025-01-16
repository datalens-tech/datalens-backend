from dl_core.fields import BIField
from dl_testing.utils import get_log_record


def test_rename_in_formula(caplog):
    map = {"apples": "oranges"}
    formula = "COUNTD([apples]) + COUNTD([bananas]) + 1"
    assert BIField.rename_in_formula(formula, map) == "COUNTD([oranges]) + COUNTD([bananas]) + 1"

    log_record = get_log_record(caplog, predicate=lambda r: r.message.startswith("Unknown field"), single=True)
    assert log_record.message == "Unknown field: bananas"
