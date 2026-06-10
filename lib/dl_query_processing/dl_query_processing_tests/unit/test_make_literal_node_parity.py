import datetime

import pytest

from dl_formula.core.datatype import DataType
import dl_formula.core.nodes as formula_nodes
from dl_query_processing.compilation.helpers import make_literal_node
import dl_query_processing.exc


@pytest.mark.parametrize(
    "raw, data_type, expected_value",
    [
        ("2", DataType.INTEGER, 2),
        ("-1", DataType.CONST_INTEGER, -1),
        ("3.14", DataType.FLOAT, 3.14),
        ("true", DataType.BOOLEAN, True),
        ("false", DataType.BOOLEAN, False),
        ("hello", DataType.STRING, "hello"),
        ("2022-04-25", DataType.DATE, datetime.date(2022, 4, 25)),
        ("2022-04-25T10:11:12", DataType.DATETIME, datetime.datetime(2022, 4, 25, 10, 11, 12)),
        ("2022-04-25 10:11:12", DataType.GENERICDATETIME, datetime.datetime(2022, 4, 25, 10, 11, 12)),
    ],
)
def test_make_literal_node_scalar_values(raw, data_type, expected_value):
    node = make_literal_node(val=raw, data_type=data_type)
    assert isinstance(node, formula_nodes.BaseLiteral)
    assert node.value == expected_value


@pytest.mark.parametrize(
    "raw, data_type",
    [
        ("0 OR 1=1 --", DataType.INTEGER),
        ("-1 UNION SELECT 999, version() --", DataType.INTEGER),
        ("not-a-bool", DataType.BOOLEAN),
        ("not-a-date", DataType.DATE),
    ],
)
def test_make_literal_node_rejects_invalid(raw, data_type):
    with pytest.raises(dl_query_processing.exc.InvalidLiteralError):
        make_literal_node(val=raw, data_type=data_type)
