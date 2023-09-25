from __future__ import annotations

from sqlalchemy import exc as sa_exc

from dl_core import exc
import dl_core.connectors.base.error_transformer as error_transformer
from dl_core.connectors.base.error_transformer import wrapper_exc_is_and_matches_re


def test_transform_with_custom_rules():
    transformer = error_transformer.make_default_transformer_with_custom_rules(
        error_transformer.ErrorTransformerRule(
            when=wrapper_exc_is_and_matches_re(Exception, "Hello"), then_raise=exc.InvalidQuery
        )
    )

    transformed_invalid_query = transformer.make_bi_error(
        sa_exc.OperationalError(orig=Exception("Well, Hello"), params={}, statement="")
    )
    assert isinstance(transformed_invalid_query, exc.InvalidQuery)

    transformed_operational = transformer.make_bi_error(
        sa_exc.OperationalError(orig=Exception(), params={}, statement="")
    )
    assert isinstance(transformed_operational, exc.DatabaseOperationalError)

    transformed_no_such_table = transformer.make_bi_error(
        sa_exc.OperationalError(orig=sa_exc.NoSuchTableError(), params={}, statement="")
    )
    assert isinstance(transformed_no_such_table, exc.SourceDoesNotExist)

    transformed_default = transformer.make_bi_error(Exception())
    assert isinstance(transformed_default, exc.DatabaseQueryError)
