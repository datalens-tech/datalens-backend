from typing import Optional


def make_data_source_id_for_connection(connection_id: Optional[str]) -> str:
    # FIXME: this is temporary
    return 'conn{}'.format(connection_id)
