import os


# FIXME: remove
def update_global_tvm_info(tvm_info: tuple[str, str, str]):  # type: ignore  # TODO: fix
    """
    Unfortunately, passing the value all the way to CHYDB adapter is too problematic,
    so set it globally in the tests (and leave it there).

    Recommended usage:

        @pytest.fixture(scope='session', autouse=True)
        def updated_global_tvm_info(tvm_secret_reader):
            return update_global_tvm_info(tvm_secret_reader.get_tvm_info())

        @pytest.fixture(scope='session')
        def tvm_info(updated_global_tvm_info, tvm_secret_reader):
            return updated_global_tvm_info or tvm_secret_reader.get_tvm_info()
    """
    import bi_blackbox_client.tvm_client as mod
    mod.TVM_INFO = tvm_info
    # For e.g. uwsgi subprocesses.
    os.environ['TVM_INFO'] = ' '.join(tvm_info)
    return tvm_info
