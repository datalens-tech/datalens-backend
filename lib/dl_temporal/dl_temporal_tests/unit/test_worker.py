import builtins
import collections.abc
from collections.abc import Iterator

import pytest
import temporalio.worker.workflow_sandbox
import temporalio.worker.workflow_sandbox._importer as sandbox_importer
import temporalio.worker.workflow_sandbox._restrictions as sandbox_restrictions

import dl_temporal.worker as worker


def _frozendict_hook_layers() -> int:
    # frozendict's pure-Python monkeypatch wraps MutableMapping.__subclasshook__ in a
    # `frozendictMutableMappingSubclasshook` closure over the previous hook on every run, so the
    # depth of the closure chain is how many times the patch ran.
    layers = 0
    current: object = collections.abc.MutableMapping.__subclasshook__
    while True:
        func = getattr(current, "__func__", current)
        if getattr(func, "__name__", None) != "frozendictMutableMappingSubclasshook":
            break
        layers += 1
        previous_hook = next(
            (
                cell.cell_contents
                for cell in getattr(func, "__closure__", None) or ()
                if not isinstance(cell.cell_contents, type) and callable(cell.cell_contents)
            ),
            None,
        )
        if previous_hook is None:
            break
        current = previous_hook
    return layers


def _import_frozendict_under(restrictions: sandbox_restrictions.SandboxRestrictions) -> None:
    importer = sandbox_importer.Importer(restrictions, sandbox_restrictions.RestrictionContext())
    with importer.applied():
        builtins.__import__("frozendict")


@pytest.fixture(name="restore_mutable_mapping_hook")
def fixture_restore_mutable_mapping_hook() -> Iterator[None]:
    saved_hook = collections.abc.MutableMapping.__dict__["__subclasshook__"]
    try:
        yield
    finally:
        # restoring the saved descriptor, not defining a method
        collections.abc.MutableMapping.__subclasshook__ = saved_hook  # type: ignore[method-assign]
        # _abc_caches_clear is a real CPython ABCMeta method, absent from typeshed stubs
        collections.abc.MutableMapping._abc_caches_clear()  # type: ignore[attr-defined]


def test_create_worker_uses_frozendict_passthrough_restrictions(
    restore_mutable_mapping_hook: None,
) -> None:
    baseline_layers = _frozendict_hook_layers()

    for _ in range(3):
        _import_frozendict_under(worker._SANDBOX_RESTRICTIONS)

    assert _frozendict_hook_layers() == baseline_layers


def test_default_restrictions_stack_the_hook(
    restore_mutable_mapping_hook: None,
) -> None:
    baseline_layers = _frozendict_hook_layers()

    _import_frozendict_under(temporalio.worker.workflow_sandbox.SandboxRestrictions.default)

    assert _frozendict_hook_layers() > baseline_layers
