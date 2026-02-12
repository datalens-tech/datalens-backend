from contextvars import ContextVar

from dl_obfuscator.engine import ObfuscationEngine
from dl_obfuscator.obfuscators.secret import SecretObfuscator
from dl_obfuscator.secret_keeper import SecretKeeper


_obfuscation_engine: ContextVar[ObfuscationEngine | None] = ContextVar("obfuscation_engine", default=None)


def set_request_obfuscation_engine(engine: ObfuscationEngine | None) -> None:
    _obfuscation_engine.set(engine)


def get_request_obfuscation_engine() -> ObfuscationEngine | None:
    return _obfuscation_engine.get()


def clear_request_obfuscation_engine() -> None:
    _obfuscation_engine.set(None)


def setup_request_obfuscation(
    engine: ObfuscationEngine | None,
    secret_keeper: SecretKeeper | None,
) -> None:
    """Call at request start. Sets the engine and adds request obfuscator."""
    set_request_obfuscation_engine(engine)
    if engine is not None and secret_keeper is not None:
        engine.add_request_obfuscator(SecretObfuscator(keeper=secret_keeper))


def teardown_request_obfuscation(engine: ObfuscationEngine | None) -> None:
    """Call at request end. Clears request obfuscators and engine reference."""
    if engine is not None:
        engine.clear_request_obfuscators()
    clear_request_obfuscation_engine()
