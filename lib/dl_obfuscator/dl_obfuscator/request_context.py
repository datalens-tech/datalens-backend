from contextvars import ContextVar

from dl_obfuscator.engine import ObfuscationEngine


_obfuscation_engine: ContextVar[ObfuscationEngine | None] = ContextVar("obfuscation_engine", default=None)


def set_request_obfuscation_engine(engine: ObfuscationEngine | None) -> None:
    _obfuscation_engine.set(engine)


def get_request_obfuscation_engine() -> ObfuscationEngine | None:
    return _obfuscation_engine.get()


def clear_request_obfuscation_engine() -> None:
    _obfuscation_engine.set(None)
