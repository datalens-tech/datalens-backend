from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Tuple, Type, Union

import attr

from .enums import RenewScriptResult, ReqScriptResult, SaveScriptResult
from .exc import CacheLockLost, CacheRedisError
from .redis_utils import eval_script
from .scripts import (
    CL_FAIL_SCRIPT,
    CL_FORCE_SAVE_SCRIPT,
    CL_RENEW_SCRIPT,
    CL_REQ_SCRIPT,
    CL_SAVE_SCRIPT,
)

if TYPE_CHECKING:
    from enum import IntEnum

    from redis.asyncio import Redis


@attr.s(auto_attribs=True, frozen=True)
class ScriptsSupport:
    encoding: str = "utf-8"
    result_error_exc_cls: Type[Exception] = CacheRedisError

    def to_bytes(self, value: Union[bytes, str]) -> bytes:
        if isinstance(value, bytes):
            return value  # rare
        return value.encode(self.encoding)

    def to_text(self, value: Union[bytes, str], **kwargs: Any) -> str:
        if isinstance(value, str):
            return value
        return value.decode(self.encoding, **kwargs)

    def parse_script_result(self, res: Any, expected_length: int) -> Tuple[Any, ...]:
        if not isinstance(res, (tuple, list)):
            raise self.result_error_exc_cls(  # rare
                f"Unexpected script result type: {type(res)}, {res!r}"
            )
        if len(res) != expected_length:
            raise self.result_error_exc_cls(  # rare
                f"Unexpected script result length: {len(res)}, {res!r}"
            )
        return tuple(res)


@attr.s
class ScriptBase:
    script: ClassVar[str]
    situation_enum: ClassVar[Optional[Type[IntEnum]]]
    expected_result_length: ClassVar[int] = 2

    cli: Redis = attr.ib()

    support: ScriptsSupport = attr.ib(factory=ScriptsSupport)

    def _to_bytes(self, value: Union[bytes, str]) -> bytes:
        return self.support.to_bytes(value)

    def _to_text(self, value: Union[bytes, str], **kwargs: Any) -> str:
        return self.support.to_text(value, **kwargs)

    def _to_msec(self, value_sec: float) -> int:
        return int(value_sec * 1000)

    def _parse_script_result(self, res: Any) -> Tuple[Any, ...]:
        assert self.expected_result_length >= 1
        prepared_res = self.support.parse_script_result(
            res=res,
            expected_length=self.expected_result_length,
        )
        situation_value = prepared_res[0]
        params = prepared_res[1:]
        if self.situation_enum is None:
            situation = situation_value  # rare
        else:
            try:
                situation = self.situation_enum(situation_value)
            except ValueError as err:
                raise self.support.result_error_exc_cls(  # rare
                    f"Unexpected script result situation: {situation_value}, {res!r}"
                ) from err
        return (situation,) + params

    async def _eval_script_base(self, keys: List[Any], args: List[Any]) -> Any:
        return await eval_script(self.cli, self.script, keys, args)

    async def _eval_script(self, keys: List[Any], args: List[Any]) -> Tuple[Any, ...]:
        res = await self._eval_script_base(keys, args)
        return res, self._parse_script_result(res)


class ReqScript(ScriptBase):
    script: ClassVar[str] = CL_REQ_SCRIPT
    situation_enum: ClassVar[Type[ReqScriptResult]] = ReqScriptResult

    async def __call__(
        self,
        lock_key: str,
        data_key: str,
        self_id: str,
        lock_ttl_sec: float,
    ) -> Tuple[ReqScriptResult, Optional[bytes]]:
        _, (situation, param) = await self._eval_script(
            keys=[self._to_bytes(lock_key), self._to_bytes(data_key)],
            args=[self._to_bytes(self_id), self._to_msec(lock_ttl_sec)],
        )
        if situation == self.situation_enum.cache_hit:
            assert param is not None
        if situation == self.situation_enum.successfully_locked:
            assert param is None or param == b""
            param = None
        return situation, param


class RenewScript(ScriptBase):
    script: ClassVar[str] = CL_RENEW_SCRIPT
    situation_enum: ClassVar[Type[RenewScriptResult]] = RenewScriptResult
    lock_lost_exc_cls: Type[Exception] = CacheLockLost

    async def __call__(
        self,
        lock_key: str,
        signal_key: Optional[str],
        self_id: str,
        lock_ttl_sec: float,
    ) -> int:
        res, (situation, param) = await self._eval_script(
            keys=[self._to_bytes(lock_key), self._to_bytes(signal_key or "")],
            args=[self._to_bytes(self_id), self._to_msec(lock_ttl_sec)],
        )
        if situation == self.situation_enum.expired and param == -1:
            raise self.lock_lost_exc_cls("Lock found to be expired on renew")
        if situation == self.situation_enum.expired and param == -2:
            raise self.lock_lost_exc_cls("Lock found to have no TTL")
        if situation == self.situation_enum.locked_by_another:
            raise self.lock_lost_exc_cls(
                f"Lock found to be owned by another: {param!r}"
            )
        if situation == self.situation_enum.extended:
            return param  # old ttl
        raise self.support.result_error_exc_cls(
            f"Unexpected renew script result: {res!r}"
        )


class SaveScript(ScriptBase):
    script: ClassVar[str] = CL_SAVE_SCRIPT
    situation_enum: ClassVar[Type[SaveScriptResult]] = SaveScriptResult
    lock_lost_exc_cls: Type[Exception] = CacheLockLost

    async def __call__(
        self,
        lock_key: str,
        signal_key: str,
        data_key: str,
        self_id: str,
        data: bytes,
        data_ttl_sec: float,
    ) -> int:
        res, (situation, param) = await self._eval_script(
            keys=[
                self._to_bytes(lock_key),
                self._to_bytes(signal_key),
                self._to_bytes(data_key),
            ],
            args=[self._to_bytes(self_id), data, self._to_msec(data_ttl_sec)],
        )
        if isinstance(param, bytes):
            param = self._to_text(param, errors="replace")  # expecting a debug value
        if situation == self.situation_enum.success:
            return param  # watchers count
        if situation == self.situation_enum.token_mismatch:
            raise self.lock_lost_exc_cls(
                f"Lock found to be owned by another: {param!r}"
            )
        if situation == self.situation_enum.not_locked:
            raise self.lock_lost_exc_cls(
                (
                    f"Lock found to be expired (but saving the data anyway);"
                    f"watchers: {param!r}"
                )
            )
        raise self.support.result_error_exc_cls(
            f"Unexpected save script result: {res!r}"
        )


class FailScript(ScriptBase):
    script: ClassVar[str] = CL_FAIL_SCRIPT
    situation_enum: ClassVar[Type[SaveScriptResult]] = SaveScriptResult
    lock_lost_exc_cls: Type[Exception] = CacheLockLost

    async def __call__(
        self,
        lock_key: str,
        signal_key: str,
        self_id: str,
        ignore_errors: bool = False,
    ) -> Optional[int]:
        res, (situation, param) = await self._eval_script(
            keys=[self._to_bytes(lock_key), self._to_bytes(signal_key)],
            args=[self._to_bytes(self_id)],
        )
        if isinstance(param, bytes):
            param = self._to_text(param, errors="replace")  # expecting a debug value
        if situation == self.situation_enum.token_mismatch and param == "":
            if ignore_errors:
                return None
            raise self.lock_lost_exc_cls("Lock found to be expired")
        if situation == self.situation_enum.token_mismatch:
            if ignore_errors:
                return None
            raise self.lock_lost_exc_cls(
                f"Lock found to be owned by another: {param!r}"
            )
        if situation == self.situation_enum.success:
            return param  # watchers count
        raise self.support.result_error_exc_cls(
            f"Unexpected fail script result: {res!r}"
        )


class ForceSaveScript(ScriptBase):
    script: ClassVar[str] = CL_FORCE_SAVE_SCRIPT
    situation_enum: ClassVar[Optional[Type[IntEnum]]] = None

    async def __call__(
        self,
        signal_key: str,
        data_key: str,
        data: bytes,
        data_ttl_sec: float,
    ) -> int:
        res = await self._eval_script_base(
            keys=[self._to_bytes(signal_key), self._to_bytes(data_key)],
            args=[data, self._to_msec(data_ttl_sec)],
        )
        if not isinstance(res, int):
            raise self.support.result_error_exc_cls(
                f"Unexpected force-save script result: {res!r}"
            )
        return res
