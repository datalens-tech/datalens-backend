from __future__ import annotations

from enum import IntEnum, unique


@unique
class ReqScriptResultBase(IntEnum):
    pass


@unique
class ReqScriptResult(ReqScriptResultBase):
    cache_hit = 130
    successfully_locked = 131
    lock_wait = 132


@unique
class ReqResultInternal(ReqScriptResultBase):
    # Special values for internal processing:
    starting = 101  # A lock request might be in flight.
    requesting = 102  # A lock request might be in flight.
    awaiting = 103  # A pubsub subscription is being awaited.

    # *MUST* have all values of the `ReqScriptResult`
    cache_hit = 130
    successfully_locked = 131
    lock_wait = 132

    # Special situations:
    cache_hit_slave = 333
    cache_hit_after_wait = 334
    network_call_timeout = 335
    lock_wait_timeout = 336
    lock_wait_unexpected_message = 337
    failure_signal = 338

    # Final situations, primarily for logging. Note that under
    # `enable_background_tasks`, these will become available during the
    # finalization task.
    generated_saved = 439
    generated_force_saved = 440
    failure_to_initialize = 441
    failure_marker_sent = 442


@unique
class RenewScriptResult(IntEnum):
    extended = 140
    expired = 141
    locked_by_another = 142


@unique
class SaveScriptResult(IntEnum):
    success = 150
    token_mismatch = 151
    not_locked = 152
