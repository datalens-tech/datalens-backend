from __future__ import annotations

# pubsub channel message prefixes
# for parsing simplicity, padding them to 10 bytes.
ALIVE_PREFIX = b"__alive___"
assert len(ALIVE_PREFIX) == 10
DATA_PREFIX = b"__data____"
assert len(DATA_PREFIX) == 10
FAIL_PREFIX = b"__fail____"
assert len(FAIL_PREFIX) == 10


# param: keys[1] - lock key
# param: keys[2] - cache data key
# params argv[1] - lock token (unique). Also used as debug identifier (add hostname+pid to it).
# params argv[2] - expiration in msec.
# Returns either of:
#  * `(ReqScriptResult.cache_hit.value, cache_data)`
#  * `(ReqScriptResult.successfully_locked.value, b'')`
#    * Which means the caller should go generate the data and then call CL_SAVE_SCRIPT.
#  * `(ReqScriptResult.lock_wait.value, b'')`, which means 'wait on the signal'
CL_REQ_SCRIPT = """ -- CL_REQ_SCRIPT
local cache_data = redis.call('get', KEYS[2])
if cache_data then
    return {130, cache_data}
end

local current_token = redis.call('get', KEYS[1])
if not current_token then
    redis.call('set', KEYS[1], ARGV[1])
    redis.call('pexpire', KEYS[1], ARGV[2])
    return {131, ''}
end

return {132, ''}
"""

# Renew the lock setting a new expiration time.
# param: keys[1] - lock key
# param: keys[2] - signal key (can be set to `''` to disable notify)
# param: argv[1] - lock token (unique)
# param: argv[2] - expiration in msec
# Returns either of:
#  * `(RenewScriptResult.expired.value, -1)` - no lock key
#    * Likely a reason to abort the locked process with an error. Or re-lock.
#  * `(RenewScriptResult.expired.value, -2)` - no TTL on lock (should never happen)
#  * `(RenewScriptResult.locked_by_another.value, another_process_token)` -
#    another process stole the lock.
#    * Definitely a reason to abort the locked process.
#  * `(RenewScriptResult.extended.value, previous_ttl)` - the normal case.
#    * Also publishes '__alive___' to the signal key if it is specified.
CL_RENEW_SCRIPT = """ -- CL_RENEW_SCRIPT
local current_ttl = redis.call('pttl', KEYS[1])
if current_ttl <= 0 then
    return {141, current_ttl}
end

local current_token = redis.call('get', KEYS[1])
if current_token ~= ARGV[1] then
    return {142, current_token}
end

redis.call('pexpire', KEYS[1], ARGV[2])
if KEYS[2] ~= '' then
    -- notify the subscribers that the generating worker is still alive
    redis.call('publish', KEYS[2], '__alive___' .. ARGV[1])
end

return {140, current_ttl}
"""

# Script to set the result and release the lock.
# param: keys[1] - lock key
# param: keys[2] - signal key
# param: keys[3] - cache data key
# param: argv[1] - lock token (unique)
# param: argv[2] - cache data
#   NOTE: it is recommended to cache the error-results too, with small TTL.
# param: argv[3] - cache data ttl
# Returns either of:
#  * `(SaveScriptResult.token_mismatch.value, another_process_token)` - another
#    process stole the lock, not saving.
#  * `(SaveScriptResult.success.value, watchers_count)` - succesfully saved and unlocked.
#  * `(SaveScriptResult.not_locked.value, watchers_count)` - the lock expired, saved anyway.

CL_SAVE_SCRIPT = """ -- CL_SAVE_SCRIPT
local current_token = redis.call('get', KEYS[1])

if current_token then
    if current_token ~= ARGV[1] then
        return {151, current_token}
    end
    redis.call('del', KEYS[1])
end

redis.call('set', KEYS[3], ARGV[2])
redis.call('pexpire', KEYS[3], ARGV[3])
local result = redis.call('publish', KEYS[2], '__data____' .. ARGV[2])

if not current_token then
    return {152, result}
end

return {150, result}
"""


# Script to release the lock without saving a result (indicating failure).
# param: keys[1] - lock key
# param: keys[2] - signal key
# param: argv[1] - lock token (unique)
# The result enum is shared with the `CL_SAVE_SCRIPT`
# Returns either of:
#  * `(SaveScriptResult.token_mismatch.value, '')` - the lock expired.
#  * `(SaveScriptResult.token_mismatch.value, another_process_token)` - another
#    process stole the lock.
#  * `(SaveScriptResult.success.value, watchers_count)` - succesfully unlocked and notified.
CL_FAIL_SCRIPT = """ -- CL_FAIL_SCRIPT
local current_token = redis.call('get', KEYS[1])
if not current_token then
    return {151, ''}
end
if current_token ~= ARGV[1] then
    return {151, current_token}
end

redis.call('del', KEYS[1])
local result = redis.call('publish', KEYS[2], '__fail____' .. ARGV[1])

return {150, result}
"""


# Script to set the result while ignoring the lock.
# param: keys[1] - signal key
# param: keys[2] - cache data key
# param: argv[1] - cache data
# param: argv[2] - cache data ttl
# Returns `watchers_count`
CL_FORCE_SAVE_SCRIPT = """ -- CL_FORCE_SAVE_SCRIPT
redis.call('set', KEYS[2], ARGV[1])
redis.call('pexpire', KEYS[2], ARGV[2])
return redis.call('publish', KEYS[1], '__data____' .. ARGV[1])
"""
