-- Claims one tenant's expired leases, atomically, into its reaped list, where they stay until
-- Kafka has acknowledged their usage events.
--
-- A settled lease ("S|event") already counted its usage when it settled: it is only moved. A held
-- lease ("H|hold|...") belongs to a request the gateway never settled, so its reservation becomes
-- usage: the request is billed at the most it could have cost.
--
-- Held tokens are per month, and one call can only touch the month keys it was given, so a held
-- lease from another month is left in place and its month is returned for a second call.
--
-- KEYS[1] lease records   KEYS[2] lease expiries   KEYS[3] reaped list
-- KEYS[4] tokens held in ARGV[1]   KEYS[5] tokens used in ARGV[1]
-- ARGV[1] month   ARGV[2] most leases to claim   ARGV[3] ttl in seconds for the month keys
-- Returns {claimed, held_reaped, stale, other months...}
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))

local claimed, heldReaped, stale = 0, 0, 0
local others, seen = {}, {}
for _, id in ipairs(ids) do
  local v = redis.call('HGET', KEYS[1], id)
  if not v then
    redis.call('ZREM', KEYS[2], id)
    stale = stale + 1
  elseif string.sub(v, 1, 2) == 'S|' then
    redis.call('RPUSH', KEYS[3], v)
    redis.call('HDEL', KEYS[1], id)
    redis.call('ZREM', KEYS[2], id)
    claimed = claimed + 1
  else
    local hold, month = string.match(v, '^H|(%d+)|%d+|([^|]+)|')
    if month == ARGV[1] then
      redis.call('DECRBY', KEYS[4], hold)
      redis.call('INCRBY', KEYS[5], hold)
      redis.call('EXPIRE', KEYS[5], ARGV[3])
      redis.call('RPUSH', KEYS[3], v)
      redis.call('HDEL', KEYS[1], id)
      redis.call('ZREM', KEYS[2], id)
      claimed = claimed + 1
      heldReaped = heldReaped + 1
    elseif not month then
      -- Unreadable: hand it to the reaper, which logs and drops it, rather than let it block the queue.
      redis.call('RPUSH', KEYS[3], v)
      redis.call('HDEL', KEYS[1], id)
      redis.call('ZREM', KEYS[2], id)
      claimed = claimed + 1
    elseif not seen[month] then
      seen[month] = true
      table.insert(others, month)
    end
  end
end

local out = {claimed, heldReaped, stale}
for _, m in ipairs(others) do table.insert(out, m) end
return out
