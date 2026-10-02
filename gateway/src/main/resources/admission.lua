-- Admission for one request, atomic in Redis: quota, rate limit, then a lease that reserves the
-- request's worst-case cost until it settles.
--
-- KEYS[1] token bucket (hash: tokens, ts)     KEYS[2] tokens used this month
-- KEYS[3] tokens held this month              KEYS[4] lease records (hash)
-- KEYS[5] lease expiries (sorted set, score = epoch ms)
-- ARGV[1] refill rate per second   ARGV[2] burst capacity   ARGV[3] monthly token quota
-- ARGV[4] lease id                 ARGV[5] prompt tokens to hold
-- ARGV[6] most output tokens the request may use
-- ARGV[7] lease ttl ms             ARGV[8] month   ARGV[9] lease event template (JSON)
-- ARGV[10] most open leases per tenant           ARGV[11] ttl in seconds for the month keys
-- Returns {code, retry_after_ms, granted_output_tokens}:
--   1 admitted, 0 rate limited, -1 quota exhausted, -2 too many open leases,
--   -3 the rest of the quota is reserved by requests in flight (retry once they settle).
local rate       = tonumber(ARGV[1])
local burst      = tonumber(ARGV[2])
local quota      = tonumber(ARGV[3])
local promptHold = tonumber(ARGV[5])
local maxOut     = tonumber(ARGV[6])

-- Quota counts what is already used plus what admitted requests might still use, so concurrent
-- requests can never jointly spend past it.
local used = tonumber(redis.call('GET', KEYS[2]) or '0')
local held = tonumber(redis.call('GET', KEYS[3]) or '0')
local remaining = quota - used - held
if remaining < promptHold + 1 then
  if quota - used >= promptHold + 1 then
    return {-3, 0, 0}
  end
  return {-1, 0, 0}
end

-- Leases pile up only when settled usage cannot reach Kafka. Past a bound, stop serving rather
-- than serve what cannot be billed.
if redis.call('ZCARD', KEYS[5]) >= tonumber(ARGV[10]) then
  return {-2, 0, 0}
end

-- Redis's own clock, so every gateway replica refills the bucket identically.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)

local code = 1
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  code = 0
  retry = math.ceil((1 - tokens) * 1000 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 1000 / rate) + 1000)
if code == 0 then
  return {0, retry, 0}
end

-- Grant only the output the tenant can still pay for; the gateway sends it upstream as max_tokens,
-- so the provider itself cannot generate past the quota.
local grant = math.min(maxOut, remaining - promptHold)
local hold = promptHold + grant
redis.call('INCRBY', KEYS[3], hold)
redis.call('EXPIRE', KEYS[3], ARGV[11])
redis.call('HSET', KEYS[4], ARGV[4], 'H|' .. hold .. '|' .. promptHold .. '|' .. ARGV[8] .. '|' .. ARGV[9])
redis.call('ZADD', KEYS[5], now + tonumber(ARGV[7]), ARGV[4])
return {1, 0, grant}
