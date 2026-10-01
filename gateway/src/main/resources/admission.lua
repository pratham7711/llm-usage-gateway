-- Admission check for one request, atomic in Redis.
-- KEYS[1] token bucket (hash: tokens, ts_ms)   KEYS[2] billed tokens this month (string, written by metering)
-- ARGV[1] refill rate per second   ARGV[2] burst capacity   ARGV[3] monthly token quota
-- Returns {code, retry_after_ms}: 1 admitted, 0 rate limited, -1 quota exhausted.
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local quota = tonumber(ARGV[3])

local used = tonumber(redis.call('GET', KEYS[2]) or '0')
if used >= quota then
  return {-1, 0}
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
return {code, retry}
