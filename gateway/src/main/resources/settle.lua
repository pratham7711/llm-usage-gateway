-- Settles one request: its reservation is released and its real usage counted, atomically, so the
-- quota never sees the request twice or not at all. The lease stays, now holding the usage event,
-- until Kafka acknowledges that event; if the gateway dies first, the reaper publishes it.
--
-- KEYS[1] lease records   KEYS[2] tokens held in the month the lease was taken
-- KEYS[3] tokens used in the month the request is billed to
-- ARGV[1] lease id   ARGV[2] billed tokens   ARGV[3] usage event (JSON)   ARGV[4] ttl seconds
-- Returns 1 if settled, 0 if the lease was gone (expired and reaped).
local v = redis.call('HGET', KEYS[1], ARGV[1])
if not v or string.sub(v, 1, 2) ~= 'H|' then
  return 0
end
local hold = tonumber(string.match(v, '^H|(%d+)|'))
redis.call('DECRBY', KEYS[2], hold)
redis.call('INCRBY', KEYS[3], ARGV[2])
redis.call('EXPIRE', KEYS[3], ARGV[4])
redis.call('HSET', KEYS[1], ARGV[1], 'S|' .. ARGV[3])
return 1
