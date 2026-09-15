-- KEYS[1] = bucket key, e.g. "{event:1001}:bucket"
-- ARGV[1] = capacity, ARGV[2] = refill_rate (tokens/sec)
-- ARGV[3] = now (float unix sec), ARGV[4] = requested tokens (usually 1)

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last   = tonumber(data[2])
local capacity   = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now        = tonumber(ARGV[3])
local requested   = tonumber(ARGV[4])

if tokens == nil or last == nil then
    tokens = capacity
    last   = now
else
    tokens = math.min(capacity, tokens + math.max(0, now - last) * refill_rate)
end

local ttl = math.ceil(capacity / refill_rate) * 2   -- auto-evict idle buckets

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_updated', now)
    redis.call('EXPIRE', KEYS[1], ttl)
    return {1, math.floor(tokens), 0}               -- allowed, remaining, retry_after
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_updated', now)
redis.call('EXPIRE', KEYS[1], ttl)
local retry_after = math.ceil((requested - tokens) / refill_rate)
return {0, math.floor(tokens), retry_after}         -- denied, remaining, retry_after