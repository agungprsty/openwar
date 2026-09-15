-- KEYS[1] = reservation:<orderID>
-- ARGV[1] = inventory shard key, ARGV[2] = qty, ARGV[3] = reason
-- Returns {1, remaining} when restored, {0, state} when already handled.
local st = redis.call('GET', KEYS[1])
if not st then return {0, 'NO_RESERVATION'} end
if st ~= 'RESERVED' then return {0, st} end   -- already PUBLISHED/COMPENSATED
redis.call('SET', KEYS[1], 'COMPENSATING')     -- atomic claim first
local remaining = redis.call('INCR', ARGV[1])  -- refund (node-local; see caveat)
redis.call('SET', KEYS[1], 'COMPENSATED')
return {1, remaining}