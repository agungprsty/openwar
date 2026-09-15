-- KEYS[1] = inventory:<sku>:shard:<i>
-- Returns {1, remaining} on success, {0, -1} when this shard is empty.
local remaining = redis.call('DECR', KEYS[1])
if remaining < 0 then
    redis.call('INCR', KEYS[1]) -- refund over-decrement; shard exhausted
    return {0, -1}
end
return {1, remaining}