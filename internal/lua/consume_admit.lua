-- KEYS[1] = admission key
-- Atomically validate + consume a single-use admission token.
local exists = redis.call('GET', KEYS[1])
if not exists then return 0 end
redis.call('DEL', KEYS[1])
return 1