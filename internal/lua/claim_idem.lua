-- KEYS[1] = idem:<userID>:<eventID>:<idemKey>
-- ARGV[1] = TTL s, ARGV[2] = request fingerprint, ARGV[3] = unix ms
local data = redis.call('HMGET', KEYS[1], 'status', 'fingerprint')
if data[1] ~= false and data[1] ~= 'FAILED' then
    if data[2] ~= ARGV[2] then return {0, 'MISMATCH'} end -- same key, diff body
    return {0, 'DUPLICATE'}                                -- replay → never re-run
end
redis.call('HSET', KEYS[1], 'status', 'PENDING', 'fingerprint', ARGV[2], 'createdAt', ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
return {1, 'ACCEPTED'}