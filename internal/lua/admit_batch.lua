-- KEYS[1] = room:<event>:queue (zset)
-- ARGV[1] = "room:<event>:hb:"  heartbeat key prefix
-- ARGV[2] = "room:<event>:admitted:" admission key prefix
-- ARGV[3] = max admissions, ARGV[4] = admission TTL seconds
local admitted, out = 0, {}
while admitted < tonumber(ARGV[3]) do
    local top = redis.call('ZPOPMIN', KEYS[1], 1)
    if not top[1] then break end              -- queue drained
    local sid = top[1]
    if redis.call('EXISTS', ARGV[1] .. sid) == 1 then
        redis.call('SET', ARGV[2] .. sid, '1', 'EX', tonumber(ARGV[4]))
        admitted = admitted + 1
        out[#out + 1] = sid
    end
    -- zombies: ZPOPMIN already removed them; slot reused in this batch
end
return out