-- KEYS[1]: 限流 key
-- ARGV[1]: 当前时间戳（毫秒）
-- ARGV[2]: 窗口大小（毫秒）
-- ARGV[3]: 最大允许次数

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local maxCount = tonumber(ARGV[3])

local windowStart = now - window

-- 1. 移除时间窗口之前的过期记录
redis.call('ZREMRANGEBYSCORE', key, 0, windowStart)

-- 2. 统计当前窗口内的请求总数
local currentCount = redis.call('ZCARD', key)

if currentCount >= maxCount then
    return 0
end

-- 3. 生成唯一 member（时间戳 + 随机数防同毫秒碰撞）并写入 ZSet
local member = tostring(now) .. "-" .. tostring(math.random(100000, 999999))
redis.call('ZADD', key, now, member)

-- 4. 设置 key 过期时间（略大于窗口时间）
redis.call('PEXPIRE', key, window + 1000)

return 1