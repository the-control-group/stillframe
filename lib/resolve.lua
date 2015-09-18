local key        = KEYS[1];
local timestamp  = tonumber(ARGV[1]);
local expiration = tonumber(ARGV[2]);

-- mark the entry as complete
redis.call('HSET', key, 'complete', timestamp);

-- set the expiration of the entire key
redis.call('PEXPIREAT', key, expiration);

return nil;