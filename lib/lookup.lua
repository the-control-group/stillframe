local key        = KEYS[1];
local timestamp  = tonumber(ARGV[1]);
local min_c      = tonumber(ARGV[2]);
local min_p      = tonumber(ARGV[3]);
local expiration = tonumber(ARGV[4]);

-- try the latest complete entry
local complete = tonumber(redis.call('HGET', key, 'complete'));
if(complete and complete > min_c) then return 'complete:' .. complete; end

-- try the latest pending entry
local pending = tonumber(redis.call('HGET', key, 'pending'));
if(pending and pending > min_p) then return 'pending:' .. pending; end

-- acquire a lock
redis.call('HSET', key, 'pending', timestamp);

-- set the expiration of the entire key
redis.call('PEXPIREAT', key, expiration);
return nil;