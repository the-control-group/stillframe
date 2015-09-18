'use strict';

var fs = require('fs');
var Redis = require('ioredis');
var stream = require('stream');
var hasher = require('object-hash');

var lookupScript = fs.readFileSync(__dirname + '/lookup.lua');
var resolveScript = fs.readFileSync(__dirname + '/resolve.lua');

function Stillframe(config, store, engines) {

	if(!this instanceof Stillframe)
		return new Stillframe(config);

	if(!store || !engines || !config)
		throw new Error('Missing required arguments.');

	this.config = config;

	// create the redis client
	this.redis = new Redis(this.config.redis);

	// attach the store
	this.store = store;

	// attach the engines
	this.engines = engines;
}

// lookup an entry in the cache manifest 
Stillframe.prototype.lookup = function lookup(engineId, hash, timestamp, ttl, callback) {
	return this.redis.eval(
		lookupScript, 1, [this.config.prefix, engineId, hash].join(':'),
		timestamp,                         // current request entry
		timestamp - ttl,                   // oldest acceptable complete entry
		timestamp - this.config.timeout,   // oldest acceptable pending entry
		timestamp + this.config.ttl,       // entry expiration
		callback
	);
};

// resolve a pending entry in the cache manifest
Stillframe.prototype.resolve = function resolve(engineId, hash, timestamp, callback) {
	return this.redis.eval(
		resolveScript, 1, [this.config.prefix, engineId, hash].join(':'),
		timestamp,                         // current request entry
		timestamp + this.config.ttl,       // entry expiration
		callback
	);
};


Stillframe.prototype.take = function take(engineId, request, options, ttl, callback) {
	var self = this;
	var switchboard = new stream.PassThrough();
	process.nextTick(function(){

		// make sure the engine exists
		if(!self.engines[engineId])
			return error(new Error('The engine "' + engineId + '" does not exist.'));

		// generate the hash
		var hash = hasher.sha1({request: request, options: options});

		// get the timestamp for this transaction
		var timestamp = Date.now();

		run();

		function error(err) {

			if(typeof callback === 'function') {
				callback(err);

				// if there's a callback, we'll only emit
				// an error if there are listeners
				if(switchboard.listeners('error').length)
					switchboard.emit('error', err);

			} else switchboard.emit('error', err);
		}

		function run(){

			// lookup hash in cache manifest
			return self.lookup(engineId, hash, timestamp, ttl, function(err, entry){
				if(err) return error(err);

				var engine = self.engines[engineId];




				// a cache entry exists
				if(typeof entry === 'string') {
					entry = entry.split(':');

					// callback with a snapshot
					if(typeof callback === 'function')
						callback(null, {status: entry[0], created: parseInt(entry[1], 10)});

					// cache is complete
					else if(entry[0] === 'complete') 
						self.store.fetch(engineId, engine, hash, parseInt(entry[1])).pipe(switchboard);

					// cache is pending; wait and try again...
					else setTimeout(run, self.config.retry);
				}





				// no valid cache found; we have already acquired a lock
				else {

					var readStream = engine.run(request, options);
					var writeStream = self.store.save(engineId, engine, hash, timestamp, ttl);
					
					// read from engineId
					readStream
					.on('error', function(e){
						err = e; switchboard.emit('error', err);
					})

					// pipe to the switchboard
					.pipe(switchboard)

					// write to the store
					.pipe(writeStream)
					.on('error', function(e){
						err = e; switchboard.emit('error', err);
					})

					// TODO: write any progress to redis
					.on('progress', function(e){})

					// resolve the cache manifest entry
					.on('finish', function(){
						if(!err) self.resolve(engineId, hash, timestamp, function(err){
							if(err) switchboard.emit('error', err);
						});
					});

					// callback with a snapshot
					if(typeof callback === 'function') return callback(null, {status: 'pending', created: timestamp});

				}






			});
		}

	});

	return switchboard;
};

module.exports = Stillframe;
