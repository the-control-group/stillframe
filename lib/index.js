'use strict';

var fs = require('fs');
var Redis = require('ioredis');
var stream = require('stream');
var hasher = require('object-hash');

var lookupScript = fs.readFileSync(__dirname + '/lookup.lua');
var resolveScript = fs.readFileSync(__dirname + '/resolve.lua');

function Stillframe(config, store, engines) {

	if (!this instanceof Stillframe)
		return new Stillframe(config);

	if (!store || !engines || !config)
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
Stillframe.prototype.lookup = function lookup(hash, timestamp, ttl, callback) {
	return this.redis.eval(
		lookupScript, 1, [this.config.prefix, hash].join(':'),
		timestamp, // current request entry
		timestamp - ttl, // oldest acceptable complete entry
		timestamp - this.config.timeout, // oldest acceptable pending entry
		timestamp + this.config.ttl, // entry expiration
		callback
	);
};

// resolve a pending entry in the cache manifest
Stillframe.prototype.resolve = function resolve(hash, timestamp, callback) {
	return this.redis.eval(
		resolveScript, 1, [this.config.prefix, hash].join(':'),
		timestamp, // current request entry
		timestamp + this.config.ttl, // entry expiration
		callback
	);
};


Stillframe.prototype.take = function take(engineId, request, options, ttl, callback) {
	var self = this;
	var switchboard = new stream.PassThrough();
	process.nextTick(function() {

		// make sure the engine exists
		var engine = self.engines[engineId];
		if (!engine) return error(new Error('The engine "' + engineId + '" does not exist.'));

		// generate the hash
		var hash = hasher.sha1({
			engineId: engineId,
			request: request,
			options: options
		});

		// get the timestamp for this transaction
		var timestamp = Date.now();

		run();

		function error(err) {
			if (typeof callback === 'function') {
				callback(err);

				// if there's a callback, we'll only emit
				// an error if there are listeners
				if (switchboard.listeners('error').length)
					switchboard.emit('error', err);

			} else switchboard.emit('error', err);
		}

		function run() {

			// lookup hash in cache manifest
			return self.lookup(hash, timestamp, ttl, function(err, entry) {
				var readStream, writeStream, buffer = new stream.PassThrough();
				if (err) return error(err);






				// a cache entry exists
				if (typeof entry === 'string') {
					entry = entry.split(':');

					// callback with a snapshot
					if (typeof callback === 'function') {
						callback(null, {
							status: entry[0],
							created: parseInt(entry[1], 10)
						});
					}

					// cache is complete
					else if (entry[0] === 'complete') {
						readStream = self.store.fetch(hash, parseInt(entry[1], 10));
						readStream.pipe(buffer);

						// read from store
						readStream

						.on('error', error)

						// forward progress events
						.on('progress', function(p) {
							switchboard.emit('progress', p);
						})

						// start piping after metadata
						.once('metadata', function(m) {

							// forward metadata events
							switchboard.emit('metadata', m);

							// pipe to the switchboard
							buffer.pipe(switchboard);
						});
					}



					// TODO: instead of polling for completion, use redis pub/sub
					// to listen for and emit progress updates, fetching the file
					// once it's complete.

					// cache is pending; wait and try again
					else setTimeout(run, self.config.retry);
				}





				// no valid cache found; we have already acquired a lock
				else {

					readStream = engine.run(request, options);

					// read from engine
					readStream
					.on('error', error)

					// forward progress events
					.on('progress', function(p) {

						// TODO: write progress to redis entry and emit the progress
						// update using redis pub/sub

						switchboard.emit('progress', p);
					})

					// start piping after metadata
					.once('metadata', function(m) {

						// forward metadata events
						switchboard.emit('metadata', m);

						// write to the store
						writeStream = self.store.save(hash, timestamp, m, ttl);
						writeStream.on('error', error);
						switchboard.pipe(writeStream);

						// pipe to the switchboard
						readStream.pipe(switchboard);


						// resolve the cache manifest entry
						readStream.on('finish', function() {
							if (!err) self.resolve(hash, timestamp, function(err) {
								if (err) error(err);
							});
						});

					});

					// callback with a snapshot
					if (typeof callback === 'function') callback(null, {
						status: 'pending',
						created: timestamp
					});

				}






			});
		}

	});

	return switchboard;
};

module.exports = Stillframe;
