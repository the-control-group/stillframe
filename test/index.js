'use strict';

var fs = require('fs');

var stream = require('stream');



// Stub Engine
// -----------

function StubEngine(config) {
	this.config = config || {};
}

// run the generator
// @return instance of stream.Readable
StubEngine.prototype.run = function run(request, options) {
	var s = new stream.PassThrough();

	// DIRTY: this timeout exists for testing purposes, which
	// really isn't a good idea, but works in a pinch

	setTimeout(function() {
		s.emit('metadata', {
			contentType: 'application/json'
		});
	}, 10);

	setTimeout(function() {
		s.end(JSON.stringify(request));
	}, 50);

	return s;
};



// Stub Store
// ----------

function StubStore(config) {
	this.config = config || {};
	this.config.path = this.config.path || __dirname + '/../../tmp';
	this.data = {};

	// make sure the directory exists
	try {
		fs.mkdirSync(this.config.path);
	} catch (err) {
		if (err.code !== 'EEXIST') throw err;
	}
}

// fetch a file from cache
// @return instance of stream.Readable
StubStore.prototype.fetch = function fetch(hash, timestamp) {
	var self = this;
	var key = [hash, timestamp].join(':');
	var s = new stream.PassThrough();
	setTimeout(function() {
		s.emit('metadata', self.data[key].metadata );
		s.emit('data', self.data[key].data );
		s.emit('end');
	}, 10);
	return s;
};

// save a file to cache
// @return instance of stream.Writable
StubStore.prototype.save = function save(hash, timestamp, metadata, ttl) {
	var self = this;
	var key = [hash, timestamp].join(':');
	self.data[key] = {metadata: {}, data: ''};

	var s = new stream.Writable();
	s._write = function(chunk, encoding, callback) {
		self.data[key].data += chunk;
		callback();
	};

	return s;
};







var config = require('../config.test.js');
var Stillframe = require('../lib/index.js');
var redis = new(require('ioredis'))(config.redis);
var assert = require('chai').assert;


var tmp = __dirname + '/../tmp';

describe('Stillframe', function() {
	before(clean);

	describe('-> resolve', function() {
		var timestamp, stillframe;

		before(function() {
			timestamp = Date.now();
			stillframe = new Stillframe(config, new StubStore({
				path: tmp
			}), {
				stub: new StubEngine()
			});
		});

		describe('new entry', function() {
			it('should return null', function(done) {
				stillframe.resolve('804e772c9acf31c5d9321c491d0e1628ea04d985', timestamp, function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should create a `complete` entry', function(done) {
				redis.hget([config.prefix, '804e772c9acf31c5d9321c491d0e1628ea04d985'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should not create a `pending` entry', function(done) {
				redis.hget([config.prefix, '804e772c9acf31c5d9321c491d0e1628ea04d985'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should set an expiration for the entry', function(done) {
				// DIRTY: make Date.now deterministic; this might be better solved by wrapping the test in a with statement
				var now = Date.now;
				Date.now = function() {
					return timestamp;
				};

				getExpiration([config.prefix, '804e772c9acf31c5d9321c491d0e1628ea04d985'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + config.ttl);

					// restore Date.now
					Date.now = now;

					done(err);
				});
			});
		});

		describe('existing entry', function() {
			before(function(done) {
				redis.hset([config.prefix, '95578ef733b8c68839469854a0f05e4e683c0a16'].join(':'), 'complete', timestamp, done);
			});

			before(function(done) {
				redis.hset([config.prefix, '95578ef733b8c68839469854a0f05e4e683c0a16'].join(':'), 'pending', timestamp, done);
			});

			it('should return null for an existing key', function(done) {
				stillframe.resolve('95578ef733b8c68839469854a0f05e4e683c0a16', timestamp + 1000, function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should update the `complete` entry', function(done) {
				redis.hget([config.prefix, '95578ef733b8c68839469854a0f05e4e683c0a16'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + 1000);
					done();
				});
			});

			it('should not update a `pending` entry', function(done) {
				redis.hget([config.prefix, '95578ef733b8c68839469854a0f05e4e683c0a16'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should update the expiration for the entry', function(done) {
				getExpiration([config.prefix, '95578ef733b8c68839469854a0f05e4e683c0a16'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + 1000 + config.ttl);
					done(err);
				});
			});
		});

		after(clean);
	});

	describe('-> lookup', function() {
		var timestamp, ttl, stillframe;

		before(function() {
			timestamp = Date.now();
			ttl = 1000 * 60 * 60;
			stillframe = new Stillframe(config, new StubStore({
				path: tmp
			}), {
				stub: new StubEngine()
			});
		});

		describe('new entry', function() {
			it('should return null', function(done) {
				stillframe.lookup('b6589fc6ab0dc82cf12099d1c2d40ab994e8410c', timestamp, ttl, function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should create a `pending` entry', function(done) {
				redis.hget([config.prefix, 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should not create a `complete` entry', function(done) {
				redis.hget([config.prefix, 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should set an expiration for the entry', function(done) {
				getExpiration([config.prefix, 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + config.ttl);
					done(err);
				});
			});
		});

		describe('pending entry - inside timeout', function() {
			before(function(done) {
				redis.hset([config.prefix, '527d3f1d205d23678d303a0c52c98ec6e08b4e40'].join(':'), 'pending', timestamp, done);
			});

			it('should return the pending entry', function(done) {
				stillframe.lookup('527d3f1d205d23678d303a0c52c98ec6e08b4e40', timestamp + 1, ttl, function(err, res) {
					assert.isNull(err);
					assert.isString(res);
					assert.equal(res, 'pending:' + timestamp);
					done();
				});
			});

			it('should not update the `pending` entry', function(done) {
				redis.hget([config.prefix, '527d3f1d205d23678d303a0c52c98ec6e08b4e40'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should not update the `complete` entry', function(done) {
				redis.hget([config.prefix, '527d3f1d205d23678d303a0c52c98ec6e08b4e40'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should not update the entry expiration', function(done) {
				getExpiration([config.prefix, '527d3f1d205d23678d303a0c52c98ec6e08b4e40'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done(err);
				});
			});
		});

		describe('pending entry - past timeout', function() {
			before(function(done) {
				redis.hset([config.prefix, '76512cc7766948303ecbf3f9f498cbfe49a4f15e'].join(':'), 'pending', timestamp, done);
			});

			it('should return null', function(done) {
				stillframe.lookup('76512cc7766948303ecbf3f9f498cbfe49a4f15e', timestamp + config.timeout + 1, ttl, function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should update the `pending` entry', function(done) {
				redis.hget([config.prefix, '76512cc7766948303ecbf3f9f498cbfe49a4f15e'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + config.timeout + 1);
					done();
				});
			});

			it('should not update the `complete` entry', function(done) {
				redis.hget([config.prefix, '76512cc7766948303ecbf3f9f498cbfe49a4f15e'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should update the entry expiration', function(done) {
				getExpiration([config.prefix, '76512cc7766948303ecbf3f9f498cbfe49a4f15e'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + config.timeout + 1 + config.ttl);
					done(err);
				});
			});
		});

		describe('complete entry - inside ttl', function() {
			before(function(done) {
				redis.hset([config.prefix, '8c7925c217ce8a109c90e707085d2a7cb1f4b9b8'].join(':'), 'complete', timestamp, done);
			});

			before(function(done) {
				redis.hset([config.prefix, '8c7925c217ce8a109c90e707085d2a7cb1f4b9b8'].join(':'), 'pending', timestamp, done);
			});

			it('should return the complete entry', function(done) {
				stillframe.lookup('8c7925c217ce8a109c90e707085d2a7cb1f4b9b8', timestamp + 1, ttl, function(err, res) {
					assert.isNull(err);
					assert.isString(res);
					assert.equal(res, 'complete:' + timestamp);
					done();
				});
			});

			it('should not update the `pending` entry', function(done) {
				redis.hget([config.prefix, '8c7925c217ce8a109c90e707085d2a7cb1f4b9b8'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should not update the `complete` entry', function(done) {
				redis.hget([config.prefix, '8c7925c217ce8a109c90e707085d2a7cb1f4b9b8'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should not update the entry expiration', function(done) {
				getExpiration([config.prefix, '8c7925c217ce8a109c90e707085d2a7cb1f4b9b8'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done(err);
				});
			});
		});

		describe('complete entry - past ttl', function() {
			before(function(done) {
				redis.hset([config.prefix, '24440aa30f1dc8a7adbc57c35c648e6616179859'].join(':'), 'complete', timestamp, done);
			});

			before(function(done) {
				redis.hset([config.prefix, '24440aa30f1dc8a7adbc57c35c648e6616179859'].join(':'), 'pending', timestamp + ttl, done);
			});

			it('should return `pending` entry', function(done) {
				stillframe.lookup('24440aa30f1dc8a7adbc57c35c648e6616179859', timestamp + ttl + 1, ttl, function(err, res) {
					assert.isNull(err);
					assert.isString(res);
					assert.equal(res, 'pending:' + (timestamp + ttl));
					done();
				});
			});

			it('should not update the `pending` entry', function(done) {
				redis.hget([config.prefix, '24440aa30f1dc8a7adbc57c35c648e6616179859'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + ttl);
					done();
				});
			});

			it('should not update the `complete` entry', function(done) {
				redis.hget([config.prefix, '24440aa30f1dc8a7adbc57c35c648e6616179859'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should not update the entry expiration', function(done) {
				getExpiration([config.prefix, '24440aa30f1dc8a7adbc57c35c648e6616179859'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done(err);
				});
			});
		});

		describe('past complete ttl and pending timeout', function() {
			before(function(done) {
				redis.hset([config.prefix, 'b1245eaa50fe4ef9ec31a993a2e9b4dacfa27ab1'].join(':'), 'complete', timestamp, done);
			});

			before(function(done) {
				redis.hset([config.prefix, 'b1245eaa50fe4ef9ec31a993a2e9b4dacfa27ab1'].join(':'), 'pending', timestamp, done);
			});

			it('should return null', function(done) {
				stillframe.lookup('b1245eaa50fe4ef9ec31a993a2e9b4dacfa27ab1', timestamp + ttl + 1, ttl, function(err, res) {
					assert.isNull(err);
					assert.isNull(res);
					done();
				});
			});

			it('should update the `pending` entry', function(done) {
				redis.hget([config.prefix, 'b1245eaa50fe4ef9ec31a993a2e9b4dacfa27ab1'].join(':'), 'pending', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + ttl + 1);
					done();
				});
			});

			it('should not update the `complete` entry', function(done) {
				redis.hget([config.prefix, 'b1245eaa50fe4ef9ec31a993a2e9b4dacfa27ab1'].join(':'), 'complete', function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp);
					done();
				});
			});

			it('should update the entry expiration', function(done) {
				getExpiration([config.prefix, 'b1245eaa50fe4ef9ec31a993a2e9b4dacfa27ab1'].join(':'), function(err, res) {
					assert.isNull(err);
					assert.equal(res, timestamp + ttl + 1 + config.ttl);
					done(err);
				});
			});
		});

		after(clean);

	});

	describe('-> take', function() {
		var timestamp, ttl, stillframe;

		before(function() {
			timestamp = Date.now();
			ttl = 1000 * 60 * 60;
			stillframe = new Stillframe(config, new StubStore({
				path: tmp
			}), {
				stub: new StubEngine()
			});
		});

		describe('(callback)', function(done) {
			it('returns an error for a nonexistant engine', function(done) {
				stillframe.take('nonexistant', {
					url: 'http://www.example.com/callback'
				}, {}, ttl, function(err, snapshot) {
					assert.instanceOf(err, Error);
					done();
				});
			});

			it('should return a new pending snapshot', function(done) {
				// DIRTY: make Date.now deterministic; this might be better solved by wrapping the test in a with statement
				var now = Date.now;
				Date.now = function() {
					return timestamp;
				};

				stillframe.take('stub', {
					url: 'http://www.example.com/callback'
				}, {}, ttl, function(err, snapshot) {
					assert.isNull(err);
					assert.deepEqual(snapshot, {
						status: 'pending',
						created: timestamp
					});

					// restore Date.now
					Date.now = now;

					done();
				});
			});

			it('should return an existing pending snapshot', function(done) {
				// DIRTY: the stub engine has a 50ms timeout
				stillframe.take('stub', {
					url: 'http://www.example.com/callback'
				}, {}, ttl, function(err, snapshot) {
					assert.isNull(err);
					assert.deepEqual(snapshot, {
						status: 'pending',
						created: timestamp
					});
					done();
				});
			});

			it('should return an complete snapshot', function(done) {
				// DIRTY: the stub engine has a 50ms timeout
				setTimeout(function() {
					stillframe.take('stub', {
						url: 'http://www.example.com/callback'
					}, {}, ttl, function(err, snapshot) {
						assert.isNull(err);
						assert.deepEqual(snapshot, {
							status: 'complete',
							created: timestamp
						});
						done();
					});
				}, 60);
			});
		});


		describe('(stream)', function(done) {
			it('emits an error for a nonexistant engine', function(done) {
				stillframe.take('nonexistant', {
						url: 'http://www.example.com/stream'
					}, {}, ttl)
					.on('error', function(err) {
						assert.instanceOf(err, Error);
						done();
					});
			});

			it('should return the final result of a new pending snapshot', function(done) {
				// DIRTY: make Date.now deterministic; this might be better solved by wrapping the test in a with statement
				var now = Date.now;
				Date.now = function() {
					return timestamp;
				};

				var data = '';
				stillframe.take('stub', {
						url: 'http://www.example.com/stream'
					}, {}, ttl)
					.on('error', done)
					.on('data', function(chunk) {
						data += chunk.toString();
					})
					.on('finish', function() {
						assert.equal(data, '{"url":"http://www.example.com/stream"}');

						// restore Date.now
						Date.now = now;

						done();
					});
			});

			it('should return the final result of an existing pending snapshot', function(done) {
				// DIRTY: make Date.now deterministic; this might be better solved by wrapping the test in a with statement
				var now = Date.now;
				Date.now = function() {
					return timestamp;
				};

				var data = '';
				stillframe.take('stub', {
					url: 'http://www.example.com/stream2'
				}, {}, ttl, function(err, snapshot) {
					stillframe.take('stub', {
							url: 'http://www.example.com/stream2'
						}, {}, ttl)
						.on('error', done)
						.on('data', function(chunk) {
							data += chunk.toString();
						})
						.on('finish', function() {
							assert.equal(data, '{"url":"http://www.example.com/stream2"}');

							// restore Date.now
							Date.now = now;

							done();
						});
				});
			});

			it('should return the final result of a complete snapshot', function(done) {
				var data = '';
				stillframe.take('stub', {
						url: 'http://www.example.com/stream'
					}, {}, ttl)
					.on('error', done)
					.on('data', function(chunk) {
						data += chunk.toString();
					})
					.on('finish', function() {
						assert.equal(data, '{"url":"http://www.example.com/stream"}');
						done();
					});
			});
		});

		after(clean);
	});
});

function deleteFolderRecursive(path) {
	if (fs.existsSync(path)) {
		fs.readdirSync(path).forEach(function(file, index) {
			var curPath = path + '/' + file;
			if (fs.lstatSync(curPath).isDirectory()) { // recurse
				deleteFolderRecursive(curPath);
			} else { // delete file
				fs.unlinkSync(curPath);
			}
		});
		fs.rmdirSync(path);
	}
}


function getExpiration(key, callback) {
	return redis.eval('local time = redis.call(\'TIME\'); local ttl = redis.call(\'PTTL\', KEYS[1]); if(ttl < 0) then return nil; end; return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) + tonumber(ttl);', 1, key, callback);
}

function clean(callback) {
	deleteFolderRecursive(tmp);
	return redis.eval('local keys = redis.call(\'keys\', ARGV[1]) \n for i=1,#keys,5000 do \n redis.call(\'del\', unpack(keys, i, math.min(i+4999, #keys))) \n end \n return keys', 0, config.prefix, callback);
}
