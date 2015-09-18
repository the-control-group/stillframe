'use strict';

var fs = require('fs');

function FileStore(config){
	this.config = config || {};
	this.config.path = this.config.path || __dirname + '/../../tmp';

	// make sure the directory exists
	try {
		fs.mkdirSync(this.config.path);
	} catch(err){
		if(err.code !== 'EEXIST') throw err;
	}
}

// fetch a file from cache
// @return instance of stream.Readable
FileStore.prototype.fetch = function fetch(engineId, hash, timestamp) {
	return fs.createReadStream(this.config.path + '/' + [engineId, hash, timestamp].join(':') );
};

// save a file to cache
// @return instance of stream.Writable
FileStore.prototype.save = function save(engineId, hash, timestamp, ttl) {
	return fs.createWriteStream(this.config.path + '/' + [engineId, hash, timestamp].join(':') );
};

module.exports = FileStore;