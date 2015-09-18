'use strict';

var stream = require('stream');

function EchoGenerator(config) {
	this.config = config || {};
};

EchoGenerator.extension = 'json';
EchoGenerator.mimetype  = 'application/json';

// run the generator
// @return instance of stream.Readable
EchoGenerator.prototype.run = function run(request, options) {
	var s = new stream.PassThrough();

	// DIRTY: this timeout exists for testing purposes
	setTimeout(function(){
		s.end(JSON.stringify(request));
	}, 50);
	
	return s;
};

module.exports = EchoGenerator;