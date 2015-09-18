'use strict';

var stream = require('stream');

function EchoEngine(config) {
	this.config = config || {};
};

// run the generator
// @return instance of stream.Readable
EchoEngine.prototype.run = function run(request, options) {
	var s = new stream.PassThrough();
	s.metadata = {
		type: 'application/json',
		extension: 'json'
	};

	// DIRTY: this timeout exists for testing purposes
	setTimeout(function(){
		s.end(JSON.stringify(request));
	}, 50);
	
	return s;
};

module.exports = EchoEngine;