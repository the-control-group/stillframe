'use strict';

var stream = require('stream');
var assert = require('chai').assert;
var EchoGenerator = require('../../lib/generators/echo.js');

describe('EchoGenerator', function(){
	var echo = new EchoGenerator();

	describe('run', function(){
		it('returns a readable stream', function(){
			var s = echo.run({url: 'http://www.example.com'});
			assert.instanceOf(s, stream.Readable);
		});

		it('streams the json-encoded request', function(done){
			var data = '';
			var s = echo.run({url: 'http://www.example.com'});
			s.on('data', function(d){ data += d; });
			s.on('end', function(){
				assert.equal(data, '{"url":"http://www.example.com"}');
				done();
			});
		});
	});

});