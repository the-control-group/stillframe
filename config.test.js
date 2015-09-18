module.exports = {
	prefix: 'stillframe_test' + Math.random(),
	timeout: 1000*60,
	retry: 1000*0.5,
	ttl: 1000*60*60*24,
	redis: {
		port: 6379,
		host: '127.0.0.1',
		options: {}
	}
}