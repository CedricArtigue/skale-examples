#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.on('connect', function() {
	console.log('Hello world')
	sc.end();
})

