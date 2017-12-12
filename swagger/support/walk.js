'use strict';

var _ = require('lodash');

module.exports = function objWalk(obj, callback, path, state) {
	var i, idx;
	state = state || {};

	if( path ) {
		path = path.slice()
	} else {
		path = [];
	}
	idx = path.length;

	obj = callback(obj, path, state);

	if( _.isArray(obj) ) {
		for( i = 0; i < obj.length; i++ ) {
			path[idx] = '[' + i + ']';
			obj[i] = objWalk(obj[i], callback, path, _.cloneDeep(state));
		}
	} else if( _.isObjectLike(obj) ) {
		for( i in obj ) {
			if( obj.hasOwnProperty(i) ) {
				path[idx] = i;
				obj[i] = objWalk(obj[i], callback, path, _.cloneDeep(state));
			}
		}
	}

	return obj;
}
