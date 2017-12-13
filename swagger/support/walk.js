'use strict';

var _ = require('lodash');

/**
 * Deep-walk an object, invoking callback for each property.
 * Properties will be replaced with any value returned by callback.
 * @param obj The object to walk
 * @param {function} callback The callback function, that will be invoked with:
 *   `propertyValue, path, state`
 * @param {path} The current path array (default is empty array)
 * @param {state} An optional state parameter
 */
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
