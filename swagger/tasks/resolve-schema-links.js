'use strict';

module.exports = function(grunt) {
	var path = require('path');
	var fs = require('fs');
	var resolve = require('json-refs').resolveRefs;

	// Deep walk obj, replacing 'ref' keys with '$ref'
	function replaceRefs(obj) {
		var k, v, i;

		for( k in obj ) {
			v = obj[k];

			if( k === 'ref' && typeof v === 'string' ) {
				obj['$ref'] = v;
				delete obj['ref'];
			}
			if( typeof v === 'object' ) {
				replaceRefs(v);
			} else if( typeof v === 'array' ) {
				for( i = 0; i < v.length; i++ ) {
					if( typeof v[i] === 'object' ) {
						replaceRefs(v[i]);
					}
				}
			}
		}
	}

	function resolveContent(res, callback) {
		var obj = JSON.parse(res.text); 
		replaceRefs(obj);
		callback(undefined, obj);
	}

	grunt.registerMultiTask('resolveSchemaLinks', 'Resolve schema references in swagger JSON file', function() {
		var src = this.data.src;
		var dest = this.data.dest;

		if(!fs.existsSync(src)) {
			grunt.log.writeln('Could not find:', src);
			return false;
		}

		var root = JSON.parse(fs.readFileSync(src).toString());
		replaceRefs(root);

		var resolveOpts = {
			filter: ['relative'],
			location: src,
			loaderOptions: {
				processContent: resolveContent
			}
		};

		var done = this.async();
		resolve(root, resolveOpts).then(function(results) {
			var data = JSON.stringify(results.resolved, null, 2);
			fs.writeFileSync(dest, data);
			done();
		});
	});
};

