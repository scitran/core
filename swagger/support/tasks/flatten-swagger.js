'use strict';

module.exports = function(grunt) {
	var path = require('path');
	var fs = require('fs');
	var yaml = require('js-yaml');
	var resolve = require('json-refs').resolveRefs;

	var SwaggerResolver = require('../swagger-resolver');

	// Filter out non-relative paths, and any schema $refs
	function refFilter(ref, path) {
		if( ref.type !== 'relative' ) {
			return false;
		}
		if( path && path.length > 0 && path[path.length-1] === 'schema' ) {
			return false;
		}
		return true;
	}

	/**
	 * This task flattens the nested swagger yaml into a single flat file.
	 * It does not resolve the JSON schema links.
	 * @param {object} options
	 * @param {string} options.format The output format, either 'yaml' or 'json' (default)
	 * @param {object} data Task data
	 * @param {string} data.src The input file (root level swagger file)
	 * @param {string} data.dest The destination file (the flattened output file)
	 */
	grunt.registerMultiTask('flattenSwagger', 'Resolve references in swagger YAML files', function() {
		var srcFile = this.data.src||'swagger.yml';
		var destFile = this.data.dest||'swagger.json';
		var resolver = new SwaggerResolver({
			log: function() {
				grunt.log.writeln.apply(grunt.log, arguments);
			}
		});

		function resolveContent(res, callback) {
			callback(undefined, resolver.resolveContent(res));
		}

		// See: http://azimi.me/2015/07/16/split-swagger-into-smaller-files.html
		// and the corresponding repo: https://github.com/mohsen1/multi-file-swagger-example
		if(!fs.existsSync(srcFile)) {
			grunt.log.writeln('Could not find:', srcFile);
			return false;
		}

		var options = this.options({
			format: 'json'
		});

		var root = yaml.safeLoad(fs.readFileSync(srcFile).toString());

		// Resolve any top-level includes or templates
		try {
			root = resolver.resolveObject(root);
		} catch (e) {
			grunt.log.writeln("Could not resolve root:", e);
			return false;
		}

		var resolveOpts = {
			filter: refFilter,
			loaderOptions: {
				processContent: resolveContent
			}
		};

		var done = this.async();
		resolve(root, resolveOpts).then(function(results) {
			var data;
			if( options.format === 'yaml' ) {
				data = yaml.safeDump(results.resolved);
			} else if( options.format === 'json' ) {
				data = JSON.stringify(results.resolved, null, 2);
			}

			fs.writeFileSync(destFile, data);
			done();
		});
	});
};

