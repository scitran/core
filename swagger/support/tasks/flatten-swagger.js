'use strict';

module.exports = function(grunt) {
	var path = require('path');
	var fs = require('fs');
	var yaml = require('js-yaml');
	var resolve = require('json-refs').resolveRefs;

	// Use YAML to load nested content
	function resolveContent(res, callback) {
		callback(undefined, yaml.safeLoad(res.text));
	}

	/**
	 * This task flattens the nested swagger yaml into a single flat file.
	 * It does not resolve the JSON schema links.
	 * @param {object} options
	 * @param {string} options.format The output format, either 'yaml' or 'json' (default)
	 * @param {object} data Task data
	 * @param {string} data.apiFile The input file (root level swagger file)
	 * @param {string} data.dest The destination file (the flattened output file)
	 */
	grunt.registerMultiTask('flattenSwagger', 'Resolve references in swagger YAML files', function() {
		var apiFile = this.data.apiFile||'swagger.yml';
		var destFile = this.data.dest||'swagger.json';

		// See: http://azimi.me/2015/07/16/split-swagger-into-smaller-files.html
		// and the corresponding repo: https://github.com/mohsen1/multi-file-swagger-example
		if(!fs.existsSync(apiFile)) {
			grunt.log.writeln('Could not find:', apiFile);
			return false;
		}

		var options = this.options({
			format: 'json'
		});

		var root = yaml.safeLoad(fs.readFileSync(apiFile).toString());
		var resolveOpts = {
			filter: ['relative'],
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

