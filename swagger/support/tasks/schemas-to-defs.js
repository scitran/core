'use strict';

module.exports = function(grunt) {
	var fs = require('fs');
	var path = require('path');
	var yaml = require('js-yaml');
	var _ = require('lodash');
	var resolve = require('json-refs').resolveRefs;
	var walk = require('../walk');
	var Schemas = require('../schemas');	

	/**
	 * This task flattens the nested swagger yaml into a single flat file.
	 * It does not resolve the JSON schema links.
	 * @param {object} options
	 * @param {string} options.format The output format, either 'yaml' or 'json' (default)
	 * @param {object} data Task data
	 * @param {string} data.src The input file (root level swagger file)
	 * @param {string} data.dest The destination file (the flattened output file)
	 * @param {string} data.schemasDir The root schema directory
	 */
	grunt.registerMultiTask('schemasToDefs', 'Convert schemas to definitions', function() {
		var srcFile = this.data.src;
		var dstFile = this.data.dest;

		var opts = {
			log: function() {
				grunt.log.writeln.apply(grunt.log, arguments);
			}
		};
		opts.cwd = this.data.schemasDir;

		if(!fs.existsSync(srcFile)) {
			grunt.log.writeln('Could not find:', srcFile);
			return false;
		}
		var root = yaml.safeLoad(fs.readFileSync(srcFile).toString());

		var schemas = new Schemas(opts);
		try {
			schemas.loadDefs();
		} catch(e) {
			grunt.log.writeln('Could not load files:', e);
			return false;
		}

		// Add all definitions to root
	 	root.definitions = _.extend(root.definitions||{}, schemas.getComplexDefinitions());

		schemas.pathResolver = function(cwd, relpath) {
			if( _.startsWith(relpath, '../definitions') ) {
				return relpath.substr(3);
			}
			return false;
		};

		// Resolve all references in the root yaml
		var resolveOpts = {
			filter: ['relative'],
			location: srcFile,
			loaderOptions: {
				processContent: function(res, callback) {
					var obj = JSON.parse(res.text);
					if( obj ) {
						delete obj['$schema'];
					}
					obj = schemas.resolve(obj);
					callback(undefined, obj);
				}
			}
		};

		var done = this.async();
		resolve(root, resolveOpts).then(function(results) {
			var data = JSON.stringify(results.resolved, null, 2);

			fs.writeFileSync(dstFile, data);
			done();
		});
	});
};

