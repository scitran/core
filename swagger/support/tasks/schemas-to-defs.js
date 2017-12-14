'use strict';

module.exports = function(grunt) {
	var fs = require('fs');
	var path = require('path');
	var yaml = require('js-yaml');
	var _ = require('lodash');
	var Schemas = require('../schemas');	

	/**
	 * This task flattens the nested swagger yaml into a single flat file.
	 * It does not resolve the JSON schema links.
	 * @param {object} options
	 * @param {string} options.format The output format, either 'yaml' or 'json' (default)
	 * @param {object} data Task data
	 * @param {string} data.srcFile The input file (root level swagger file)
	 * @param {string} data.dest The destination file (the flattened output file)
	 */
	grunt.registerMultiTask('schemasToDefs', 'Convert schemas to definitions', function() {
		var srcFile = this.data.srcFile;
		var dstFile = this.data.dstFile;

		var opts = {
			log: function() {
				grunt.log.writeln.apply(grunt.log, arguments);
			}
		};
		if( this.files.length ) {
			opts.cwd = this.files[0].orig.cwd;
		}

		if(!fs.existsSync(srcFile)) {
			grunt.log.writeln('Could not find:', srcFile);
			return false;
		}
		var root = yaml.safeLoad(fs.readFileSync(srcFile).toString());


		var schemas = new Schemas(opts);
		try {
			// Parse each of the input files
			this.files.forEach(function(file) {
				file.src.forEach(schemas.load.bind(schemas));
			});
		} catch(e) {
			grunt.log.writeln('Could not load files:', e);
			return false;
		}

		// Resolve all references
		try {
			schemas.resolve();
		} catch(e) {
			grunt.log.writeln('Could not resolve references:', e);
			return false;
		}

		// Add all definitions to root
		if( !root.definitions ) {
			root.definitions = {};
		}
		// _.extend(root.definitions, schemas.getComplexDefinitions());

		// Write destination file
		var data = JSON.stringify(root, null, 2);
		fs.writeFileSync(dstFile, data);
	});
};

