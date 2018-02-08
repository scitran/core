'use strict';

module.exports = function(grunt) {
	var path = require('path');
	var fs = require('fs');
	var _ = require('lodash');
	var yaml = require('js-yaml');
	var spec = require('swagger-tools').specs.v2;

	function formatPath(path) {
		path = _.map(path, function(el) {
			return el.replace(/\//g, '~1');
		});
		return '#/' + path.join('/');
	}

	function formatInner(err, indent) {
		var result = '';
		if( err.inner ) {
			indent = indent||'  ';
			err.inner.forEach(function(inner) {
				result = result + '\n' + indent + 
					formatPath(inner.path) + ': ' + inner.message;
				result = result + formatInner(inner, indent + '  ');
			});
		}
		return result;
	}

	function formatError(prefix, err) {
		return prefix + formatPath(err.path) + ': ' + err.message
			+ formatInner(err);
	}

	/**
	 * This task simply runs swagger-tools validation on the final swagger doc. 
	 * @param {object} data Task data
	 * @param {string} data.src The input file (root level swagger file)
	 * @param {array} data.ignoreWarnings The optional list of warnings to ignore
	 */
	grunt.registerMultiTask('validateSwagger', 'Validate swagger API file', function() {
		var srcFile = this.data.src||'swagger.yaml';
		var ignoreWarnings = this.data.ignoreWarnings||[];
		
		if(!fs.existsSync(srcFile)) {
			grunt.log.error('Could not find:', srcFile);
			return false;
		}

		var done = this.async();
		var root = yaml.safeLoad(fs.readFileSync(srcFile).toString());

		spec.validate(root, function(err, result) {
			if( err ) {
				grunt.log.error('Unable to validate swagger document: ' + err);
				done(false);
				return;
			}


			if( ignoreWarnings && ignoreWarnings.length ) {
				result.warnings = _.filter(result.warnings, function(err) {
					return (ignoreWarnings.indexOf(err.code) === -1 );
				});
			}

			result.warnings.forEach(function(err) {
				// Print codes for warnings so that we can disable the undesirables
				var pfx = 'WARNING (' + err.code + ') ';
				grunt.log.writeln(formatError(pfx, err));
			});

			result.errors.forEach(function(err) {
				grunt.log.writeln(formatError('ERROR '.red, err));
			});


			if( result.errors.length || result.warnings.length ) {
				grunt.log.writeln();
				grunt.log.writeln(result.errors.length + ' errors and ' + result.warnings.length + ' warnings.');
			}

			if( result.errors.length > 0 ) {
				grunt.log.error('Swagger file is invalid!');
				done(false);
			} else {
				grunt.log.ok('Swagger file is valid!');
				done();
			}
		});

	});
};

