'use strict';

module.exports = function(grunt) {
	var _ = require('lodash');
	var path = require('path');
	var fs = require('fs');
	var yaml = require('js-yaml');

	var SUPPORTED_OPS = {
		'get': true,
		'post': true,
		'put': true,
		'patch': true,
		'delete': true,
		'head': true,
		'options': true,
		'trace': true
	};

	function pathToRegexStr(path) {
		return path.replace(/\{[^/]+\}/g, '[^/]+');
	}

	/**
	 * This task prints missing endpoints in API documentation
	 * @param {object} options
	 * @param {string} options.format The output format, either 'yaml' or 'json' (default)
	 * @param {object} data Task data
	 * @param {string} data.src The input file (compiled swagger file)
	 * @param {string} data.endpoints The file containing the endpoint list
	 * @param {array} data.ignoredPaths A list of regular expressions of paths to ignore for errors
	 * @param {boolean} data.failOnErrors Set to false to prevent errors from failing the build
	 */
	grunt.registerMultiTask('printDocCoverage', 'Print endpoints that are missing from docs', function() {
		var srcFile = this.data.src;
		var endpointsFile = this.data.endpoints;
		var ignored = this.data.ignoredPaths||[];

		if(!fs.existsSync(srcFile)) {
			grunt.log.writeln('Could not find:', srcFile);
			return false;
		}
		if(!fs.existsSync(endpointsFile)) {
			grunt.log.writeln('Could not find endpoints file, please run integration tests!'.red);
			return;
		}
	
		var isIgnored = function(path) {
			return _.some(ignored, function(spec) { if( spec instanceof RegExp ) {
					return spec.test(path);
				}
				return path === spec;
			});
		};

		var hasDescription = function(obj) {
			return obj.summary || obj.description;
		};

		var root = yaml.safeLoad(fs.readFileSync(srcFile).toString());
		var endpoints = yaml.safeLoad(fs.readFileSync(endpointsFile).toString());

		// convert all endpoints in swagger docs to regular expressions
		var endpointRegexes = [];
		var basePath = root.basePath;
		var errors = false;

		_.forEach(root.paths, function(def, path) {
			var pathRegex = pathToRegexStr(basePath + path);
			_.forEach(def, function(obj, method) {
				if( SUPPORTED_OPS[method] ) {
					var reStr = method.toUpperCase() + ' ' + pathRegex;
					var pathDesc = method.toUpperCase() + ' ' + path;

					endpointRegexes.push(new RegExp(reStr));

					// Also print an error if there is no (or empty) summary or description on method
					if( !hasDescription(obj) ) {
						grunt.log.writeln('ERROR '.red + pathDesc + ' is missing description');
						errors = true;
					}
					
					// Also print a warning if there is no (or empty) description on response
					_.forEach(obj.responses, function(resp, code) {
						if( !hasDescription(resp) ) {
							grunt.log.writeln('WARNING ' + pathDesc + ' ' + code + ' response is missing description'); 
						}	
					});
				}
			});
		});
	
		// Go through the list of accessed endpoints, matching them to defined endpoints
		_.forEach(endpoints, function(ep) {
			if( isIgnored(ep) ) {
				return;
			}

			var matched = _.some(endpointRegexes, function(re) {
				return re.test(ep);
			});
			if( !matched ) {
				grunt.log.writeln('ERROR '.red + ep + ' is undocumented!');
				errors = true;
			}
		});

		if( errors && this.data.failOnErrors !== false ) {
			return false;
		}
	});

};

