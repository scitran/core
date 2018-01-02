'use strict';

module.exports = function(grunt) {
	var path = require('path');
	var fs = require('fs');
	var _ = require('lodash');
	var yaml = require('js-yaml');
	var walk = require('../walk');
	var Schemas = require('../schemas');

	/**
	 * This task simplifies models in a swagger file.
	 * @param {object} data Task data
	 * @param {string} data.src The input file (root level swagger file)
	 * @param {string} data.dst The output file
	 */
	grunt.registerMultiTask('simplifySwagger', 'Simplify models in swagger API file', function() {
		var srcFile = this.data.src||'swagger.yaml';
		var dstFile = this.data.dst;
		
		if(!fs.existsSync(srcFile)) {
			grunt.log.error('Could not find:', srcFile);
			return false;
		}

		var root = yaml.safeLoad(fs.readFileSync(srcFile).toString());

		// walk through all schemas
		// That's every definition and every response and body schema
		root = walk(root, function(obj, path) {
			if( isSchema(path) ) {
				return simplifySchema(obj, path);
			}
			return obj;
		});

		var data = JSON.stringify(root, null, 2);
		fs.writeFileSync(dstFile, data);
	});

	function formatPath(path) {
		path = _.map(path, function(el) {
			return el.replace(/\//g, '~1');
		});
		return '#/' + path.join('/');
	}

	function isSchema(path) {
		if( path.length === 2 && path[0] === 'definitions' ) {
			return true;
		}
		if( path.length === 4 && path[0] === 'definitions' && path[2] === 'properties' ) {
			return true;
		}
		if( path.length > 1 && path[path.length-1] === 'schema' ) {
			return true;
		}
		return false;
	}

	function isValidSchema(schema) {
		return( schema.type || schema.$ref || 
			schema.allOf || schema.oneOf || schema.anyOf || schema.not );
	}

	// Performs all of the simplifying steps, and
	// returns a simplified version of schema
	function simplifySchema(schema, path) {
		schema = _.cloneDeep(schema);
		if( !isValidSchema(schema) ) {
			grunt.log.writeln('WARNING '.red + 'Invalid schema (no object type specified) at: ' + formatPath(path));
			schema.type = 'object';
		} else if( schema.type === 'array' && schema.items ) {
			path = _.concat(path, 'items');
			schema.items = simplifySchema(schema.items, path);				
		} else if( schema.allOf ) {
			if( schema.allOf.length === 1 ) {
				if( schema.allOf[0].$ref || Schemas.isPrimitiveType(schema.allOf[0].type) ) {
					schema = schema.allOf[0];
				} else {
					grunt.log.writeln('WARNING: Cannot simplify "allOf" definition at: ' + formatPath(path));
				}
			} else {
				grunt.log.writeln('WARNING: Cannot simplify "allOf" definition at: ' + formatPath(path));
			}
		}
		return schema;
	}

};

	
