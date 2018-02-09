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

		var context = {
			aliases: {}
		};

		// Walk through definitions, simplifying models where we can
		simplifyDefinitions(root, context);

		// walk through all schemas
		// That's every definition and every response and body schema
		root = walk(root, function(obj, path) {
			if( isSchema(path) ) {
				return simplifySchema(obj, path, context);
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

	function unformatPath(path) {
		if( !path.substr ) {
			grunt.log.writeln('Invalid path: ' + JSON.stringify(path));
			return path;
		}
		var parts = path.substr(2).split('/');
		return _.map(parts, function(el) {
			return el.replace(/~1/g, '/');
		});
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

	function isDefinition(path) {
		return ( path.length === 2 &&  path[0] === 'definitions' );
	}

	function simplifyDefinitions(root, context) {
		var defs = root.definitions||{};
		var keys = _.keys(defs);

		_.each(keys, function(k) {
			var schema = defs[k];
			var path = formatPath(['definitions', k]);

			if( schema.type === 'array' ) {
				// Setup an alias for array objects (don't generate a model)
				context.aliases[path] = simplifySchema(schema, ['definitions', k], context);
				delete defs[k];
			} else if( schema.allOf && schema.allOf.length === 1 && schema.allOf[0].$ref ) {
				// For objects that are just aliases for other objects, copy all of the properties
				var target = unformatPath(schema.allOf[0].$ref);
				var targetObj = resolvePathObj(root, target);
				if( targetObj ) {
					defs[k] = targetObj;
				} else {
					grunt.log.writeln('ERROR '.red + 'Cannot find alias for: ' + path + ' (' + schema.allOf[0].$ref + ')');
				}
			} else if( schema.$ref ) {
				// Replace pure references
				context.aliases[path] = schema;
				delete defs[k];
			} else if( Schemas.isPrimitiveType(schema.type) ) {
				// For simple types in definitions, alias them
				context.aliases[path] = schema;
				delete defs[k];
			}
		});
	}

	// Performs all of the simplifying steps, and
	// returns a simplified version of schema
	function simplifySchema(schema, path, context) {
		schema = _.cloneDeep(schema);
		// If an x-sdk-schema is specified, use that
		if( schema['x-sdk-schema'] ) {
			schema = schema['x-sdk-schema'];
		}

		if( !isValidSchema(schema) ) {
			grunt.log.writeln('WARNING '.red + 'Invalid schema (no object type specified) at: ' + formatPath(path));
			schema.type = 'object';
		} else if( schema.type === 'array' && schema.items ) {
			path = _.concat(path, 'items');
			schema.items = simplifySchema(schema.items, path, context);
		} else if( schema.allOf ) {
			if( schema.allOf.length === 1 ) {
				if( schema.allOf[0].$ref ) {
					var alias = context.aliases[schema.allOf[0].$ref];
					// Replace alias for allOf fields
					if( alias ) {
						schema = _.cloneDeep(alias); 
					} else {
						schema = schema.allOf[0];
					}
				} else if( Schemas.isPrimitiveType(schema.allOf[0].type) ) {
					schema = schema.allOf[0];
				} else {
					grunt.log.writeln('WARNING Cannot simplify "allOf" definition at: ' + formatPath(path));
				}
			} else {
				grunt.log.writeln('WARNING Cannot simplify "allOf" definition at: ' + formatPath(path));
			}
		} else if( schema.$ref ) {
			// Replace alias for $ref fields
			var alias = context.aliases[schema.$ref];
			if( alias ) {
				schema = _.cloneDeep(alias); 
			}
		}
		return schema;
	}

	function resolvePathObj(root, path) {
		var current = root;
		path = path.slice();
		while( current && path.length ) {
			current = current[path.shift()];
		}
		return current;
	}
};

	
