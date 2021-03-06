'use strict';

var _ = require('lodash');

var RE_SCHEMA_URL_VERSION = /^http:\/\/json-schema.org\/(draft-\d+)\/schema/;
var RE_LOCAL_REF = /^#\/definitions\/([^\/]+)$/

// Properties that will be stripped from top-level schemas
var OMITTED_PROPERTIES = ['$schema', 'key_fields'];

/**
 * @class SchemaTranspiler
 * Converts from JSON Schema Specifications to OpenAPI
 * @param {object} options Compiler options
 * @param {function} options.log Optional function to print log messages
 */
var SchemaTranspiler = function(options) {
	options = options||{};
	this.log = options.log||function(){
		console.log.apply(console, arguments);
	};
};

/**
 * Log a warning to the log function.
 * @param {string|null} The optional id
 * @param {string} message The warning message
 */
SchemaTranspiler.prototype.warn = function(id, message) {
	message = 'Warning - ' + message;
	if( id ) {
		message = id + ': ' + message;
	}
	this.log(message);
};

/**
 * Convert the given schema to Open API 2.0 object definition.
 * @param {object} schema The schema object
 * @param {object} defs The definitions object that contains local references
 * @param {string} id Optional id for warning/error messages
 * @return {object} The converted schema
 */
SchemaTranspiler.prototype.toOpenApi2 = function(schema, defs, id) {
	// Determine schema version
	var version = null;
	if( schema.$schema ) {
		var m = RE_SCHEMA_URL_VERSION.exec(schema.$schema);
		if( m ) {
			version = m[1];
		}
	}
	if( !version || version === 'draft-04' ) {
		return this.draft4ToOpenApi2(schema, defs, id);
	}
	throw 'Unsupported schema version: ' + version;
}

// JSON Schema Draft-4 to OpenAPI 2.0 Object Definitions
// Assumes that all relative references have been resolved
// Transpiler rules
// 1. Delete $schema property
// 2. If type is an array, convert it to a single type
// 3. allOf, with one element: Replace inline
// 4. anyOf, oneOf, combine fields
SchemaTranspiler.prototype.draft4ToOpenApi2 = function(schema, defs, id) {
	var ref, defname;

	// Drop the $schema property, and make a copy
	schema = _.omit(schema, OMITTED_PROPERTIES);

	// Replace type array with type
	if( _.isArray(schema.type) ) {
		schema.type = this._selectTypeFromArray(schema.type, id);
	}

	// Check for top-level $ref, allOf, anyOf, oneOf
	if( schema.$ref && schema.example ) {
		// Special case, if object has $ref and example, then
		// resolve the ref, replacing all contents except example
		schema = this._mergeExampleWithRef(schema, defs, id);
	}

	if( schema.anyOf ) {
		// Merge anyOf properties (if object)
		schema = this._mergeAnyOrOneOf(schema, defs, 'anyOf', id);
	}

	if( schema.oneOf ) {
		// Merge oneOf properties (if object)
		schema = this._mergeAnyOrOneOf(schema, defs, 'oneOf', id);
	}

	if( schema.not ) {
		this.warn(id, '"not" is not supported in OpenApi 2');
		delete schema.not;
	}

	if( schema.patternProperties ) {
		var keys = _.keys(schema.patternProperties);
		if( keys.length > 1 ) {
			this.warn(id, 'Can only support one type in additionalProperties (from "patternProperties")');
		}
		schema.additionalProperties = this.draft4ToOpenApi2(schema.patternProperties[keys[0]], defs, id);
		delete schema.patternProperties;
	}

	if( schema.type === 'array' ) {
		// Check items
		if( schema.items ) {
			schema.items = this.draft4ToOpenApi2(schema.items, defs, id);
		} else {
			this.warn(id, 'items property on array!');
		}
	}

	if( schema.type === 'object' ) {
		// Check properties
		if( schema.properties ) {
			for( var k in schema.properties ) {
				if( schema.properties.hasOwnProperty(k) ) {
					schema.properties[k] = this.draft4ToOpenApi2(schema.properties[k], defs, id);
				}
			}
		}
	}

	return schema;
};

SchemaTranspiler.prototype._mergeExampleWithRef = function(schema, defs, id) {
	var ref, m;
	if( defs ) {
		m = RE_LOCAL_REF.exec(schema.$ref);
		if( m ) {
			ref = defs[m[1]];
			if( ref ) {
				schema = _.pickBy(schema, isKeyExample);
				_.extend(schema, ref);
			} else {
				this.warn(id, 'Could not find reference: ' + schema.$ref);
			}
		} else {
			this.warn(id, 'Non-local reference: ' + schema.$ref);
		}
	} else {
		this.warn(id, 'No definitions provided');
	}
	return schema;
};

SchemaTranspiler.prototype._flattenAllOf = function(schema) {
	var allOf = schema.allOf;
	delete schema.allOf;

	return _.extend(schema, allOf[0]);
};

SchemaTranspiler.prototype._mergeAnyOrOneOf = function(schema, defs, key, id) {
	var items = schema[key];
	delete schema[key];

	items = _.map(items, function(item) {
		return this._resolveRef(item, defs);
	}.bind(this));

	// Filter any null items
	items = _.filter(items, function(item) {
		return item.type !== 'null';
	});

	// If there's only one item left, use that
	if( items.length === 1 ) {
		return _.extend(schema, items[0]);
	}

	var canMerge = _.every(items, function(value) {
		return value.type === 'object' || value.type === undefined;
	});

	if( !canMerge ) {
		this.warn(id, 'Cannot merge "' + key + '" properties (they are not all objects)');
		// Just take the first type
		return _.extend(schema, items[0]);		
	}

	// Merge all properties for schema, ignore required / additional properties fields
	schema = _.extend({type: 'object', properties: {}}, schema);
	for( var i = 0; i < items.length; i++ ) {
		if( items[i].properties ) {
			_.extend(schema.properties, items[i].properties);
		}
	}	
	return schema;
};

SchemaTranspiler.prototype._selectTypeFromArray = function(types, id) {
	// If null is in the array, remove it then return the first type.
	types = _.filter(types, isTypeNotNull);

	if( types.length === 0 ) {
		return 'null';
	}
	if( types.length > 1 ) {
		this.warn(id, 'More than one non-null type in type array, returning the first one!');
	}
	return types[0];
};

SchemaTranspiler.prototype._resolveRef = function(schema, defs) {
	var m, ref;
	
	if( !defs ) {
		if( schema.$ref ) {
			this.warn(id, 'No definitions provided, cannot resolve: ' + schema.$ref);
		}
		return schema;
	}

	while( schema.$ref ) {
		m = RE_LOCAL_REF.exec(schema.$ref);
		if( !m ) {
			this.warn(id, 'Non-local reference: ' + schema.$ref);
			break;
		}
		ref = defs[m[1]];
		if( !ref ) {
			throw 'Error - definition ' + schema.$ref + ' not found';
		}
		schema = ref;
	}
	return schema;
};

function isTypeNotNull(type) {
	return type !== 'null';
}

function isKeyExample(_value, key) {
	return key === 'example';
}



module.exports = SchemaTranspiler;
