'use strict';

var _ = require('lodash');

var RE_SCHEMA_URL_VERSION = /^http:\/\/json-schema.org\/(draft-\d+)\/schema/;
var RE_LOCAL_REF = /^#\/definitions\/([^\/]+)$/

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
 * Convert the given schema to Open API 2.0 object definition.
 * @param {object} schema The schema object
 * @param {object} defs The definitions object that contains local references
 * @return {object} The converted schema
 */
SchemaTranspiler.prototype.toOpenApi2 = function(schema, defs) {
	// Determine schema version
	var version = null;
	if( schema.$schema ) {
		var m = RE_SCHEMA_URL_VERSION.exec(schema.$schema);
		if( m ) {
			version = m[1];
		}
	}
	if( !version || version === 'draft-04' ) {
		return this.draft4ToOpenApi2(schema, defs);
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
SchemaTranspiler.prototype.draft4ToOpenApi2 = function(schema, defs) {
	var ref, defname;

	// Drop the $schema property, and make a copy
	schema = _.cloneDeep(schema);
	delete schema.$schema;

	// Replace type array with type
	if( _.isArray(schema.type) ) {
		schema.type = this._selectTypeFromArray(schema.type);
	}

	if( schema.allOf && schema.allOf.length === 1 && !schema.required ) {
		// Merge all of object with top-level object
		schema = this._flattenAllOf(schema);
	}

	// Check for top-level $ref, allOf, anyOf, oneOf
	if( schema.$ref && schema.example ) {
		// Special case, if object has $ref and example, then
		// resolve the ref, replacing all contents except example
		schema = this._mergeExampleWithRef(schema, defs);
	}

	if( schema.anyOf ) {
		this.log('Warning - "anyOf" not supported in OpenApi 2');
		// Merge anyOf properties (if object)
		schema = this._mergeAnyOrOneOf(schema, defs, 'anyOf');
	}

	if( schema.oneOf ) {
		this.log('Warning - "oneOf" not supported in OpenApi 2');
		// Merge oneOf properties (if object)
		schema = this._mergeAnyOrOneOf(schema, defs, 'oneOf');
	}

	if( schema.not ) {
		this.log('Warning - "not" is not supported in OpenApi 2');
		delete schema.not;
	}

	if( schema.type === 'array' ) {
		// Check items
		if( schema.items ) {
			schema.items = this.draft4ToOpenApi2(schema.items, defs);
		} else {
			this.log('Warning - No items property on array!');
		}
	}

	if( schema.type === 'object' ) {
		// Check properties
		if( schema.properties ) {
			for( var k in schema.properties ) {
				if( schema.properties.hasOwnProperty(k) ) {
					schema.properties[k] = this.draft4ToOpenApi2(schema.properties[k], defs);
				}
			}
		}
	}

	return schema;
};

SchemaTranspiler.prototype._mergeExampleWithRef = function(schema, defs) {
	var ref, m;
	if( defs ) {
		m = RE_LOCAL_REF.exec(schema.$ref);
		if( m ) {
			ref = defs[m[1]];
			if( ref ) {
				schema = _.pickBy(schema, isKeyExample);
				_.extend(schema, ref);
			} else {
				this.log('Warning - could not find reference: ' + schema.$ref);
			}
		} else {
			this.log('Warning - non-local reference: ' + schema.$ref);
		}
	} else {
		this.log('Warning - no definitions provided');
	}
	return schema;
};

SchemaTranspiler.prototype._flattenAllOf = function(schema) {
	var allOf = schema.allOf;
	delete schema.allOf;

	return _.extend(schema, allOf[0]);
};

SchemaTranspiler.prototype._mergeAnyOrOneOf = function(schema, defs, key) {
	var items = schema[key];
	delete schema[key];

	items = _.map(items, function(item) {
		return this._resolveRef(item, defs);
	}.bind(this));

	var canMerge = _.every(items, function(value) {
		return value.type === 'object';
	});

	if( !canMerge ) {
		this.log('Error - cannot merge "' + key + '" properties (they are not all objects)');
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

SchemaTranspiler.prototype._selectTypeFromArray = function(types) {
	// If null is in the array, remove it then return the first type.
	types = _.filter(types, isTypeNotNull);

	if( types.length === 0 ) {
		return 'null';
	}
	if( types.length > 1 ) {
		this.log('Warning - more than one non-null type in type array, returning the first one!');
	}
	return types[0];
};

SchemaTranspiler.prototype._resolveRef = function(schema, defs) {
	var m, ref;
	
	if( !defs ) {
		if( schema.$ref ) {
			this.log('Warning - no definitions provided, cannot resolve: ' + schema.$ref);
		}
		return schema;
	}

	while( schema.$ref ) {
		m = RE_LOCAL_REF.exec(schema.$ref);
		if( !m ) {
			this.log('Warning - non-local reference: ' + schema.$ref);
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
