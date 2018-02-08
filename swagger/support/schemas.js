'use strict';

var fs = require('fs');
var path = require('path');
var process = require('process');
var walk = require('./walk');
var _ = require('lodash');
var yaml = require('js-yaml');

var PRIMITIVE_TYPES = {
	'integer': true,
	'number': true,
	'string': true,
	'boolean': true,
	'null': true
};

function isPrimitiveType(type) {
	return !!PRIMITIVE_TYPES[type];
}

function normalizeName(name) {
	return name.replace('_', '-');
}

// Returns a definition name in snake case
function makeDefinitionName(relpath) {
	// Split on path separators
	var name = relpath.replace('\\', '/').split('/');

	// Remove the special 'definitions' prefix for top-level definitions
	if( name.length > 1 && name[0] === 'definitions' ) {
		name.splice(0, 1);
	}

	// Convert back to string
	name = name.join('-');

	// Remove extension
	var parts = /([-\w]+)(\..*)?$/.exec(name);
	return parts[1];
}

var Schemas = function(options) {
	options = options||{};
	this.cwd = path.resolve(options.cwd||process.cwd());
	this.log = options.log||function() {
		console.log.apply(console, arguments);
	};

	this.definitions = {};
	this.definitions_by_path = {};
};

Schemas.prototype.loadDefs = function() {
	var defsDir = path.join(this.cwd, 'definitions');
	fs.readdirSync(defsDir).forEach(function(filename) {
		this.load(path.join(defsDir, filename));
	}.bind(this));
	this.resolveDefs();
};

// Here's the plan
// Load all of the schemas specified
// Build up a reference list
// Create definitions
Schemas.prototype.load = function(file) {
	var relpath = path.relative(this.cwd, file);
	var schema = yaml.safeLoad(fs.readFileSync(file).toString());
	// Remove $schema attribute
	delete schema['$schema'];
	if( schema.hasOwnProperty('definitions') ) {
		this.addDefinitions(relpath, schema.definitions);
		// Remove extra definitions
		delete schema['definitions'];
	}
	if( schema.hasOwnProperty('type') ) {
		this.log('Warning - schema at ' + relpath + ' has a top-level definition');
		this.addSchema(relpath, schema);
	}
};

Schemas.prototype.addSchema = function(relpath, schema) {
	var name = makeDefinitionName(relpath);
	this.addDefinition(schema, name, relpath);
};

Schemas.prototype.addDefinitions = function(relpath, definitions) {
	var name, fullname, def, root = makeDefinitionName(relpath);
	for( name in definitions ) {
		if( definitions.hasOwnProperty(name) ) {
			def = definitions[name];
			
			// Convert name, special case to remove duplicate roots
			if( !_.startsWith(name, root) ) {
				fullname = root + '-' + name;
			} else {
				fullname = name;
			}

			this.addDefinition(def, fullname, relpath + '#/definitions/' + name);
		}
	}
};

Schemas.prototype.addDefinition = function(schema, name, relpath) {
	var properName = normalizeName(name);
	
	// Add the definition under proper name
	if( this.definitions.hasOwnProperty(properName) ) {
		var origpath = _.findKey(this.definitions_by_path, function(v) {
			return v === properName
		});
		this.log('Error - duplicate definition of ' + properName + ' found at: ' + relpath);
		this.log('  original defintion at: ' + origpath);
		return;
	}
	this.definitions[properName] = schema;
	this.definitions_by_path[relpath] = properName;
};

// Resolve all $ref/ref fields
Schemas.prototype.resolveDefs = function() {
	var relpath, name, idx;
	for( relpath in this.definitions_by_path ) {
		name = this.definitions_by_path[relpath];
		if( (idx = relpath.indexOf('#')) != -1 ) {
			relpath = relpath.substr(0, idx);
		}
		this.definitions[name] = walk(this.definitions[name], this.resolveRefs.bind(this, relpath));
	}
};

Schemas.prototype.resolve = function(obj, relpath) {
	relpath = relpath||this.cwd;
	return walk(obj, this.resolveRefs.bind(this, relpath));
};

Schemas.prototype.resolveRefs = function(relpath, obj, jsonpath) {
	var $ref, parts, curpath, actualpath, pathpart;
	// Ignore examples
	if( jsonpath && jsonpath[jsonpath.length-1] === 'example' ) {
		return obj;
	}
	if( obj && typeof obj === 'object' ) {
		if( !obj.hasOwnProperty('$ref') ) {
			return obj;
		}

		// Need to replace the $ref with a local reference
		$ref = obj['$ref'];
		if( $ref[0] === '#' ) {
			// references this file:
			actualpath = relpath + $ref;
		} else {
			// Determine the current path, relative to root
			curpath = path.dirname(path.resolve(this.cwd, relpath));
			parts = obj['$ref'].split('#');

			// If the user provided a path resolver, use that.
			if( typeof this.pathResolver === 'function' ) {
				pathpart = this.pathResolver(this.cwd, parts[0]);
			}

			// Otherwise, resolve the path relative to root
			if( !pathpart ) {
				pathpart = path.resolve(curpath, parts[0]);
				pathpart = path.relative(this.cwd, pathpart);
			}

			// If there is no hash value (e.g. 'xyz.json#') remove it
			parts[0] = pathpart;
			if( parts.length > 1 && parts[1] === '' ) {
				parts.splice(1, 1);
			}
			actualpath = parts.join('#');
		}

		if( this.definitions_by_path.hasOwnProperty(actualpath) ) {
			var defName = this.definitions_by_path[actualpath];
			// Replace simple types inline
			if( this.isPrimitiveDef(defName) ) {
				return _.cloneDeep(this.definitions[defName]);
			} else {
				obj['$ref'] = '#/definitions/' + defName;
			}
		} else {
			this.log('Error - Cannot find path for reference: ' + actualpath + ' in ' + relpath);
		}
	}
	return obj;
};

Schemas.prototype.isPrimitiveDef = function(name) {
	var def = this.definitions[name];
	if( def ) {
		return isPrimitiveType(def.type);
	}	
	return false;
};


Schemas.prototype.getComplexDefinitions = function() {
	return _.pickBy(this.definitions, function(value) {
		return !isPrimitiveType(value.type);
	});
};

module.exports = Schemas;

