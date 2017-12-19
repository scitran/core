'require strict';

var process = require('process');
var fs = require('fs');
var path = require('path');
var _ = require('lodash');
var walk = require('./walk');

var DEFAULT_OPTS = {
	anonymousObjects: true,
	topLevelDefinitions: true,
	definitionsInRefs: true,
	duplicateNames: true,
	definitionNameStyle: 'kebab',
	defDirs: [],
	refDirs: []
};

var KEBAB_NAME_RE = /^[a-z]+[a-z0-9]*(-[a-z]+[a-z0-9]*)*$/;

var NAME_VALIDATORS = {
	kebab: function(name) {
		return KEBAB_NAME_RE.test(name);
	},
	none: function() {
		return true;
	}
};

var LintError = function(message, filename) {
	this.message = message;
	this.filename = filename;
};
LintError.prototype.toString = function() {
	if( this.filename ) {
		return this.filename + ': ' + this.message;
	}
	return this.message;
};

/**
 * @class SchemaLinter
 *
 * Does basic style and sanity checking on schemas. Conceptually, there are two sets of schemas
 * that have different validation rules: Definitions, and References. Definitions are definitions
 * of simple and complex object schemas, whereas References should only reference definitions and not
 * declare their own schemas.
 *
 * @param {object} options Linter options
 * @param {array} options.defDirs Definition directories
 * @param {array} options.refDirs Reference-only directories
 * @param {boolean} options.anonymousObjects Whether or not to warn on anonymous objects
 * @param {boolean} options.topLevelDefinitions Whether or not to warn on file-level definitions
 * @param {boolean} options.definitionsInRefs Whether or not to warn on definitions in ref-only folders (input/output schemas)
 * @param {boolean} options.duplicateNames Whether or not to warn on duplicate definition names
 * @param {string} options.definitionNameStyle The defintion name style, or false to disable
 */
function SchemaLinter(options) {
	this.options = _.extend(DEFAULT_OPTS, options);

	this.log = options.log||function() {};

	if( this.options.definitionNameStyle !== false ) {
		this.validateName = NAME_VALIDATORS[this.options.definitionNameStyle];
		if( typeof this.validateName !== 'function' ) {
			throw 'Unknown definitionNameStyle: ' + this.options.definitionNameStyle;
		}
	} 

	this.defLinters = this._makeLinters('definitions');
	this.refLinters = this._makeLinters('references');

	this.reset();
};

SchemaLinter.prototype.reset = function() {
	this.defNames = {};
};

SchemaLinter.prototype.lint = function() {
	var results = [];

	this.reset();

	this.options.defDirs.forEach(function(dirpath) {
		var errors = this.lintDirectory(this.defLinters, dirpath);
		results = results.concat(errors);
	}.bind(this));

	this.options.refDirs.forEach(function(dirpath) {
		var errors = this.lintDirectory(this.refLinters, dirpath);
		results = results.concat(errors);
	}.bind(this));

	return results;
};

SchemaLinter.prototype.lintDirectory = function(linters, dirpath) {
	var results = [];

	fs.readdirSync(dirpath).forEach(function(name) {
		var filepath = path.join(dirpath, name);
		var errors = this.lintFile(linters, filepath);
		results = results.concat(errors);
	}.bind(this));
};

SchemaLinter.prototype.lintFile = function(linters, filename) {
	var schema;	

	try {
		schema = JSON.parse(fs.readFileSync(filename).toString());
	} catch( e ) {
		return [new LintError(e.toString(), filename)];
	}
	
	return this.lintSchema(linters, schema, filename);
};

SchemaLinter.prototype.lintSchema = function(linters, schema, filename) {
	var results = [];

	if( schema['x-lint'] === false ) {
		return results;
	}

	if( linters === 'definitions' ) {
		linters = this.defLinters;
	} else if( linters === 'references' ) {
		linters = this.refLinters;
	}

	for( i = 0; i < linters.length; i++ ) {
		errors = linters[i](schema, filename);
		if( errors ) {
			results = results.concat(errors);
		}
	}

	return results;
};

SchemaLinter.prototype._makeLinters = function(type) {
	var result = [];

	if( type === 'definitions' ) {
		// Definition only linters
		if( this.options.topLevelDefinitions ) {
			result.push(this._lintTopLevelDefinitions.bind(this));
		}
		if( this.options.duplicateNames ) {
			result.push(this._lintDuplicateNames.bind(this));
		}
		if( this.options.definitionNameStyle ) {
			result.push(this._lintDefinitionNameStyle.bind(this));
		}
	} else if( type === 'references' ) {
		// Reference only linters
		if( this.options.definitionsInRefs ) {
			result.push(this._lintDefsInRefs.bind(this));
		}
	}

	// Global linters
	if( this.options.anonymousObjects ) {
		result.push(this._lintAnonymousObjects.bind(this));
	}

	return result;
};

SchemaLinter.prototype._lintAnonymousObjects = function(schema, filename) {
	var errors = [];

	walk(schema, function(obj, path) {
		var maxDepth = 2;

		if( obj && obj.type === 'object' ) {
			// Object definition deeper than first level properties
			if( path[0] === 'definitions' ) {
				maxDepth = 4;
			}
			if( path.length >= maxDepth ) {
				errors.push(new LintError(
					'Anonymous object defined at ' + path.join('/'), filename));
			}
		}

		return obj;
	});

	return errors;
};

SchemaLinter.prototype._lintTopLevelDefinitions = function(schema, filename) {
	var errors = [];

	if( schema.type === 'object' && schema.properties ) {
		errors.push(new LintError('Top level definitions are not allowed', filename));
	}

	return errors;
};

SchemaLinter.prototype._lintDuplicateNames = function(schema, filename) {
	var k, errors = [];

	if( schema.definitions ) {
		for( k in schema.definitions ) {
			if( schema.definitions.hasOwnProperty(k) ) {
				if( this.defNames[k] ) {
					errors.push(new LintError('Duplicate definition found: "' + k + '"', filename)); 
				}
				this.defNames[k] = true;
			}
		}
	}

	return errors;
};

SchemaLinter.prototype._lintDefinitionNameStyle = function(schema, filename) {
	var k, errors = [];

	if( schema.definitions ) {
		for( k in schema.definitions ) {
			if( schema.definitions.hasOwnProperty(k) ) {
				if( !this.validateName(k) ) {
					errors.push(new LintError('Invalid definition name: "' + k + '"', filename));
				}
			}
		}
	}

	return errors;
};

SchemaLinter.prototype._lintDefsInRefs = function(schema, filename) {
	var errors = [];

	if( schema.definitions ) {
		errors.push(new LintError('Definitions not allowed in references', filename));
	}

	return errors;
};

module.exports = SchemaLinter;

