'use strict';

var _ = require('lodash');
var path = require('path');
var fs = require('fs');
var process = require('process');
var yaml = require('js-yaml');
var pluralize = require('pluralize');
var Mustache = require('mustache');

var walk = require('./walk');

var TEMPLATE_FUNCS = {
	'pluralize': function() {
		return function(text, render) {
			return pluralize.plural(render(text));
		};
	},
	'singularize': function() {
		return function(text, render) {
			return pluralize.singular(render(text));
		};
	}
};

// Throws if args are missing
function validateTemplateArgs(tmplpath, template, args) {
	var i, param;
	if( !template || !template.parameters ) {
		return;
	}
	for( i = 0; i < template.parameters.length; i++ ) {
		param = template.parameters[i];
		if( !param.name ) {
			throw 'Template "' + tmplpath + '" parameter does not have a name!';
		}
		if( _.isNil(args[param.name]) ) {
			throw 'Template "' + tmplpath + '" invocation is missing parameter: ' + param.name;
		}
	}
}

/**
 * @class SwaggerResolver
 * Performs stateful resolution of $include and $template directives
 * in YAML files.
 * @param {object} options The optional configuration
 * @param {string} options.path The root path to resolve templates and includes from. (Defaults to cwd)
 * @param {function} options.log The optional logging function
 */
var SwaggerResolver = function(options) {
	options = options||{};
	this.path = options.path||process.cwd();
	this.templates = {};
	this.log = options.log||function() {
		console.log.apply(console, arguments);
	};
};

/**
 * Recursively resolve any includes and templates in the data.
 * @param {object} data The data object
 * @param {string} data.text The text data, in string format. (Either JSON or YAML)
 */
SwaggerResolver.prototype.resolveContent = function(data) {
	// Reset template arguments for this run
	// Possible issue: templates including templates will smash this, can we make it
	// more stack oriented?
	this.templateArguments = null;

	// Do conversion first
	var obj = yaml.safeLoad(data.text);
	return this.resolveObject(obj);
};

SwaggerResolver.prototype.resolveObject = function(obj) {
	// Perform a deep-walk of the object, replacing 
	return walk(obj, this.visit.bind(this));
};

SwaggerResolver.prototype.loadFile = function(relpath) {
	var abspath = path.join(this.path, relpath);
	if( !fs.existsSync(abspath) ) {
		throw 'File does not exist: ' + abspath;
	}
	return yaml.safeLoad(fs.readFileSync(abspath).toString());
};

SwaggerResolver.prototype.visit = function(obj) {
	if( !obj ) {
		return obj;
	}

	// obj will have $template or $include, not both
	if( obj.hasOwnProperty('$include') ) {
		obj = this.resolveIncludes(obj);
	} else if( obj.hasOwnProperty('$template') ) {
		obj = this.resolveTemplate(obj);

		// Recursively resolve content
		obj = this.resolveObject(obj);
	} else if( obj.hasOwnProperty('$template_arguments') ) {
		// Save off template arguments for later
		this.templateArguments = obj['$template_arguments'];
		delete obj['$template_arguments'];
	}

	return obj;
};

SwaggerResolver.prototype.resolveIncludes = function(obj) {
	var i, includes, inc;

	includes = obj['$include'];
	if( typeof includes === 'string' ) {
		includes = [includes];
	}

	delete obj['$include'];
	for( i = 0; i < includes.length; i++ ) {
		// Load and resolve the include file
		inc = this.loadFile(includes[i]);
		inc = this.resolveObject(inc);

		// And merge its contents into obj
		_.extend(obj, inc);
	}
	
	return obj;
};

SwaggerResolver.prototype.resolveTemplate = function(obj) {
	var tmplpath, tmpl, args, text;

	tmplpath = obj['$template'];
	tmpl = this.templates[tmplpath];

	if( !tmpl ) {
		tmpl = this.loadFile(tmplpath);
		this.templates[tmplpath] = tmpl;
		Mustache.parse(tmpl.template);
	}

	// Validate arguments?
	args = _.extend({}, TEMPLATE_FUNCS, this.templateArguments, obj['arguments']);
	validateTemplateArgs(tmplpath, tmpl, args);

	// Render the template, and parse
	text = Mustache.render(tmpl.template, args);
	return yaml.safeLoad(text);
};

module.exports = SwaggerResolver;

