'use strict';

module.exports = function(grunt) {
	var path = require('path');
	var fs = require('fs');
	var process = require('process');
	var yaml = require('js-yaml');
	var mustache = require('mustache');
	var _ = require('lodash');
	var walk = require('../walk');

	function resolveTemplatesFunc(templates) {
		var loaded_templates = {};
		return function(obj) {
			return walk(obj, function(obj) {
				var tmplpath, tmpl;

				if( obj.hasOwnProperty('$template') ) {
					tmplpath = path.join(templates, obj['$template']);
					if( !loaded_templates[tmplpath] ) {
						if( !fs.existsSync(tmplpath) ) {
							throw 'Template file does not exist: ' + tmplpath;
						}
						loaded_templates[tmplpath] = yaml.safeLoad(fs.readFileSync(tmplpath).toString());
					}

					tmpl = _.cloneDeep(loaded_templates[tmplpath]['template']);
					obj = walk(tmpl, resolveParamsFunc(obj['arguments']));
				}

				return obj;
			});
		};
	}

	function resolveIncludesFunc(templates) {
		return function(obj) {
			return walk(obj, function(obj) {
				var i, includes, incpath, inc;

				if( obj.hasOwnProperty('$include') ) {
					includes = obj['$include'];
					delete obj['$include'];
					for( i = 0; i < includes.length; i++ ) {
						incpath = path.join(templates, includes[i]);
						if( !fs.existsSync(incpath) ) {
							throw 'Included file does not exist: ' + incpath;
						}
						inc = yaml.safeLoad(fs.readFileSync(incpath).toString());
						_.extend(obj, inc);
					}
				}

				return obj;
			});
		};
	}

	function resolveParamsFunc(args) {
		return function(obj) {
			if( typeof obj === 'string' ) {
				return mustache.render(obj, args||{});
			}
			return obj;
		};
	}

	grunt.registerMultiTask('resolveTemplates', 'Resolve templates in swagger JSON file', function() {
		var src = this.data.src;
		var dest = this.data.dest;
		var templates = this.data.templates||process.cwd();
		var resolveTemplates = resolveTemplatesFunc(templates);
		var resolveIncludes = resolveIncludesFunc(templates);

		if(!fs.existsSync(src)) {
			grunt.log.writeln('Could not find:', src);
			return false;
		}

		var root = JSON.parse(fs.readFileSync(src).toString());
		try {
			root = resolveIncludes(root);
			root = resolveTemplates(root);
		} catch( e ) {
			grunt.log.writeln('Error resolving templates:', e);
			return false;
		}

		var data = JSON.stringify(root, null, 2);
		fs.writeFileSync(dest, data);
	});
};


