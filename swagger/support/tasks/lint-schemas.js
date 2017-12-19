'use strict';

module.exports = function(grunt) {
	var SchemaLinter = require('../schema-lint');

	/**
	 * This task flattens the nested swagger yaml into a single flat file.
	 * It does not resolve the JSON schema links.
	 * @param {object} data Task data (See schema-lint.js for options)
	 */
	grunt.registerMultiTask('lintSchemas', 'Lint JSON schemas', function() {
		var i, errors;
		var linter = new SchemaLinter(this.data);

		try {
			errors = linter.lint();
		} catch( e ) {
			grunt.log.writeln('Error running lint:', e);
			return false;
		}

		for( i = 0; i < errors.length; i++ ) {
			grunt.log.error(errors[i].toString());
		}

		if( errors.length && this.data.failOnError !== false ) {
			return false;
		}
		return true;
	});
};


