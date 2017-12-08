'use strict';

var loadTasks = require('load-grunt-tasks');

module.exports = function(grunt) {
	require('load-grunt-tasks')(grunt);
	grunt.task.loadTasks('tasks/');

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		
		/**
		 * Copy schema files into build. Once we fully transition to swagger,
		 * the schema files should live in this subdirectory permanently.
		 */
		copy: {
			schema: {
				files: [
					{ expand: true, cwd: '../raml/schemas', src: ['**'], dest: 'build/schemas' }
				]
			}
		},

		/**
		 * Flatten the swagger file into an intermediate JSON file.
		 * Schemas will not be resolved during this step
		 */
		flattenSwagger: {
			core: {
				apiFile: 'index.yaml',
				dest: 'build/swagger-flat.json'
			}
		}
	});

	grunt.registerTask('default', ['copy', 'flattenSwagger']);
};


