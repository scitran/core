'use strict';

var loadTasks = require('load-grunt-tasks');
var SWAGGER_UI_PORT = 9009;
var SWAGGER_UI_LIVE_RELOAD_PORT = 19009;

module.exports = function(grunt) {
	require('load-grunt-tasks')(grunt);
	grunt.task.loadTasks('tasks/');

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		
		copy: {
			/**
			 * Copy schema files into build. Once we fully transition to swagger,
			 * the schema files should live in this subdirectory permanently.
			 */
			schema: {
				files: [
					{ 
						expand: true, 
						cwd: '../raml/schemas', 
						src: ['**'], 
						dest: 'build/schemas' 
					}
				]
			},
			/**
			 * Copy swagger ui dist and config files
			 */
			swaggerUi: {
				files: [
					{
						expand: true,
						cwd: 'swagger-ui',
						src: ['**'],
						dest: 'build/swagger-ui'
					},
					{
						expand: true, 
						cwd: './node_modules/swagger-ui-dist', 
						src: [
							'swagger-ui-bundle.js',
							'swagger-ui-standalone-preset.js',
							'*.png',
							'*.css'
						],
						dest: 'build/swagger-ui'
					}
				]
			},
			/**
			 * Copy swagger to swagger-ui
			 */
			swaggerUiSchema: {
				src: 'build/swagger-flat.json',
				dest: 'build/swagger-ui/swagger.json'
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
		},

		/**
		 * Static hosting for swagger-ui docs
		 */
		connect: {
			uiServer: {
				options: {
					port: SWAGGER_UI_PORT,
					base: 'build/swagger-ui',
					livereload: SWAGGER_UI_LIVE_RELOAD_PORT,
					open: true
				}
			}
		},

		/**
		 * Live reload for swagger-ui
		 */
		watch: {
			apis: {
				options: {
					livereload: SWAGGER_UI_LIVE_RELOAD_PORT
				},
				files: [
					'**/*.yaml',
					'../raml/schemas/**/*.json'
				],
				tasks: [
					'build-schema',
					'copy:swaggerUiSchema'
				]
			}
		}
	});

	/**
	 * Build the swagger schemas
	 */
	grunt.registerTask('build-schema', [
		'copy:schema', 
		'flattenSwagger'
	]);

	/**
	 * Build swagger-ui
	 */
	grunt.registerTask('build-ui', [
		'build-schema',
		'copy:swaggerUi',
		'copy:swaggerUiSchema'
	]);

	grunt.registerTask('default', ['build-ui']);

	/**
	 * Run a live server with swagger-ui
	 */
	grunt.registerTask('live', ['build-ui', 'connect', 'watch']);
};


