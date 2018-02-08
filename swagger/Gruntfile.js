'use strict';

var loadTasks = require('load-grunt-tasks');
var SWAGGER_UI_PORT = 9009;
var SWAGGER_UI_LIVE_RELOAD_PORT = 19009;

module.exports = function(grunt) {
	loadTasks(grunt);
	grunt.task.loadTasks('support/tasks/');

	grunt.task.registerTask('createBuildDir', function() {
		grunt.file.mkdir('build');
	});

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		
		lintSchemas: {
			core: {
				defDirs: [
					'schemas/definitions'
				],
				refDirs: [
					'schemas/input',
					'schemas/output'
				]
			}
		},

		copy: {
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
				src: 'build/swagger-ui.json',
				dest: 'build/swagger-ui/swagger.json'
			}
		},

		/**
		 * Flatten the swagger file into an intermediate JSON file.
		 * Schemas will not be resolved during this step
		 */
		flattenSwagger: {
			core: {
				src: 'index.yaml',
				dest: 'build/swagger-flat.json',
				version: grunt.option('docs-version')
			}
		},

		/**
		 * Convert schema files to swagger definitions
		 */
		schemasToDefs: {
			core: {
				src: 'build/swagger-flat.json',
				dest: 'build/swagger-ui.json',
				schemasDir: './schemas',
				location: 'index.yaml'
			}
		},

		/**
		 * Validate swagger
		 */
		validateSwagger: {
			core: {
				ignoreWarnings: ['UNUSED_DEFINITION'],
				src: 'build/swagger-ui.json'
			}
		},

		/**
		 * Print API Doc Coverage
		 */
		printDocCoverage: {
			core: {
				ignoredPaths: [
					/\/api$/,
					/\/api\/schemas\/.*$/
				],
				failOnErrors: false,
				src: 'build/swagger-ui.json',
				endpoints: '../endpoints.json'
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
					'schemas/**/*.json'
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
		'lintSchemas',
		'createBuildDir',
		'flattenSwagger',
		'schemasToDefs',
		'validateSwagger'
	]);

	/**
	 * Build swagger-ui
	 * TODO: Put the distributed version of swagger-ui in the ../docs folder
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

	/**
	 * Generate docs and print coverage
	 */
	grunt.registerTask('coverage', ['build-ui', 'printDocCoverage']);
};


