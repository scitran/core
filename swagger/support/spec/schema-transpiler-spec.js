'use strict';

var LOG_TO_CONSOLE = false;

var SchemaTranspiler = require('../schema-transpiler');

describe('SchemaTranspiler', function() {
	it('should exist', function() {
		expect(SchemaTranspiler).toBeDefined();
	});
});

describe('SchemaTranspiler draft4ToOpenApi2', function() {
	var options = {};
	if( !LOG_TO_CONSOLE ) {
		options.log = function() {};
	}

	var transpiler = new SchemaTranspiler(options);

	it('should drop $schema attribute', function() {
		var schema = {
			$schema: 'http://json-schema.org/draft-04/schema#',
			type: 'string'
		};

		var result = transpiler.toOpenApi2(schema);
		expect(result).toEqual({type: 'string'});
	});

	it('should replace type array with string 1', function() {
		var schema = {
			type: ['null', 'string']
		};

		var result = transpiler.toOpenApi2(schema);
		expect(result).toEqual({type: 'string'});
	});

	it('should replace type array with string 2', function() {
		var schema = {
			type: ['number', 'string']
		};

		var result = transpiler.toOpenApi2(schema);
		expect(result).toEqual({type: 'number'});
	});

	it('should merge examples with $refS', function() {
		var defs = {
				Foo: {
					type: 'object',
					properties: {
						updated: {type: 'boolean'}
					}
				}
			},
			schema = {
				$ref: '#/definitions/Foo',
				example: { updated: false }
			};
					
		var result = transpiler.toOpenApi2(schema, defs);
		expect(result).toEqual({
			type: 'object',
			properties: {
				updated: {type: 'boolean'}
			},
			example: {
				updated: false
			}
		});
	});

	it('should flatten allOf with one element', function() {
		var schema = {
			allOf: [{$ref:'#/definitions/Foo'}]
		};

		var result = transpiler.toOpenApi2(schema);
		expect(result).toEqual({$ref:'#/definitions/Foo'});
	});

	it('should merge properties for anyOf', function() {
		var defs = {
				Foo: {
					type: 'object',
					properties: {
						updated: {type: 'boolean'}
					},
					required: ['updated']
				}
			},
			schema = {
				anyOf: [
					{
						type: 'object',
						properties: {
							modified: { type: 'number' }
						}
					},
					{$ref: '#/definitions/Foo'}
				]
			};

		var result = transpiler.toOpenApi2(schema, defs);
		expect(result).toEqual({
			type: 'object',
			properties: {
				updated: {type: 'boolean'},
				modified: {type: 'number'}
			}
		});
	});

	it('should remove "not" elements', function() {
		var schema = {
			not: { type: 'boolean' }
		};

		var result = transpiler.toOpenApi2(schema);
		expect(result).toEqual({});
	});

	it('should flatten array elements', function() {
		var defs = {
				Foo: {
					type: 'object',
					properties: {
						updated: {type: 'boolean'}
					},
					required: ['updated']
				}
			},
			schema = {
				type: 'array',
				items: {
					allOf: [{$ref:'#/definitions/Foo'}]
				}
			};
		
		var result = transpiler.toOpenApi2(schema, defs);
		expect(result).toEqual({
			type: 'array',
			items: {$ref:'#/definitions/Foo'}
		});
	});

	it('should recurse into properties', function() {
		var schema = {
			type: 'object',
			properties: {
				bar: { type: ['string', 'null'] },
				foo: { allOf: [{$ref: '#/definitions/Foo'}] }
			}
		};

		var result = transpiler.toOpenApi2(schema);
		expect(result).toEqual({
			type: 'object',
			properties: {
				bar: {type: 'string'},
				foo: {$ref: '#/definitions/Foo'}
			}
		});
	});

});

