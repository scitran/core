'use strict';

var SchemaLinter = require('../schema-lint');


describe('SchemaLinter', function() {
	it('should exist', function() {
		expect(SchemaLinter).toBeDefined();	
	});

	it('should honor x-lint: false', function() {
		var linter = new SchemaLinter();
		var schema = {
			'x-lint': false,
			definitions: {
				Foo: {
					type: 'object',
					properties: {
						bar: {
							type: 'object',
							properties: {
								qaz: {type: 'integer'}
							}
						}
					}
				}
			}
		};

		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(0);
	});
});

describe('SchemaLinter anonymousObjects linter', function() {
	var linter = new SchemaLinter();

	it('should warn on anonymous objects in definitions', function() {
		var schema = {
			definitions: {
				'foo': {
					type: 'object',
					properties: {
						bar: {
							type: 'object',
							properties: {
								x: {type: 'integer'},
								y: {type: 'integer'}
							}
						}
					}
				}
			}
		};

		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(1);
		expect(errors[0].message).toBe('Anonymous object defined at definitions/foo/properties/bar');
	});

	it('should warn on anonymous objects in top level schemas', function() {
		var schema = {
			type: 'array',
			items: {
				type: 'object',
				properties: {
					x: {type: 'integer'},
					y: {type: 'integer'}
				}
			}
		};

		var errors = linter.lintSchema('references', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(1);
		expect(errors[0].message).toBe('Anonymous object defined at items');
	});
});

describe('SchemaLinter topLevelDefinition linter', function() {
	var linter = new SchemaLinter();

	it('should warn on top level definition', function() {
		var schema = {
			type: 'object',
			properties: {
				foo: {type: 'string'}
			}
		};
		
		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(1);
		expect(errors[0].message).toBe('Top level definitions are not allowed');
	});

	it('should not warn on top level arrays', function() {
		var schema = {
			type: 'array',
			items: {$ref:'#/definitions/foo'}
		};
		
		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(0);
	});

	it('should not warn on top level allof', function() {
		var schema = {
			type: 'object',
			allOf: [{$ref:'#/definitions/foo'}]
		};
		
		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(0);
	});

	it('should not warn on top level references', function() {
		var schema = {'$ref': '#/definitions/foo'};
		
		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(0);
	});
});

describe('SchemaLinter duplicateName linter', function() {
	var linter = new SchemaLinter();
	
	it('should warn on duplicate names', function() {
		var schema1 = {
				definitions: {
					foo: { type: 'object' }
				}
			},
			schema2 = {
				definitions: {
					foo: { type: 'object' },
					bar: { type: 'string' }
				}
			};

		var errors = linter.lintSchema('definitions', schema1);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(0);

		errors = linter.lintSchema('definitions', schema2);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(1);
		expect(errors[0].message).toBe('Duplicate definition found: "foo"');
	});
});

describe('SchemaLinter definitionNameStyle linter', function() {
	var linter = new SchemaLinter();

	it('should allow valid names', function() {
		var schema = {
			definitions: {
				foo: {type: 'object'},
				'foo-bar': {type: 'object'},
				'foo-bar13': {type: 'object'},
				'a': {type: 'object'},
				'a-b': {type: 'object'}
			}
		};
		
		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(0);
	});

	it('should not allow invalid names', function() {
		var schema = {
			definitions: {
				'-abc': {type: 'object'},
				'fooBar': {type: 'object'},
				'FooBar': {type: 'object'},
				'foo-BAR': {type: 'object'},
				'foo-': {type: 'object'},
				'foo-1': {type: 'object'}
			}
		};
		
		var errors = linter.lintSchema('definitions', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(6);

		var found = false;
		for( var i = 0; i < errors.length; i++ ) {
			if( errors[i].toString() === 'Invalid definition name: "-abc"' ) {
				found = true;
				break;
			}
		}

		expect(found).toBe(true);
	});


});

describe('SchemaLinter definintionsInReferences linter', function() {
	var linter = new SchemaLinter();

	it('should not allow definitions', function() {
		var schema = {
			type: 'object',
			properties: {
				foo: { type: 'string' }
			},
			definitions: {
				foo: {type: 'string'},
				'foo-bar': {type: 'string'},
				'foo-bar13': {type: 'string'},
				'a': {type: 'number'},
				'a-b': {type: 'boolean'}
			}
		};
		
		var errors = linter.lintSchema('references', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(1);
		expect(errors[0].message).toEqual('Definitions not allowed in references');
	});

	it('should not allow object declarations', function() {
		var schema = {
			type: 'object',
			properties: {
				foo: { type: 'string' }
			}
		};

		var errors = linter.lintSchema('references', schema);
		expect(errors).toBeDefined();
		expect(errors.length).toEqual(1);
		expect(errors[0].message).toEqual('Definitions not allowed in references');
	});

});

