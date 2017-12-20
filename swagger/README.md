# API Documentation

**NOTE**: The `schemas/` folder referenced in this README is currently located at `raml/schemas`.

API Documentation is written in [Swagger 2.0](https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md), using
separate JSON schemas in the `schemas/` folder for the object definitions.
Currently we're using 2.0 version of the spec due to a lack of support for OpenAPI 3.0 in code-generation tools.

### Documentation Generation

The `swagger/` subfolder is a **Node.js** project that provides services for generating the swagger documentation, as well as
linting the JSON schemas in the schemas folder.

### Getting Started

The fastest way to get started writing documentation is to run:
```bash
> cd swagger/
> npm install
> npm run watch
```
This will open the documentation in a browser, and watch for changes to the Swagger **yaml** files as well as the JSON schemas. 

The documentation should open automatically. If not, it should be hosted at: [http://localhost:9009/](http://localhost:9009).

# Writing Documentation

Please follow the [API Documentation Style Guide](STYLE_GUIDE.md) when updating documentation.

The following files and folders contain API documentation files:
* `index.yaml` - This is the index file for the swagger documentation. Other files are included or referenced from this file.
* `paths` - This folder contains documentation for API Endpoints, typically separated by functionality.
* `responses` - This folder contains definitions for common responses (used across endpoints)
* `templates` - This folder contains reusable template files for use in the swagger definitions. (See extensions)

In addition, JSON Schemas are stored in the `schemas/` folder, and example responses are stored in the `examples/` folder.
The schemas folder is layed out as follows:
* `definitions/` - This folder contains all of the reusable, named definitions. These definitions will automatically be added to the swagger definitions.
* `input/` - This folder contains additional schemas for api endpoint inputs. (i.e. **body** data on PUT/POST)
* `output/` - This folder contains additional schemas for api endpoint outputs. (i.e. **response body**)

### Swagger Extensions

In order to simplify swagger documentation and reduce repetition, includes and templates were added in the swagger flattening step.

#### $include
The `$include` extension is similar to `$ref` in that you can use it to include another file. The difference is that `$include` supports including an array of files, and will merge the contents of each file with its parent object, rather than replacing the contents of the parent object. This is mostly used in the swagger `index.yaml` file to include multiple files within the paths directive. 

#### $template
The `$template` extension allows the paramaterized re-use of endpoint definitions to reduce repetition. Template files specify what the required parameters are, and use [mustache.js](https://github.com/janl/mustache.js/) to compile the template data with parameters provided.

Parameters can be provided in one of two places:
* At the file-level under the `$template_parameters` section. This allows for reuse of template parameters within a file.
* At template invocation time, under the `arguments` section. This allows overriding or specifying additional parameters.

For example, if my yaml file looks like this:
```yaml
$template_arguments:
  resource: group
  tag: groups

/groups/{GroupId}/tags:
  $template: templates/tags.yaml
  arguments:
    tag: tags
	parameter: GroupId
``` 

Then the template arguments for the `templates/tags.yaml` template invocation will be:
* `resource`: group
* `tag`: tags
* `parameter`: GroupId

### JSON Schemas

JSON Schemas are used both for input validation and as the definitions for the Swagger documentation.

Current JSON Schema support level is: [JSON Schema draft-4](http://json-schema.org/specification-links.html#draft-4).

In order to comply with the Swagger schema specifications, JSON schemas go through a simplifying transpile step.

# Other Scripts

You can manually build the swagger docs by running:
```bash
> npm run build
```

You can run just the JSON Schema linter by running:
```bash
> npm run lint
```

Finally, you can run unit tests for the support code by running:
```bash
> npm run test
```

