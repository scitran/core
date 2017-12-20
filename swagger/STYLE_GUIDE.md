# API Documentation Style Guidelines

## SWAGGER YAML

#### Tags
Tags are defined in `index.yaml`. Every tag used should have a description, and endpoints that belong to one
resource/collection should be (e.g. endpoints under /users/) should be grouped together by a tag.

#### Responses
Common responses are defined in `responses/index.yaml`, and should follow the naming convention: `<status code>:<name>`. 
(e.g. `404:resource-not-found`).

Responses can be referenced from any of the endpoint definitions: 
```yaml
responses:
  '404':
    $ref: '#/responses/404:resource-not-found'
```

#### Endpoints
Endpoints are located under the `paths/` folder, and in general should be grouped by resource/collection, or related endpoints.
In general, each method should include:
* **summary**: A one line description of the endpoint. This shows up in the main list of endpoint operations.
* **description**: Should provide additional details:
  * General summary of inputs and outputs
  * Any differing behaviors based on inputs
  * Summary of query parameters, if applicable
* **operationId**: Should be a unique name for the operation in lower [snake_case](https://en.wikipedia.org/wiki/Snake_case).
* **parameters**: 
  * **body**: JSON bodies **MUST** include a schema (reference to an `input` schema) 
  * **query**: Query parameters should be typed and include a description of what the parameter does.
* **responses**: 
  * JSON responses **MUST** include a schema and **SHOULD** include an example.
  * All responses *should* include a description.
  * `2xx` and `3xx` responses should be documented. 
  * `4xx` responses should be documented if they have special meaning beyond normal HTTP Status codes.

## JSON Schemas

JSON Schemas come in two flavors - `definitions` and `references`. Definitions are located in the `schemas/definitions` folder,
and references are located in the `schemas/input` and `schemas/output` folders.

As a general rule, there should be no anonymous object schemas. That is to say, any schema of a type `object` that includes `properties`, must be a named definition in the definitions folder.

#### Definitions
JSON schemas in the definitions folder **MUST NOT** have top-level schema definitions. 
All schemas must be defined under the `definitions` property, and definition names **MUST** be lower kebab-case.

#### References
Reference schemas (in the `input/` and `output/` folders) must have a single top-level definition that references or includes (e.g. via `allOf`) a named definition from the `definitions/` folder.

Reference schemas should include an example definition, either inline in the schema (preferable) or with the same name in the `examples/input` or `examples/output` folder.

