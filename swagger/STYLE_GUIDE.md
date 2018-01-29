# API Documentation Style Guidelines

## SWAGGER YAML

**NOTE:** Since markdown is supported in descriptions, description fields should use the [literal style](http://www.yaml.org/spec/1.2/spec.html#id2795688) indicator.

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


### Field Ordering
To promote readability, fields should be ordered as follows: (see Example Endpoint below)

Under an endpoint, parameters (if there are any) should be the first entry, listed in order, with fields in the order of: `name, in, type, required, description, schema` followed by
methods in the order of `get, put, post, delete`

Each method should have fields in the order: `summary, description, operationId, tags, parameters, responses`

Responses should be ordered by numeric code in ascending order, with fields ordered: `description`, `schema`, `example`

## JSON Schemas

JSON Schemas come in two flavors - `definitions` and `references`. Definitions are located in the `schemas/definitions` folder,
and references are located in the `schemas/input` and `schemas/output` folders.

As a general rule, there should be no anonymous object schemas. That is to say, any schema of a type `object` that includes `properties`, must be a named definition in the definitions folder.

#### Definitions
JSON schemas in the definitions folder **MUST NOT** have top-level schema definitions. 

All schemas must be defined under the `definitions` property, and definition names **MUST** be lower kebab-case.

All schemas (including simple types) **SHOULD** include a description property. (See Example)

#### References
Reference schemas (in the `input/` and `output/` folders) must have a single top-level definition that references or includes (e.g. via `allOf`) a named definition from the `definitions/` folder.

Reference schemas should include an example definition, either inline in the schema (preferable) or with the same name in the `examples/input` or `examples/output` folder.

# Examples

## Example Endpoint
```yaml
/groups/{GroupId}:
  parameters:
    - name: GroupId
      in: path
      type: string
      required: true
  get:
    summary: Get group info
    operationId: get_group
    tags:
    - groups
    responses:
      '200':
        description: ''
        schema:
          $ref: schemas/output/group.json
        examples:
          response:
            $ref: examples/output/group.json
  put:
    summary: Update group
    description: |
      This method supports patch semantics. 
      Only fields that are defined in the update body will be updated.
    operationId: modify_group
    tags:
    - groups
    parameters:
      - name: body
        in: body
        required: true
        schema:
          $ref: schemas/input/group-update.json
    responses:
      '400':
        $ref: '#/responses/400:invalid-body-json'
  delete:
    summary: Delete a group
    operationId: delete_group
    tags:
    - groups
    responses:
      '200':
        $ref: '#/responses/200:deleted-with-count'
```

## Example JSON Schema

```javascript
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "definitions":{
        "label": {
            "maxLength": 64,
            "minLength": 1,
            "pattern": "^[0-9A-Za-z][0-9A-Za-z .@_-]{0,30}[0-9A-Za-z]$",
            "title": "label",
            "type": "string",
            "description": "A unique label for the group"
        },
        "group-input":{
          "type": "object",
		  "description": "Used to create or update a group",
          "properties": {
            "_id": { 
				"type": "string",
				"description": "A universally unique object-id"
			},
            "label": {"$ref": "#/definitions/label"}
          },
          "additionalProperties": false
        }
	}
}
```

