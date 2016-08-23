## Steps for Contributing
1. Create a new branch off of up-to-date master
2. Make your changes.  If you are adding an endpoint or resource to the API,
follow "Adding an API Resource"
3. Make sure your code follows the "Code Standards" section of this document
4. Use "git rebase -i origin/master" to squash your commits into one or a few. Follow commit message guidelines.
5. Push your feature branch and make sure Travis CI passes
6. Submit a pull request to master and tag at least one reviewer

## Code Standards
### Docstrings
- Use [google style docstrings](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html).
- Add docstrings to all functions with a one-line description of its purpose
### Format
Use descriptive but not overly verbose variable names.  incoming_request instead of r.  Follow PEP8, exceptions by reviewer approval.  

## Adding an API Resource
### Design resource
1. Begin by writing a description of your API resource and it’s business functionality
2. Choose a name for your resource that describes what it does
3. Create a url from the name.  If your resource is a collection and the name is a noun, pluralize the name e.g. “uploads”

### Add RAML for API endpoints
1. Create a new resource file, called <resource_name>.raml   e.g. “uploads.raml”.  Create this file in the resources raml/resources directory
2. In api.raml, add a line with the URL of your resource and an include directive for your resource raml file.  E.g.   “/uploads: !include resources/uploads.raml”
3. In your resource file, define your resource.  Begin by adding a “description” property with the description you wrote in step 1
4. Add example properties for both request and response.  Examples should be stored in the examples/ directory, for example core/raml/examples/request/uploads.json 
5. Use http://jsonschema.net/#/  to generate jsonschema for both request and response body.  Edit jsonschema as necessary.  Before generating your schema, scroll down and uncheck “allow additional properties”  Schemas are stored in the “schemas” folder, for example core/raml/schemas/input/uploads.json   

### [Testing](https://github.com/scitran/core/blob/master/TESTING.md) - Click here

