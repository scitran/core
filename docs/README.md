### Updating api-console

1. Run a node container with the core root-dir bind-mounted:

   `$ docker run -itv $(pwd):/core -w /core node bash`


2. Install [api-console](https://github.com/mulesoft/api-console)'s recommended [cli build tool](https://github.com/mulesoft/api-console/blob/master/docs/build-tools.md):

   `$ npm install -g api-console-cli`


3. Generate "standalone" console into `./docs`:

   `$ api-console build https://raw.githubusercontent.com/scitran/core/master/raml/api.raml --output ./docs`

   Notes:
   * This can take a couple minutes
   * The `./docs` folder is recreated in the container
       - Owned by root - use `sudo chown -R $USER:$USER docs` on the host
       - Intermediate build artifacts can be removed: `$ rm -rf docs/bower_components docs/src`
       - This readme gets wiped - use `git checkout docs/README.md`


4. Enable branch selection via search & replace in `docs/index.html`:

   ```javascript
   document.querySelector("raml-js-parser").loadApi("https://raw.githubusercontent.com/scitran/core/master/raml/api.raml")
   ```

   ```javascript
   var url=new URL(location.href);var branch=url.searchParams.get("branch")||"master";document.querySelector("raml-js-parser").loadApi("https://raw.githubusercontent.com/scitran/core/"+branch+"/raml/api.raml")
   ```


5. Test it out:

   1. `$ cd docs && python2 -m SimpleHTTPServer`
   2. Visit http://localhost:8000
