## Run the tests

### Ubuntu
Run automated tests:
```
# Follow installation instructions in README first
. /runtime/bin/activate # Or wherever your scitran virtualenv is
./test/bin/setup-integration-tests-ubuntu.sh
./test/bin/run-tests-ubuntu.sh
```
All tests are executed by default. Subsets can be run using the filtering options:

* To run linting, use `--lint` (`-l`)
* To run unit tests, use `--unit` (`-u`)
* To run integration tests, use `--integ` (`-i`)
* To pass any arguments to `py.test`, use `-- PYTEST_ARGS`

See [py.test usage](https://docs.pytest.org/en/latest/usage.html) for more.

### Docker
Build scitran-core image and run automated tests in a docker container:
```
./tests/bin/run-tests-docker.sh
```
* To skip building the image, use `--no-build` (`-B`)
* To pass any arguments to `run-tests-ubuntu.sh`, use `-- TEST_ARGS`


#### Example
Without rebuilding the image, run only integration tests matching `foo`, use the highest verbosity level for test output and jump into a python debugger session in case an assertion fails:
```
./tests/bin/run-tests-docker.sh -B -- -i -- -k foo -vvv --pdb
```

**NOTE:** The mongodb version is pinned via the `MONGO_VERSION` variable in `tests/bin/run-tests-docker.sh`.
