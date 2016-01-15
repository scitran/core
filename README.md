[![Build Status](https://travis-ci.org/scitran/api.svg?branch=travis)](https://travis-ci.org/scitran/api)
[![Coverage Status](https://coveralls.io/repos/scitran/api/badge.svg?service=github)](https://coveralls.io/github/scitran/api)
[![Code Climate](https://codeclimate.com/github/scitran/api/badges/gpa.svg)](https://codeclimate.com/github/scitran/api)

# SciTran – Scientific Data Management


### Usage
```
./bin/run.sh [config file]
```
or
```
PYTHONPATH=. uwsgi --http :8443 --virtualenv ./runtime --master --wsgi-file bin/api.wsgi
```


### Maintenance

#### Upgrading Python Packages

List outdated packages
```
pip list --local --outdated
```

Then review and decide what upgrades to make, if any.<br>
Changes to `requirements.txt` should always be a pull request.
