[![Build Status](https://travis-ci.org/scitran/core.svg?branch=master)](https://travis-ci.org/scitran/core)
[![Coverage Status](https://coveralls.io/repos/github/scitran/core/badge.svg?branch=master)](https://coveralls.io/github/scitran/core?branch=master)
[![Code Climate](https://codeclimate.com/github/scitran/core/badges/gpa.svg)](https://codeclimate.com/github/scitran/core)

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
