[![Build Status](https://travis-ci.org/scitran/core.svg?branch=master)](https://travis-ci.org/scitran/core)
[![Coverage Status](https://coveralls.io/repos/github/scitran/core/badge.svg?branch=master)](https://coveralls.io/github/scitran/core?branch=master)
[![Code Climate](https://codeclimate.com/github/scitran/core/badges/gpa.svg)](https://codeclimate.com/github/scitran/core)

[//]: # (Looking for a markdown editor?  Try Dillinger.io)

# SciTran – Scientific Data Management

### Overview

HTTP API and core components for the SciTran SDM system.  To learn more about the Scientific Transparency Project as a whole and other related software, see scitran.github.io

### [Documentation](https://scitran.github.io/core)

### [Contributing](https://github.com/scitran/core/blob/master/CONTRIBUTING.md)

### [Testing](https://github.com/scitran/core/blob/master/TESTING.md)

### [License](https://github.com/scitran/core/blob/master/LICENSE)
  

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

Then review and decide what upgrades to make, if any.  
Changes to `requirements.txt` should always be a pull request.

