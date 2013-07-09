#!/usr/bin/env python
#
# @author:  Gunnar Schaefer


import os
import sys
import site

site.addsitedir('/var/local/webapp2/lib/python2.7/site-packages')

sys.path.append('/var/local/nimsapi')

os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'

import nimsapi
application = nimsapi.nimsapi
