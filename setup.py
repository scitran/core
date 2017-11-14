from setuptools import setup, find_packages

install_requires = [
    'django >= 1.11.5',
    'elasticsearch==5.3.0',
    'enum34==1.1.6',
    'jsonschema==2.6.0',
    'Markdown==2.6.5',
    'pymongo==3.2',
    'python-dateutil==2.4.2',
    'pytz==2015.7',
    'requests==2.9.1',
    'rfc3987==1.3.4',
    'strict-rfc3339==0.7',
    'unicodecsv==0.9.0',
    'webapp2==2.5.2',
    'WebOb==1.5.1',
    'gears',
]

dependency_links = [
    'git+https://github.com/flywheel-io/gears.git@v0.1.4#egg=gears',
]

setup(
    name = 'example',
    version = '0.0.1',
    description = '',
    author = '',
    author_email = '',
    url = '',
    license = '',
    packages = find_packages(),
    dependency_links = dependency_links,
    install_requires = install_requires,
)
