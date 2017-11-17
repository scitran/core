from setuptools import setup, find_packages

install_requires = open('requirements.txt').readlines()
tests_require = open('tests/requirements.txt').readlines()
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
    tests_require = tests_require,
)
