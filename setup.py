from setuptools import setup, find_packages

requirements = open('requirements.txt').readlines()
install_requires = [r for r in requirements if not r.startswith('git+')]
dependency_links = [r for r in requirements if r.startswith('git+')]
tests_require = open('tests/requirements.txt').readlines()

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
