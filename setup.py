import re
from setuptools import setup, find_packages

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements


long_description = open('README.md', 'r').read()

install_reqs = parse_requirements('requirements.txt', session=False)
reqs = list(install_reqs)
try:
    reqs = [str(ir.req) for ir in reqs]
except:
    reqs = [str(ir.requirement) for ir in reqs]

setup(
    name="merkl",
    packages=find_packages(),
    entry_points={"console_scripts": ['merkl = merkl:main']},
    version="0.4",
    description="MerkL",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Martin Pettersson",
    author_email="me@martindbp.com",
    url="https://github.com/martindbp/merkl",
    install_requires=reqs
)
