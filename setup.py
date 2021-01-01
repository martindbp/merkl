import re
from setuptools import setup

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements


try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except:
    long_description = open('README.md', 'r').read()


install_reqs = parse_requirements('requirements.txt', session='hack')
reqs = [str(ir.requirement) for ir in install_reqs]

setup(
    name="merkl",
    packages=["merkl"],
    entry_points={"console_scripts": ['merkl = merkl:main']},
    version="0.1",
    description="MerkL",
    long_description=long_description,
    author="Martin Pettersson",
    author_email="me@martindbp.com",
    url="https://github.com/martindbp/merkl",
    install_requires=reqs
)
