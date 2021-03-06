#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

# To update the package version number, edit ticclat/__version__.py
version = {}
with open(os.path.join(here, 'ticclat', '__version__.py')) as f:
    exec(f.read(), version)

with open('README.rst') as readme_file:
    readme = readme_file.read()

setup(
    name="ticclat",
    version=version["__version__"],
    description="TICCLAT is a tool for text-induced corpus correction and lexical assessment.",
    long_description=readme + "\n\n",
    author="Janneke van der Zwaan, Patrick Bos, Tom Klaver",
    author_email='j.vanderzwaan@esciencecenter.nl',
    url='https://github.com/ticclat/ticclat',
    packages=[
        'ticclat',
    ],
    package_dir={'ticclat':
                 'ticclat'},
    include_package_data=True,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='ticclat',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
    install_requires=[
        "pandas",
        "sh",
        "SQLAlchemy",
        "tqdm",
        "mysqlclient",
        "sqlalchemy_utils",
        "Flask-SQLAlchemy-Session",
        "sklearn",
        "python-dotenv",
        "flask",
        "click",
        "gunicorn",
        "bokeh"
    ],  # FIXME: add your package's dependencies to this list
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        # dependencies for `python setup.py build_sphinx`
        'sphinx',
        'sphinx_rtd_theme',
        'recommonmark'
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'pycodestyle',
        'pyfakefs',
        'pytest-datafiles',
        'testcontainers[mysql]',
        'cryptography',
        'pylint>=2.4',
        'nltk',
    ],
    extras_require={
        'dev': ['prospector[with_pyroma]', 'yapf', 'isort'],
        'benchmark': ['pyfakefs'],
    }
)
