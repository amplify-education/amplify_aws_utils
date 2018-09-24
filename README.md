[![Build Status](https://travis-ci.org/amplify-education/amplify_aws_utils.svg?branch=master)](https://travis-ci.org/amplify-education/amplify_aws_utils)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/amplify-education/amplify_aws_utils/master/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/nine.svg)](https://pypi.org/project/amplify-aws-utils/)
[![Python Versions](https://img.shields.io/badge/Python-3.6-brightgreen.svg,https://img.shields.io/badge/Python-2.7-brightgreen.svg)](https://pypi.python.org/pypi/amplify_aws_utils)
[![Downloads](https://img.shields.io/badge/dynamic/json.svg?label=downloads&url=https://img.shields.io/badge/dynamic/json.svg?label=Downloads&url=https%3A%2F%2Fpypistats.org%2Fapi%2Fpackages%2Famplify-aws-utils%2Frecent&query=data.last_month&colorB=brightgreen&suffix=%2FMonth)](https://pypistats.org/api/packages/amplify-aws-utils/recent)

# amplify_aws_utils

Utility functions for working with AWS resources though Boto3 with less hiccups.

About Amplify
=============

Amplify builds innovative and compelling digital educational products that empower teachers and students across the country. We have a long history as the leading innovator in K-12 education - and have been described as the best tech company in education and the best education company in tech. While others try to shrink the learning experience into the technology, we use technology to expand what is possible in real classrooms with real students and teachers.
  

# Getting Started
## Prerequisites
amplify_aws_utils requires the following to be installed:
```
python >= 2.7
python >=3.6
```

For development, `tox>=2.9.1` is recommended.

## Building
Python package can be built as follows:

`python setup.py sdist`

This creates a package in `dist` directory.

## Running Tests
As mentioned above, amplify_aws_utils uses tox, so running `tox` will automatically execute linters as well as the unit tests. You can also run functional and integration tests by using the -e argument.

For example, `tox -e lint,py27-unit,py27-integration` will run the linters, and then the unit and integration tests in python 2.7.

To see all the available options, run `tox -l`.

## Deployment
Deployment is done with Travis.

Package is built as described above, and is uploaded to PyPI repo using `devpi-client`

## Installation
This package can be installed using `pip`

`pip install amplify_aws_utils`

## Usage
Functions provided by this package can be imported after package has been installed.

Example:

`from amplify_aws_utils.resource_helper import throttled_call`
