[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9f6400386de74fe0b86acd6a081f3302)](https://www.codacy.com/app/amplify-education/amplify_aws_utils?utm_source=github.com&utm_medium=referral&utm_content=amplify-education/amplify_aws_utils&utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/9f6400386de74fe0b86acd6a081f3302)](https://www.codacy.com/app/amplify-education/amplify_aws_utils?utm_source=github.com&utm_medium=referral&utm_content=amplify-education/amplify_aws_utils&utm_campaign=Badge_Coverage)
[![Build Status](https://travis-ci.org/amplify-education/amplify_aws_utils.svg?branch=master)](https://travis-ci.org/amplify-education/amplify_aws_utils)
[![PyPI](https://img.shields.io/pypi/v/amplify-aws-utils.svg)](https://pypi.org/project/amplify-aws-utils/)
[![Python Versions](https://img.shields.io/pypi/pyversions/amplify-aws-utils.svg)](https://pypi.python.org/pypi/amplify-aws-utils)
[![Downloads](https://img.shields.io/pypi/dm/amplify_aws_utils.svg)](https://pypistats.org/api/packages/amplify-aws-utils/recent)

# amplify_aws_utils

Utility functions for working with AWS resources though Boto3 with less hiccups.

## About Amplify

Amplify builds innovative and compelling digital educational products that empower teachers and students across the
country. We have a long history as the leading innovator in K-12 education - and have been described as the best tech
company in education and the best education company in tech. While others try to shrink the learning experience into the
technology, we use technology to expand what is possible in real classrooms with real students and teachers.

## Getting Started

### Prerequisites

amplify_aws_utils requires the following to be installed:

```text
python >= 3.8
```

### Installation

This package can be installed using `pip`

`pip install amplify_aws_utils`

### Building From Source

For development, `tox>=2.9.1` is recommended.

Python package can be built as follows:

`python setup.py sdist`

This creates a package in `dist` directory.

### Running Tests

`amplify_aws_utils` uses `tox`. You will need to install tox with `pip install tox`.
Running `tox` will automatically execute linters as well as the unit tests. You can also run them individually with
the -e argument.

For example, `tox -e lint,py38-unit` will run the linters, and then the unit tests in python 3.8

To see all the available options, run `tox -l`.

### Deployment

Deployment is done with Travis.

Package is built as described above, and is uploaded to PyPI repo using `devpi-client`

### Usage

Functions provided by this package can be imported after package has been installed.

Example:

`from amplify_aws_utils.resource_helper import throttled_call`
