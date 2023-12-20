from __future__ import print_function

"""setup.py controls the build, testing, and distribution of the egg"""

from setuptools import setup, find_packages
import re
import os.path


VERSION_REGEX = re.compile(
    r"""
    ^__version__\s=\s
    ['"](?P<version>.*?)['"]
""",
    re.MULTILINE | re.VERBOSE,
)

VERSION_FILE = os.path.join("amplify_aws_utils", "version.py")


def get_long_description():
    """Reads the long description from the README"""
    this_directory = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(this_directory, "README.md"), encoding="utf-8") as file:
        return file.read()


def get_version():
    """Reads the version from the package"""
    with open(VERSION_FILE) as handle:
        lines = handle.read()
        result = VERSION_REGEX.search(lines)
        if result:
            return result.groupdict()["version"]
        else:
            raise ValueError("Unable to determine __version__")


def get_requirements():
    """Reads the installation requirements from requirements.txt"""
    with open("requirements.txt") as reqfile:
        return [
            line
            for line in reqfile.read().split("\n")
            if not line.startswith(("#", "-"))
        ]


setup(
    name="amplify_aws_utils",
    python_requires=">=3.8.0",
    version=get_version(),
    description="Utility functions for working with AWS resources",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords="",
    author="Amplify Education",
    author_email="astrotools@amplify.com",
    url="https://github.com/amplify-education/amplify_aws_utils/",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=get_requirements(),
    test_suite="nose.collector",
)
