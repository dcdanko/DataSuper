"""DataSuper - Organization for scientific projects."""

import os
import sys

from setuptools import setup, find_packages
from setuptools.command.install import install


version = {}
VERSION_PATH = os.path.dirname(os.path.realpath(__file__))
with open('{0}/datasuper/version.py'.format(VERSION_PATH)) as fp:
    exec(fp.read(), version)


dependencies = [
    'click~=6.7',
    'py-archy~=1.0.1',
    'PyYAML~=3.12',
    'tinydb~=3.5.0',
    'ujson~=1.35',
    'yaml_backed_structs~=0.9.0',
]


def readme():
    """Print long description."""
    with open('README.rst') as readme_file:
        return readme_file.read()


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version."""

    description = 'Verify that the git tag matches our version.'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')

        if tag != 'v{0}'.format(version['__version__']):
            info = 'Git tag: {0} does not match the version of this app: {1}'
            info = info.format(tag, version['__version__'])
            sys.exit(info)


setup(
    name="DataSuper",
    version=version['__version__'],
    url="https://github.com/dcdanko/DataSuper",

    author="David C. Danko",
    author_email="dcdanko@gmail.com",

    description="Organization for scientific projects.",
    long_description=readme(),

    packages=find_packages(exclude=['tests']),

    install_requires=dependencies,

    entry_points={
        'console_scripts': [
            'datasuper=datasuper.cli:main'
        ]
    },

    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    cmdclass={
        'verify': VerifyVersionCommand,
    },
)
