#!/usr/bin/env python
#

import os.path

import setuptools

from sprockets.clients import dynamodb


def read_requirements(filename):
    requirements = []
    try:
        with open(os.path.join('requires', filename)) as req_file:
            for line in req_file:
                if '#' in line:
                    line = line[:line.index('#')]
                line = line.strip()
                if line.startswith('-r'):
                    requirements.extend(read_requirements(line[2:].strip()))
                elif line:
                    requirements.append(line)
    except IOError:
        pass
    return requirements


setuptools.setup(
    name='sprockets.clients.dynamodb',
    version=dynamodb.__version__,
    description='DynamoDB connector',
    author='AWeber Communications',
    author_email='api@aweber.com',
    url='https://github.com/sprockets/sprockets.clients.dynamodb',
    install_requires=read_requirements('installation.txt'),
    license='BSD',
    namespace_packages=['sprockets', 'sprockets.clients'],
    packages=setuptools.find_packages(exclude=['examples']),
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    test_suite='nose.collector',
    tests_require=read_requirements('testing.txt'),
    zip_safe=True,
)
