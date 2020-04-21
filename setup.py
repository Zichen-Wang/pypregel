#!/usr/bin/env python
from setuptools import setup, find_packages


with open('README.md') as readme_file:
    README = readme_file.read()


install_requires = [
    'mpi4py',
    'wheel',
    'setuptools'
]


setup(
    name='pypregel',
    version='0.0.1',
    description='Graph framework',
    long_descriptioon='README',
    author='Zichen Wang; Hongxin Chu; Chufeng Gao',
    install_requires=install_requires,
)

