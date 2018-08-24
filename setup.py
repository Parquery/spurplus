"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""
import os

from setuptools import setup, find_packages

# pylint: disable=redefined-builtin

here = os.path.abspath(os.path.dirname(__file__))  # pylint: disable=invalid-name

with open(os.path.abspath('version.txt'), encoding='utf-8') as fid:
    version = fid.read().strip()  # pylint: disable=invalid-name

with open(os.path.join(here, 'README.rst'), encoding='utf-8') as fid:
    long_description = fid.read().strip()  # pylint: disable=invalid-name

setup(
    name='spurplus',
    version=version,
    description='Manage remote machines and file operations over SSH.',
    long_description=long_description,
    url='http://github.com/Parquery/spurplus',
    author='Marko Ristin',
    author_email='marko@parquery.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='ssh sftp spur paramiko execute remote commands modify files',
    packages=find_packages(exclude=['tests']),
    install_requires=['spur==0.3.20', 'typing_extensions>=3.6.2.1'],
    extras_require={
        'dev': [
            'mypy==0.620', 'pylint==1.8.2', 'yapf==0.20.2', 'tox>=3.0.0', 'temppathlib>=1.0.3,<2', 'coverage>=4.5.1,<5',
            'pydocstyle>=2.1.1,<3'
        ],
        'test': ['tox>=3.0.0', 'temppathlib>=1.0.3,<2', 'coverage>=4.5.1,<5']
    },
    py_modules=['spurplus'],
    include_package_data=True,
    package_data={"spurplus": ["py.typed"],
                  '': ['version.txt', 'LICENSE.txt']})
