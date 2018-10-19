"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""
import os

from setuptools import setup, find_packages

# pylint: disable=redefined-builtin

here = os.path.abspath(os.path.dirname(__file__))  # pylint: disable=invalid-name

with open(os.path.join(here, 'README.rst'), encoding='utf-8') as fid:
    long_description = fid.read().strip()  # pylint: disable=invalid-name

setup(
    name='spurplus',
    version='2.1.1',
    description='Manage remote machines and file operations over SSH.',
    long_description=long_description,
    url='http://github.com/Parquery/spurplus',
    author='Marko Ristin',
    author_email='marko.ristin@gmail.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    license='License :: OSI Approved :: MIT License',
    keywords='ssh sftp spur paramiko execute remote commands modify files',
    packages=find_packages(exclude=['tests']),
    install_requires=['spur==0.3.20', 'typing_extensions>=3.6.2.1', 'icontract>=1.5.7,<2'],
    extras_require={
        'dev': [
            'mypy==0.620', 'pylint==1.8.2', 'yapf==0.20.2', 'tox>=3.0.0', 'temppathlib>=1.0.3,<2', 'coverage>=4.5.1,<5',
            'pydocstyle>=2.1.1,<3'
        ]
    },
    py_modules=['spurplus', 'spurplus.sftp'],
    include_package_data=True,
    package_data={
        "spurplus": ["py.typed"],
        '': ['LICENSE.txt', 'README.rst']
    })
