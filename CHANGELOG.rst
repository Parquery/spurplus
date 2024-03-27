2.3.5
=====
* Upgrade spur version to 0.3.23, so spurplus can install with paramiko 3.x.


2.3.4
=====
* Added support for Python 3.7 and 3.8, respectively.

2.3.3
=====
* Fixed live tests for Windows

2.3.2
=====
* Added separate named temporary files for Windows compatibility

2.3.1
=====
* Set the default of ``cwd`` argument to ``None`` instead of ``""``

2.3.0
=====
* Added initialization of reconnecting SFTP through paramiko

2.2.1
=====
* Added ``temppathlib`` to dependencies (was in dev dependencies before)

2.2.0
=====
* Updated to icontract 2.0.1
* Added ``listdir`` to ``spurplus.sftp``

2.1.2
=====
* Replaced mutable argument types with immutable ones where appropriate

2.1.1
=====
* Fixed ``sync_to_remote`` to delete children before parent directories

2.1.0
=====
* Added ``whoami`` to ``spurplus.SshShell``

2.0.0
=====
* Removed all functions from ``spurplus.sftp`` that ``spurplus`` does not use
* Added ``hostname`` and ``port`` property to SshShell
* Removed ``open()`` from ``spurplus.sftp`` and ``spurplus``. Get/put operations are now atomic when reconnecting
* Improved tests of ``spurplus.sftp``

1.2.5
=====
* Fixed problems with version.txt files
* tox runs with Python 3.5 and 3.6

1.2.4
=====
* Fixed ``mkdir`` failing on an existing directory with ``exit_ok=True``

1.2.3
=====
* Added version and license to the package

1.2.2
=====
* Fixed a bug related to local MD5 given as objects instead of hexdigests

1.2.1
=====
* Improved the documentation
* Added continuous integration

1.2.0
=====
* Added ``sync_to_remote``

1.1.2
=====
* Fixed identifier for py.typed in setup.py

1.1.1
=====
* Moved from bitbucket.com to github.com
* Added py.typed to comply with mypy

1.1.0
=====
* Added wrappers for stat, chown and chmod
* ``put`` keeps the permissions and ownership
* ``mkdir`` raises more specific permission errors
* ``sftp`` uses union of reconnecting sftp and paramiko sftp

1.0.1
=====
* Added computation of multiple md5 sums at once
* Fixed formatting of the environment variables for testing in the readme

1.0.0
=====
* Initial version
