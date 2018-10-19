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
