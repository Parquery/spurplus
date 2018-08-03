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