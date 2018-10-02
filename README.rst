Spur+
=====

.. image:: https://api.travis-ci.com/Parquery/spurplus.svg?branch=master
    :target: https://api.travis-ci.com/Parquery/spurplus.svg?branch=master
    :alt: Build Status

.. image:: https://coveralls.io/repos/github/Parquery/spurplus/badge.svg?branch=master
    :target: https://coveralls.io/github/Parquery/spurplus?branch=master
    :alt: Coverage

.. image:: https://readthedocs.org/projects/spurplus/badge/?version=latest
    :target: https://spurplus.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
    
.. image:: https://badge.fury.io/py/spurplus.svg
    :target: https://pypi.org/project/spurplus/
    :alt: PyPi

.. image:: https://img.shields.io/pypi/pyversions/spurplus.svg
    :alt: PyPI - Python Version

Spur+ is a library to manage remote machines and perform file operations over SSH.

It builds on top of Spur_ and Paramiko_ libraries. While we already find that Spur_ and Paramiko_ provide most of the
functionality out-of-the-box, we missed certain features:

- typing. Since spur supports both Python 2 and 3, it does not provide any type annotations which makes it harder to use
  with type checkers such as mypy.

- pathlib.Path support. We find it easier to manipulate paths using pathlib.Path instead of plain strings. spur+
  provides support for both.

- a function for creating directories. spur relies on sftp client. While it is fairly straightforward to get an sftp
  client from ``spur.SshShell`` and create a directory, we think that it merits a wrapper function akin to
  ``pathlib.Path.mkdir()`` provided how often this functionality is needed.

- reading/writing text and binary data in one go. Similarly to creating directories, ``spur.SshShell.open()`` already
  provides all the functionality you need to read/write files. However, we found the usage code to be more readable when
  written in one line and no extra variables for file descriptors are introduced.

- a function for putting and getting files to/from the remote host, respectively.

- a function to sync a local directory to a remote directory (similar to ``rsync``).

- a function for computing MD5 checksums.

- a function to check if a file exists.

- a more elaborate context manager for a temporary directory which allows for specifying prefix, suffix and
  base directory and gives you a pathlib.Path. In contrast, ``spur.temporary_directory()`` gives you only a string with
  no knobs.

- an initializer function to repeatedly re-connect on connection failure. We found this function particularly important
  when you spin a virtual instance in the cloud and need to wait for it to initialize.

- a wrapper around paramiko's SFTP client (``spurplus.sftp.ReconnectingSFTP``) to automatically reconnect if the SFTP
  client experienced a connection failure. While original ``spur.SshShell.open()`` creates a new SFTP client on every
  call in order to prevent issues with time-outs, `spurplus.SshShell` is able to re-use the SFTP client over multiple
  calls via ``spurplus.sftp.ReconnectingSFTP``.

  This can lead up to 10x speed-up (see the benchmark in ``tests/live_test.py``).

.. _Spur: https://github.com/mwilliamson/spur.py
.. _Paramiko: https://github.com/paramiko/paramiko

Usage
=====
.. code-block:: python

    import pathlib

    import spurplus

    # Re-try on connection failure; sftp client and the underlying spur SshShell
    # are automatically closed when the shell is closed.
    with spurplus.connect_with_retries(
            hostname='some-machine.example.com', username='devop') as shell:
        p = pathlib.Path('/some/directory')

        # Create a directory
        shell.mkdir(remote_path=p, parents=True, exist_ok=True)

        # Write a file
        shell.write_text(remote_path=p/'some-file', text='hello world!')

        # Read from a file
        text = shell.read_text(remote_path=p/'some-file')

        # Change the permissions
        shell.chmod(remote_path=p/'some-file', mode=0o444)

        # Sync a local directory to a remote.
        # Only differing files are uploaded,
        # files missing locally are deleted before the transfer and
        # the permissions are mirrored from the local.
        sync_to_remote(
            local_path="/some/local/directory",
            remote_path="/some/remote/directory",
            delete=spurplus.Delete.BEFORE,
            preserve_permissions = True)

        # Stat the file
        print("The stat of {}: {}".format(p/'some-file', shell.stat(p/'some-file')))

        # Use a wrapped SFTP client
        sftp = shell.as_sftp()
        # Do something with the SFTP
        for attr in sftp.listdir_attr(path=p.as_posix()):
            do_something(attr.filename, attr.st_size)

Documentation
=============
The documentation is available on `readthedocs <https://spurplus.readthedocs.io/en/latest/>`_.

Installation
============

* Create a virtual environment:

.. code-block:: bash

    python3 -m venv venv3

* Activate it:

.. code-block:: bash

    source venv3/bin/activate

* Install spur+ with pip:

.. code-block:: bash

    pip3 install spurplus

Development
===========

* Check out the repository.

* In the repository root, create the virtual environment:

.. code-block:: bash

    python3 -m venv venv3

* Activate the virtual environment:

.. code-block:: bash

    source venv3/bin/activate

* Install the development dependencies:

.. code-block:: bash

    pip3 install -e .[dev]

* There are live tests for which you need to have a running SSH server. The parameters of the tests
  are passed via environment variables:

    - ``TEST_SSH_HOSTNAME`` (host name of the SSH server, defaults to "127.0.0.1"),
    - ``TEST_SSH_PORT`` (optional, defaults to 22),
    - ``TEST_SSH_USERNAME`` (optional, uses paramiko's default),
    - ``TEST_SSH_PASSWORD`` (optional, uses private key file if not specified) and
    - ``TEST_SSH_PRIVATE_KEY_FILE`` (optional, looks for private key in expected places if not specified).

We use tox for testing and packaging the distribution. Assuming that the above-mentioned environment variables has
been set, the virutal environment has been activated and the development dependencies have been installed, run:

.. code-block:: bash

    tox

Pre-commit Checks
-----------------
We provide a set of pre-commit checks that lint and check code for formatting.

Namely, we use:

* `yapf <https://github.com/google/yapf>`_ to check the formatting.
* The style of the docstrings is checked with `pydocstyle <https://github.com/PyCQA/pydocstyle>`_.
* Static type analysis is performed with `mypy <http://mypy-lang.org/>`_.
* Various linter checks are done with `pylint <https://www.pylint.org/>`_.
* Doctests are executed using the Python `doctest module <https://docs.python.org/3.5/library/doctest.html>`_.

Run the pre-commit checks locally from an activated virtual environment with development dependencies:

.. code-block:: bash

    ./precommit.py

* The pre-commit script can also automatically format the code:

.. code-block:: bash

    ./precommit.py  --overwrite


Versioning
==========
We follow `Semantic Versioning <http://semver.org/spec/v1.0.0.html>`_. The version X.Y.Z indicates:

* X is the major version (backward-incompatible),
* Y is the minor version (backward-compatible), and
* Z is the patch version (backward-compatible bug fix).
