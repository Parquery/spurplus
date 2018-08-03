Spur+
=====

Spur+ builds on top of Spur_ library to help you manage the remote machines running a common Linux distribution via SSH.
While we already find that Spur_ provides most of the functionality out-of-the-box, we missed certain features:

- typing. Since spur supports both Python 2 and 3, it does not provide any type annotations which makes it harder to use
  with type checkers such as mypy.

- pathlib.Path support. We find it easier to manipulate paths using pathlib.Path instead of plain strings. spur+
  provides support for both.

- a function for creating directories. spur relies on sftp client. While it is fairly straightforward to get an sftp
  client from `spur.SshShell` and create a directory, we think that it merits a wrapper function akin to
  `pathlib.Path.mkdir()` provided how often this functionality is needed.

- reading/writing text and binary data in one go. Similarly to creating directories, `spur.SshShell.open()` already
  provides all the functionality you need to read/write files. However, we found the usage code to be more readable when
  written in one line and no extra variables for file descriptors are introduced.

- a function for putting and getting files to/from the remote host, respectively.

- a function for computing MD5 checksums.

- a function to check if a file exists.

- a more elaborate context manager for a temporary directory which allows for specifying prefix, suffix and
  base directory and gives you a pathlib.Path. In contrast, `spur.temporary_directory()` gives you only a string with
  no knobs.

- an initializer function to repeatedly re-connect on connection failure. We found this function particularly important
  when you spin a virtual instance in the cloud and need to wait for it to initialize.

- a wrapper around paramiko's SFTP client (`spurplus.sftp.ReconnectingSFTP`) to automatically reconnect if the SFTP
  client experienced a connection failure. While original `spur.SshShell.open()` creates a new SFTP client on every
  call in order to prevent issues with time-outs, `spurplus.SshShell` is able to re-use the SFTP client over multiple
  calls via `spurplus.sftp.ReconnectingSFTP`.

  This can lead up to 25x speed-up (see the benchmark in `tests/live_test.py`).

.. _Spur: https://github.com/mwilliamson/spur.py

Usage
=====
.. code-block:: python

    import pathlib
    import contextlib

    import spurplus

    # re-try on connection failure; sftp client and the underlying spur SshShell are automatically closed when
    # shell is closed.
    with spurplus.connect_with_retries(hostname='some-machine.example.com', username='devop') as shell:
        p = pathlib.Path('/some/directory')

        # create a directory
        shell.mkdir(remote_path=p, parents=True, exist_ok=True)

        # write a file
        shell.write_text(remote_path=p/'some-file', text='hello world!')

        # read from a file
        text = shell.read_text(remote_path=p/'some-file')

        # change the permissions
        shell.chmod(remote_path=p/'some-file', mode=0o444)

        # stat the file
        print("The stat of {}: {}".format(p/'some-file', shell.stat(p/'some-file')))

        # use a wrapped SFTP client
        sftp = shell.as_sftp()
        # do something with the SFTP
        for attr in sftp.listdir_iter(path=p.as_posix()):
            do_something(attr.filename, attr.st_size)


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

    * ``TEST_SSH_HOSTNAME`` (host name of the SSH server, defaults to "127.0.0.1"),
    * ``TEST_SSH_PORT`` (optional, defaults to 22),
    * ``TEST_SSH_USERNAME`` (optional, uses paramiko's default),
    * ``TEST_SSH_PASSWORD`` (optional, uses private key file if not specified) and
    * ``TEST_SSH_PRIVATE_KEY_FILE`` (optional, looks for private key in expected places if not specified).

* We use tox for testing and packaging the distribution. Assuming that the above-mentioned environment variables has
  been set, the virutal environment has been activated and the development dependencies have been installed, run:

.. code-block:: bash

    tox

* We also provide a set of pre-commit checks that lint and check code for formatting. Run them locally from an activated
  virtual environment with development dependencies:

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