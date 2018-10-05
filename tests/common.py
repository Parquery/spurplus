#!/usr/bin/env python3

# pylint: disable=missing-docstring

import os
import pathlib
import pwd
from typing import Optional  # pylint: disable=unused-import

import spur.ssh

import spurplus


class Params:
    """ represents parameters used to connect to the server. """

    def __init__(self) -> None:
        self.hostname = ''
        self.port = None  # type: Optional[int]
        self.username = None  # type: Optional[str]
        self.password = None  # type: Optional[str]
        self.private_key_file = None  # type: Optional[pathlib.Path]


def params_from_environ() -> Params:
    params = Params()
    params.hostname = os.environ.get("TEST_SSH_HOSTNAME", "127.0.0.1")

    if 'TEST_SSH_PORT' in os.environ:
        params.port = int(os.environ['TEST_SSH_PORT'])

    if 'TEST_SSH_USERNAME' in os.environ:
        params.username = os.environ['TEST_SSH_USERNAME']
    else:
        # Get the current local username
        params.username = pwd.getpwuid(os.getuid())[0]

    if 'TEST_SSH_PASSWORD' in os.environ:
        params.password = str(os.environ['TEST_SSH_PASSWORD'])

    if 'TEST_SSH_PRIVATE_KEY_FILE' in os.environ:
        params.private_key_file = pathlib.Path(os.environ['TEST_SSH_PRIVATE_KEY_FILE'])

    return params


def set_up_test_shell() -> spurplus.SshShell:
    """sets up a shell to the testing instance."""
    params = params_from_environ()

    try:
        shell = spurplus.connect_with_retries(
            hostname=params.hostname,
            port=params.port,
            username=params.username,
            password=params.password,
            private_key_file=params.private_key_file,
            missing_host_key=spur.ssh.MissingHostKey.accept,
            retries=2,
            retry_period=1)
    except ConnectionError as err:
        raise ConnectionError("Failed to connect to {}@{}:{}, private key file: {}, password is not None: {}".format(
            params.username, params.hostname, params.port, params.private_key_file,
            params.password is not None)) from err

    return shell
