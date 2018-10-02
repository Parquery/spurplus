#!/usr/bin/env python3
"""Wrap paramiko.SFTP."""
import os
import pathlib
import socket
import time
from typing import TypeVar, Callable, Optional, Union  # pylint: disable=unused-import

import paramiko

T = TypeVar('T')


class ReconnectingSFTP:
    """Open automatically a new paramiko.SFTP on connection failure."""

    # pylint: disable=too-many-public-methods

    def __init__(self, sftp_opener: Callable[[], paramiko.SFTP], max_retries: int = 10,
                 retry_period: float = 0.1) -> None:
        """
        Iniialize.

        :param sftp_opener: method to open a new SFTP connection
        :param max_retries: maximum number of retries before raising ConnectionError
        :param retry_period: how long to wait between two retries; in seconds
        """
        self.__sftp_opener = sftp_opener
        self.max_retries = max_retries
        self.retry_period = retry_period

        self._sftp = None  # type: Optional[paramiko.SFTP]

        # last recorded working directory
        self.last_working_directory = None  # type: Optional[str]

    def close(self) -> None:
        """Close the the underlying paramiko SFTP client."""
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None

    def __enter__(self) -> 'ReconnectingSFTP':
        """Return self prepared in a constructor upon enter."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close upon exist."""
        self.close()

    def __wrap(self, method: Callable[[paramiko.SFTP], T]) -> T:
        """
        Wrap the SFTP method in a retry loop.

        Open an SFTP connection, if necessary, and change to the last recorded working directory before
        executing the method.

        :param method: to be wrapped
        :return: method's result
        """
        last_err = None  # type: Optional[Union[socket.error, EOFError]]

        success = False
        for _ in range(0, self.max_retries):
            try:
                if self._sftp is None:
                    self._sftp = self.__sftp_opener()
                assert self._sftp is not None

                if self._sftp.sock.closed:
                    self._sftp = self.__sftp_opener()
                assert not self._sftp.sock.closed

                if self.last_working_directory is not None:
                    self._sftp.chdir(path=self.last_working_directory)

                success = True

            except (socket.error, EOFError) as err:
                last_err = err

                if self._sftp is not None:
                    self._sftp.close()
                    self._sftp = None

                time.sleep(self.retry_period)

        if not success:
            raise ConnectionError(
                "Failed to execute an SFTP command after {} retries due to connection failure: {}".format(
                    self.max_retries, last_err))

        return method(self._sftp)

    def listdir_attr(self, path='.'):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.listdir_attr(path))

    def remove(self, path):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.remove(path))

    unlink = remove

    def posix_rename(self, oldpath, newpath):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.posix_rename(oldpath, newpath))

    def mkdir(self, path, mode=0o777):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.mkdir(path, mode))

    def rmdir(self, path):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.rmdir(path))

    def stat(self, path):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.stat(path))

    def lstat(self, path):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.lstat(path))

    def symlink(self, source, dest):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.symlink(source, dest))

    def chmod(self, path, mode):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.chmod(path, mode))

    def chown(self, path, uid, gid):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.chown(path, uid, gid))

    def put(self, localpath, remotepath, callback=None, confirm=True):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.put(localpath, remotepath, callback, confirm))

    def get(self, remotepath, localpath, callback=None):
        """See paramiko.SFTP documentation."""
        return self.__wrap(method=lambda sftp: sftp.get(remotepath, localpath, callback))


def _exists(sftp: Union[paramiko.SFTP, ReconnectingSFTP], remote_path: Union[str, pathlib.Path]) -> bool:
    """
    Check if a file exists on a remote machine.

    :param sftp: SFTP client
    :param remote_path: to the file
    :return: True if the file exists on the remote machine at `remote_path`
    """
    if isinstance(remote_path, str):
        rmt_pth_str = remote_path
    elif isinstance(remote_path, pathlib.Path):
        rmt_pth_str = remote_path.as_posix()
    else:
        raise NotImplementedError("Unhandled type of remote path: {}".format(type(remote_path)))

    permerr = None  # type: Optional[PermissionError]
    try:
        sftp.stat(rmt_pth_str)
        return True
    except FileNotFoundError:
        return False

    except PermissionError as err:
        permerr = err

    if permerr is not None:
        raise PermissionError("The remote path could not be accessed: {}".format(rmt_pth_str))

    raise AssertionError("Expected to raise before.")


def _mkdir(sftp: Union[paramiko.SFTP, ReconnectingSFTP],
           remote_path: Union[str, pathlib.Path],
           mode: int = 0o777,
           parents: bool = False,
           exist_ok: bool = False) -> None:
    """
    Create the remote directory with the given SFTP client.

    :param sftp: SFTP client
    :param remote_path: to the directory
    :param mode: directory permission mode
    :param parents: if set, creates the parent directories
    :param exist_ok: if set, ignores an existing directory.
    :return:
    """
    # pylint: disable=too-many-branches
    if isinstance(remote_path, str):
        rmt_pth = pathlib.Path(os.path.normpath(remote_path))
    elif isinstance(remote_path, pathlib.Path):
        rmt_pth = pathlib.Path(os.path.normpath(str(remote_path)))
    else:
        raise NotImplementedError("Unhandled type of remote path: {}".format(type(remote_path)))

    if _exists(sftp=sftp, remote_path=remote_path):
        if not exist_ok:
            raise FileExistsError("The remote directory already exists: {}".format(remote_path))
        else:
            return

    oserr = None  # type: Optional[OSError]

    if not parents:
        if not _exists(sftp=sftp, remote_path=rmt_pth.parent):
            raise FileNotFoundError(
                "The parent remote directory {} does not exist, parents=False and we need to mkdir: {}".format(
                    rmt_pth.parent, remote_path))

        try:
            sftp.mkdir(path=rmt_pth.as_posix(), mode=mode)
        except OSError as err:
            oserr = err

        if oserr is not None:
            msg = "Failed to create the directory {}: {}".format(rmt_pth.as_posix(), oserr)
            if isinstance(oserr, PermissionError):
                raise PermissionError(msg)
            else:
                raise OSError(msg)
    else:
        directories = list(reversed(rmt_pth.parents))
        directories.append(rmt_pth)

        root = pathlib.Path('/')

        for directory in directories:
            if directory == root:
                continue

            directory_exists = _exists(sftp=sftp, remote_path=directory)
            if directory_exists:
                continue

            try:
                sftp.mkdir(path=directory.as_posix(), mode=mode)
            except OSError as err:
                oserr = err

            if oserr is not None:
                msg = "Failed to create the directory {}: {}".format(directory.as_posix(), oserr)
                if isinstance(oserr, PermissionError):
                    raise PermissionError(msg)
                else:
                    raise OSError(msg)
