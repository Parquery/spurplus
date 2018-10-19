#!/usr/bin/env python3
"""Manage remote machines and perform file operations over SSH."""

import enum
import hashlib
import os
import pathlib
import shutil
import socket
import stat as stat_module
import subprocess
import time
import uuid
from typing import Optional, Union, TextIO, List, Dict, Sequence, Set

import paramiko
import spur
import spur.results
import spur.ssh
import temppathlib

import icontract
import spurplus.sftp

# pylint: disable=protected-access
# pylint: disable=too-many-lines


class Delete(enum.Enum):
    """Enumerate delete strategies when syncing."""

    BEFORE = 1
    AFTER = 2


class _SyncMap:
    """
    Represent the listing of file and the directories needed for syncing remote and local directories.

    All paths are given as relative paths.

    :ivar file_set: set of all files available in a directory including the files in subdirectories
    :ivar directory_set: set of all sub-directories in a directory (including the subdirectories with depth > 1)
    """

    def __init__(self) -> None:
        """Initialize file and directory set with empty sets."""
        self.file_set = set()  # type: Set[pathlib.Path]
        self.directory_set = set()  # type: Set[pathlib.Path]


def _local_sync_map(local_path: pathlib.Path) -> Optional[_SyncMap]:
    """
    List all the files and directories beneath the ``local_path``.

    All paths are given as relative paths to the ``local_path``.

    :param local_path: path to a local directory
    :return: collected sync map of a local directory, or None if the directory does not exist
    """
    if not local_path.exists():
        return None

    if not local_path.is_dir():
        raise NotADirectoryError("Local path is not a directory: {}".format(local_path))

    file_set = set()  # type: Set[pathlib.Path]
    directory_set = set()  # type: Set[pathlib.Path]
    for pth in local_path.glob("**/*"):
        rel_pth = pth.relative_to(local_path)

        if not pth.is_dir():
            file_set.add(rel_pth)
        else:
            directory_set.add(rel_pth)

    sync_map = _SyncMap()
    sync_map.file_set = file_set
    sync_map.directory_set = directory_set

    return sync_map


class DirectoryDiff:
    """
    Represent the difference between a local and a remote directory.

    All paths are given as relative.
    **L** designates the local machine, **R** designates the remote machine.

    :ivar local_only_files: files which exist on **L**, but are missing on **R**
    :ivar identical_files: files which are the same on **L** and **R**
    :ivar differing_files: files which differ between **L** and **R**
    :ivar remote_only_files: files which exist on **R**, but are missing on **L**
    :ivar local_only_directories: directories that exist only on **L**, but are missing on **R**
    :ivar common_directories: directories that exist both on **L** and on **R**
    :ivar remote_only_directories: directories that exist on **R**, but are missing on **L**

    """

    def __init__(self) -> None:
        """Initialize all the properties with empty lists."""
        self.local_only_files = []  # type: List[pathlib.Path]
        self.identical_files = []  # type: List[pathlib.Path]
        self.differing_files = []  # type: List[pathlib.Path]
        self.remote_only_files = []  # type: List[pathlib.Path]
        self.local_only_directories = []  # type: List[pathlib.Path]
        self.common_directories = []  # type: List[pathlib.Path]
        self.remote_only_directories = []  # type: List[pathlib.Path]


class SshShell(icontract.DBC):
    """
    Wrap a spur.SshShell instance.

    This wrapper adds typing and support for pathlib.Path and facilitates common tasks such as md5 sum computation and
    file operations.

    :ivar hostname: host name of the machine
    :vartype hostname: str

    :ivar port: port of the SSH connection
    :vartype port: int

    """

    # pylint: disable=too-many-public-methods

    def __init__(self,
                 spur_ssh_shell: spur.SshShell,
                 sftp: Union[paramiko.SFTP, spurplus.sftp.ReconnectingSFTP],
                 close_spur_shell: bool = True,
                 close_sftp: bool = True) -> None:
        """
        Initialize the SSH wrapper with the given underlying spur SshShell and the SFTP client.

        :param spur_ssh_shell: to wrap
        :param sftp: to wrap
        :param close_spur_shell: if set, closes spur shell when the wrapper is closed
        :param close_sftp: if set, closes SFTP when this wrapper is closed
        """
        self._spur = spur_ssh_shell
        self._sftp = sftp

        self.close_spur_shell = close_spur_shell
        self.close_sftp = close_sftp

        self.hostname = spur_ssh_shell._hostname
        self.port = spur_ssh_shell._port

    def as_spur(self) -> spur.ssh.SshShell:
        """
        Get the underlying spur shell instance.

        Use that instance if you need undocumented spur functionality.

        :return: underlying spur shell
        """
        return self._spur

    def as_sftp(self) -> Union[paramiko.SFTP, spurplus.sftp.ReconnectingSFTP]:
        """
        Get the underlying SFTP client.

        Use that client if you need fine-grained SFTP functionality not available in this class.

        :return: underlying SFTP client
        """
        return self._sftp

    def run(self,
            command: List[str],
            cwd: Union[str, pathlib.Path] = "",
            update_env: Optional[Dict[str, str]] = None,
            allow_error: bool = False,
            stdout: Optional[TextIO] = None,
            stderr: Optional[TextIO] = None,
            encoding: str = 'utf-8',
            use_pty: bool = False) -> spur.results.ExecutionResult:
        """
        Run a command on the remote instance and waits for it to complete.

        From https://github.com/mwilliamson/spur.py/blob/0.3.20/README.rst:

        :param command: to be executed
        :param cwd: change the current directory to this value before executing the command.
        :param update_env:
            environment variables to be set before running the command.

            If there's an existing environment variable with the same name, it will be overwritten.
            Otherwise, it is unchanged.

        :param allow_error:
            If False, an exception is raised if the return code of the command is anything but 0.
            If True, a result is returned irrespective of return code.

        :param stdout:
            if not None, anything the command prints to standard output during its execution will also be
            written to stdout using stdout.write.

        :param stderr:
            if not None, anything the command prints to standard error during its execution will also be
            written to stderr using stderr.write.

        :param encoding:
            if set, this is used to decode any output. By default, any output is treated as raw bytes.
            If set, the raw bytes are decoded before writing to the passed stdout and stderr arguments (if set)
            and before setting the output attributes on the result.

        :param use_pty: (undocumented in spur 0.3.20) If set, requests a pseudo-terminal from the server.

        :return: execution result
        :raise: spur.results.RunProcessError on an error if allow_error=False

        """
        # pylint: disable=too-many-arguments

        return self.spawn(
            command=command,
            cwd=cwd if isinstance(cwd, str) else cwd.as_posix(),
            update_env=update_env,
            allow_error=allow_error,
            stdout=stdout,
            stderr=stderr,
            encoding=encoding,
            use_pty=use_pty).wait_for_result()

    def check_output(self,
                     command: List[str],
                     update_env: Optional[Dict[str, str]] = None,
                     cwd: str = "",
                     stderr: Optional[TextIO] = None,
                     encoding: str = 'utf-8',
                     use_pty: bool = False) -> str:
        """
        Run a command on the remote instance that is not allowed to fail and captures its output.

        See run() for further documentation.

        :return: the captured output
        """
        # pylint: disable=too-many-arguments
        return self.run(
            command=command, update_env=update_env, cwd=cwd, stderr=stderr, encoding=encoding, use_pty=use_pty).output

    def spawn(self,
              command: List[str],
              update_env: Optional[Dict[str, str]] = None,
              store_pid: bool = False,
              cwd: Union[str, pathlib.Path] = "",
              stdout: Optional[TextIO] = None,
              stderr: Optional[TextIO] = None,
              encoding: str = 'utf-8',
              use_pty: bool = False,
              allow_error: bool = False) -> spur.ssh.SshProcess:
        """
        Spawn a remote process.

        From https://github.com/mwilliamson/spur.py/blob/0.3.20/README.rst:

        :param command: to be executed
        :param cwd: change the current directory to this value before executing the command.
        :param update_env:
            environment variables to be set before running the command.

            If there's an existing environment variable with the same name, it will be overwritten. Otherwise,
            it is unchanged.

        :param store_pid:
            If set to True, store the process id of the spawned process as the attribute pid on the
            returned process object.

        :param allow_error:
            If False, an exception is raised if the return code of the command is anything but 0.
            If True, a result is returned irrespective of return code.

        :param stdout:
            If not None, anything the command prints to standard output during its execution will also be
            written to stdout using stdout.write.

        :param stderr:
            If not None, anything the command prints to standard error during its execution will also be
            written to stderr using stderr.write.

        :param encoding:
            If set, this is used to decode any output. By default, any output is treated as raw bytes.
            If set, the raw bytes are decoded before writing to the passed stdout and stderr arguments (if set) and
            before setting the output attributes on the result.

        :param use_pty: (undocumented in spur 0.3.20) If set, requests a pseudo-terminal from the server.

        :return: spawned process
        :raise: spur.results.RunProcessError on an error if allow_error=False
        """
        # pylint: disable=too-many-arguments

        update_env_dict = {} if update_env is None else update_env

        return self._spur.spawn(
            command=command,
            cwd=cwd if isinstance(cwd, str) else cwd.as_posix(),
            update_env=update_env_dict,
            store_pid=store_pid,
            allow_error=allow_error,
            stdout=stdout,
            stderr=stderr,
            encoding=encoding,
            use_pty=use_pty)

    def md5(self, remote_path: Union[str, pathlib.Path]) -> str:
        """
        Compute MD5 checksum of the remote file. It is assumed that md5sum command is available on the remote machine.

        :param remote_path: to the file
        :return: MD5 sum
        """
        out = self.run(command=['md5sum', str(remote_path)]).output
        remote_hsh, _ = out.strip().split()

        return remote_hsh

    def md5s(self, remote_paths: Sequence[Union[str, pathlib.Path]]) -> List[Optional[str]]:
        """
        Compute MD5 checksums of multiple remote files individually.

        It is assumed that md5sum command is available on the remote machine.

        :param remote_paths: to the files
        :return: MD5 sum for each remote file separately; if a file does not exist, its checksum is set to None.
        """
        pth_to_index = dict(((str(pth), i) for i, pth in enumerate(remote_paths)))

        existing_pths = [str(pth) for pth in remote_paths if self.exists(remote_path=pth)]

        # chunk in order not to overflow the maximum argument length and count
        chunks = chunk_arguments(args=existing_pths)

        result = [None] * len(remote_paths)  # type: List[Optional[str]]

        for chunk in chunks:
            lines = self.check_output(command=['md5sum'] + chunk).splitlines()
            for line in lines:
                if len(line) > 0:
                    remote_hsh, pth = line.strip().split()
                    index = pth_to_index[pth]
                    result[index] = remote_hsh

        return result

    def put(self,
            local_path: Union[str, pathlib.Path],
            remote_path: Union[str, pathlib.Path],
            create_directories: bool = True,
            consistent: bool = True) -> None:
        """
        Put a file on the remote host.

        Mind that if you set consistent to True, the file will be copied to a temporary file and then
        POSIX rename function will be used to rename it. The ownership and the permissions of the original 'remote_path'
        are preserved. However, if the original 'remote_path' has read-only permissions and you still have write
        permissions to the directory, the 'remote_path' will be overwritten nevertheless due to the logic of
        POSIX rename.

        :param local_path: to the file
        :param remote_path: to the file
        :param create_directories: if set, creates the parent directory of the remote path with mode 0o777
        :param consistent: if set, copies to a temporary remote file first, and then renames it.
        :return:
        """
        # pylint: disable=too-many-branches
        # pylint: disable=too-many-statements
        rmt_pth = remote_path if isinstance(remote_path, pathlib.Path) else pathlib.Path(remote_path)

        loc_pth_str = local_path if isinstance(local_path, str) else str(local_path)

        if create_directories:
            spurplus.sftp._mkdir(sftp=self._sftp, remote_path=rmt_pth.parent, mode=0o777, parents=True, exist_ok=True)

        oserr = None  # type: Optional[OSError]

        if not consistent:
            try:
                self._sftp.put(localpath=loc_pth_str, remotepath=rmt_pth.as_posix())
            except OSError as err:
                oserr = err

            if oserr is not None:
                msg = "Failed to put the local file {} to the remote path {}: {}".format(
                    local_path, rmt_pth.as_posix(), oserr)
                if isinstance(oserr, PermissionError):
                    raise PermissionError(msg)
                else:
                    raise OSError(msg)

        else:
            tmp_pth = rmt_pth.parent / (rmt_pth.name + ".{}.tmp".format(uuid.uuid4()))
            success = False

            try:
                try:
                    self._sftp.put(localpath=loc_pth_str, remotepath=tmp_pth.as_posix())
                except OSError as err:
                    oserr = err

                if oserr is not None:
                    msg = "Failed to put the local file {} to the remote temporary path {}: {}".format(
                        local_path, tmp_pth, oserr)

                    if isinstance(oserr, PermissionError):
                        raise PermissionError(msg)
                    else:
                        raise OSError(msg)

                # apply the same permissions to the temporary file
                stat = None  # type: Optional[paramiko.SFTPAttributes]
                try:
                    stat = self._sftp.stat(rmt_pth.as_posix())
                except FileNotFoundError:
                    pass

                if stat is not None:
                    try:
                        self._sftp.chmod(path=tmp_pth.as_posix(), mode=stat.st_mode)
                        self._sftp.chown(path=tmp_pth.as_posix(), uid=stat.st_uid, gid=stat.st_gid)
                    except OSError as err:
                        oserr = err

                    if oserr is not None:
                        msg = ("Failed to change the permissions and ownership of "
                               "the remote temporary path {}: {}").format(tmp_pth, oserr)
                        if isinstance(oserr, PermissionError):
                            raise PermissionError(msg)
                        else:
                            raise OSError(msg)

                ioerr = None  # type: Optional[IOError]
                try:
                    self._sftp.posix_rename(oldpath=tmp_pth.as_posix(), newpath=rmt_pth.as_posix())
                except IOError as err:
                    ioerr = err

                if ioerr is not None:
                    raise IOError("Failed to rename the remote temporary file {} to the remote path {}: {}".format(
                        tmp_pth, remote_path, ioerr))

                success = True
            finally:
                if not success and self.exists(remote_path=tmp_pth):
                    self._sftp.unlink(path=tmp_pth.as_posix())

    def write_bytes(self,
                    remote_path: Union[str, pathlib.Path],
                    data: bytes,
                    create_directories: bool = True,
                    consistent: bool = True) -> None:
        """
        Write the binary data to a remote file.

        First, the data is written to a temporary local file. Next the local file is transferred to the remote path
        making sure that the connection is reestablished if needed.

        :param remote_path: to the file
        :param data: to be written
        :param create_directories: if set, creates the parent directory of the remote path with mode 0o777
        :param consistent: if set, writes to a temporary remote file first, and then renames it.
        :return:
        """
        rmt_pth = remote_path if isinstance(remote_path, pathlib.Path) else pathlib.Path(remote_path)

        if create_directories:
            spurplus.sftp._mkdir(sftp=self._sftp, remote_path=rmt_pth.parent, mode=0o777, parents=True, exist_ok=True)
        with temppathlib.NamedTemporaryFile() as tmp:
            tmp.path.write_bytes(data)
            self.put(
                local_path=tmp.path.as_posix(),
                remote_path=rmt_pth.as_posix(),
                consistent=consistent,
                create_directories=create_directories)

    def write_text(self,
                   remote_path: Union[str, pathlib.Path],
                   text: str,
                   encoding: str = 'utf-8',
                   create_directories: bool = True,
                   consistent: bool = True) -> None:
        """
        Write the binary content to the remote host.

        :param remote_path: to the file
        :param text: to be written
        :param encoding: to encode the text
        :param create_directories: if set, creates the parent directory of the remote path with mode 0o777
        :param consistent: if set, writes to a temporary remote file first, and then renames it.
        :return:
        """
        # pylint: disable=too-many-arguments
        data = text.encode(encoding=encoding)
        self.write_bytes(
            remote_path=remote_path, data=data, create_directories=create_directories, consistent=consistent)

    def _remote_sync_map(self, remote_path: pathlib.Path) -> Optional[_SyncMap]:
        """
        List all the files and directories beneath the ``remote_path``.

        :param remote_path: path to the remote directory
        :return: collected sync map, or None if the ``remote_path`` does not exist
        """
        a_stat = self.stat(remote_path=remote_path)
        if a_stat is None:
            return None

        if not stat_module.S_ISDIR(a_stat.st_mode):
            raise NotADirectoryError("Remote path is not a directory: {} (mode: {})".format(
                remote_path, a_stat.st_mode))

        file_set = set()  # type: Set[pathlib.Path]
        directory_set = set()  # type: Set[pathlib.Path]

        stack = []  # type: List[pathlib.Path]
        stack.append(remote_path)

        while stack:
            remote_subpth = stack.pop()

            for attr in self._sftp.listdir_attr(remote_subpth.as_posix()):
                remote_subsubpth = remote_subpth / attr.filename

                rel_pth = remote_subsubpth.relative_to(remote_path)

                if stat_module.S_ISDIR(attr.st_mode):
                    stack.append(remote_subsubpth)
                    directory_set.add(rel_pth)
                else:
                    file_set.add(rel_pth)

        sync_map = _SyncMap()
        sync_map.file_set = file_set
        sync_map.directory_set = directory_set

        return sync_map

    def directory_diff(self, local_path: Union[str, pathlib.Path],
                       remote_path: Union[str, pathlib.Path]) -> DirectoryDiff:
        """
        Iterate through the local and the remote directory and computes the diff.

        If one of the directories does not exist, all files are assumed "missing" in that directory.

        The identity of the files is based on MD5 checksums.

        :param local_path: path to the local directory
        :param remote_path: path to the remote directory
        :return: difference between the directories
        """
        local_pth = local_path if isinstance(local_path, pathlib.Path) else pathlib.Path(local_path)

        remote_pth = remote_path if isinstance(remote_path, pathlib.Path) else pathlib.Path(remote_path)

        local_map = _local_sync_map(local_path=local_pth)
        remote_map = self._remote_sync_map(remote_path=remote_pth)

        if local_map is None and remote_map is None:
            raise FileNotFoundError("Both the local and the remote path do not exist: {} and {}".format(
                local_pth, remote_pth))

        if local_map is None:
            assert remote_map is not None
            result = DirectoryDiff()
            result.remote_only_files = sorted(remote_map.file_set)
            result.remote_only_directories = sorted(remote_map.directory_set)
            return result

        if remote_map is None:
            assert local_map is not None
            result = DirectoryDiff()
            result.local_only_files = sorted(local_map.file_set)
            result.local_only_directories = sorted(local_map.directory_set)
            return result

        result = DirectoryDiff()
        result.local_only_files = sorted(local_map.file_set.difference(remote_map.file_set))
        result.local_only_directories = sorted(local_map.directory_set.difference(remote_map.directory_set))

        result.remote_only_files = sorted(remote_map.file_set.difference(local_map.file_set))

        result.remote_only_directories = sorted(remote_map.directory_set.difference(local_map.directory_set))

        result.common_directories = sorted(remote_map.directory_set.intersection(local_map.directory_set))

        # compare the files
        common_files = sorted(local_map.file_set.intersection(remote_map.file_set))

        local_md5s = []
        for rel_pth in common_files:
            local_md5s.append(hashlib.md5((local_pth / rel_pth).read_bytes()).hexdigest())

        remote_md5s = self.md5s(remote_paths=[remote_pth / rel_pth for rel_pth in common_files])

        for rel_pth, local_md5, remote_md5 in zip(common_files, local_md5s, remote_md5s):
            if local_md5 != remote_md5:
                result.differing_files.append(rel_pth)
            else:
                result.identical_files.append(rel_pth)

        return result

    def sync_to_remote(self,
                       local_path: Union[str, pathlib.Path],
                       remote_path: Union[str, pathlib.Path],
                       consistent: bool = True,
                       delete: Optional[Delete] = None,
                       preserve_permissions: bool = False) -> None:
        """
        Sync all the files beneath the ``local_path`` to ``remote_path``.

        Both local path and remote path are directories. If the ``remote_path`` does not exist, it is created. The
        files are compared with MD5 first and only the files whose MD5s mismatch are copied.

        Mind that the directory lists and the mapping (path -> MD5) needs to fit in memory for both the local path and
        the remote path.

        :param local_path: path to the local directory
        :param remote_path: path to the remote directory
        :param consistent: if set, writes to a temporary remote file first on each copy, and then renames it.
        :param delete:
            if set, files and directories missing in ``local_path`` and existing in ``remote_path`` are deleted.
        :param preserve_permissions:
            if set, the remote files and directories are chmod'ed to reflect the local files and directories,
            respectively.
        :return:
        """
        # pylint: disable=too-many-arguments
        # pylint: disable=too-many-branches
        local_pth = local_path if isinstance(local_path, pathlib.Path) else pathlib.Path(local_path)

        remote_pth = remote_path if isinstance(remote_path, pathlib.Path) else pathlib.Path(remote_path)

        if not local_pth.exists():
            raise FileNotFoundError("Local path does not exist: {}".format(local_pth))

        if not local_pth.is_dir():
            raise NotADirectoryError("Local path is not a directory: {}".format(local_pth))

        dir_diff = self.directory_diff(local_path=local_pth, remote_path=remote_pth)

        if delete is not None and delete == Delete.BEFORE:
            for rel_pth in dir_diff.remote_only_files:
                try:
                    self._sftp.remove(path=(remote_pth / rel_pth).as_posix())
                except FileNotFoundError as err:
                    raise FileNotFoundError("Failed to remove the file since it does not exist: {}".format(
                        remote_pth / rel_pth)) from err
                except OSError as err:
                    raise OSError("Failed to remove the file: {}".format(remote_pth / rel_pth)) from err

            # We need to go in reverse order in order to delete the children before the parent directories.
            for rel_pth in reversed(sorted(dir_diff.remote_only_directories)):
                self.remove(remote_path=remote_pth / rel_pth, recursive=False)

        # Create directories missing on the remote
        for rel_pth in dir_diff.local_only_directories:
            self.mkdir(remote_path=remote_pth / rel_pth)

        for rel_pths in [dir_diff.local_only_files, dir_diff.differing_files]:
            for rel_pth in rel_pths:
                self.put(
                    local_path=local_pth / rel_pth,
                    remote_path=remote_pth / rel_pth,
                    create_directories=False,
                    consistent=consistent)

        if preserve_permissions:
            for rel_pths in [dir_diff.local_only_directories, dir_diff.common_directories]:
                self.mirror_local_permissions(relative_paths=rel_pths, local_path=local_pth, remote_path=remote_pth)

            for rel_pths in [dir_diff.local_only_files, dir_diff.identical_files, dir_diff.differing_files]:
                self.mirror_local_permissions(relative_paths=rel_pths, local_path=local_pth, remote_path=remote_pth)

        if delete is not None and delete == Delete.AFTER:
            for rel_pth in dir_diff.remote_only_files:
                try:
                    self._sftp.remove(path=(remote_pth / rel_pth).as_posix())
                except FileNotFoundError as err:
                    raise FileNotFoundError("Failed to remove the file since it does not exist: {}".format(
                        remote_pth / rel_pth)) from err
                except OSError as err:
                    raise OSError("Failed to remove the file: {}".format(remote_pth / rel_pth)) from err

            # We need to go in reverse order in order to delete the children before the parent directories.
            for rel_pth in reversed(sorted(dir_diff.remote_only_directories)):
                self.remove(remote_path=remote_pth / rel_pth, recursive=False)

    def mirror_local_permissions(self, relative_paths: Sequence[Union[str, pathlib.Path]],
                                 local_path: Union[str, pathlib.Path], remote_path: Union[str, pathlib.Path]) -> None:
        """
        Set the permissions of the remote files to be the same as the permissions of the local files.

        The files are given as relative paths and are expected to exist both beneath ``local_path`` and
        beneath ``remote_path``.

        :param relative_paths: relative paths of files whose permissions are changed
        :param local_path: path to the local directory
        :param remote_path: path to the remote directory
        :return:
        """
        local_pth = local_path if isinstance(local_path, pathlib.Path) else pathlib.Path(local_path)

        remote_pth = remote_path if isinstance(remote_path, pathlib.Path) else pathlib.Path(remote_path)

        if not local_pth.exists():
            raise FileNotFoundError("Local path does not exist: {}".format(local_pth))

        if not local_pth.is_dir():
            raise NotADirectoryError("Local path is not a directory: {}".format(local_pth))

        if not self.is_dir(remote_path=remote_path):
            raise NotADirectoryError("Remote path is not a directory: {}".format(remote_pth))

        for rel_pth in relative_paths:
            local_file_pth = local_pth / rel_pth
            remote_file_pth = remote_pth / rel_pth

            self.chmod(remote_path=remote_file_pth, mode=local_file_pth.stat().st_mode)

    def get(self,
            remote_path: Union[str, pathlib.Path],
            local_path: Union[str, pathlib.Path],
            create_directories: bool = True,
            consistent: bool = True) -> None:
        """
        Get a file from the remote host.

        :param remote_path: to the file
        :param local_path: to the file
        :param create_directories: if set, creates the parent directories of the local path with permission mode 0o777
        :param consistent: if set, copies to a temporary local file first, and then renames it.
        :return:
        """
        rmt_pth_str = remote_path if isinstance(remote_path, str) else remote_path.as_posix()

        loc_pth = local_path if isinstance(local_path, pathlib.Path) else pathlib.Path(local_path)

        if create_directories:
            loc_pth.parent.mkdir(mode=0o777, exist_ok=True, parents=True)

        if consistent:
            with temppathlib.TemporaryDirectory() as local_tmpdir:
                tmp_pth = local_tmpdir.path / str(uuid.uuid4())
                self._sftp.get(remotepath=rmt_pth_str, localpath=tmp_pth.as_posix())
                shutil.move(src=tmp_pth.as_posix(), dst=loc_pth.as_posix())
        else:
            self._sftp.get(remotepath=rmt_pth_str, localpath=loc_pth.as_posix())

    def read_bytes(self, remote_path: Union[str, pathlib.Path]) -> bytes:
        """
        Read the binary data from a remote file.

        First the remote file is copied to a temporary local file making sure that the connection is reestablished if
        needed. Next the data is read.

        :param remote_path: to the file
        :return: binary content of the file
        """
        rmt_pth_str = remote_path if isinstance(remote_path, str) else remote_path.as_posix()

        permerr = None  # type: Optional[PermissionError]
        notfounderr = None  # type: Optional[FileNotFoundError]
        try:
            with temppathlib.NamedTemporaryFile() as tmp:
                self.get(remote_path=rmt_pth_str, local_path=tmp.path.as_posix(), consistent=True)
                return tmp.path.read_bytes()
        except PermissionError as err:
            permerr = err
        except FileNotFoundError as err:
            notfounderr = err

        if permerr is not None:
            raise PermissionError("The remote path could not be accessed: {}".format(rmt_pth_str))

        if notfounderr is not None:
            raise FileNotFoundError("The remote path was not found: {}".format(rmt_pth_str))

        raise AssertionError("Expected an exception before.")

    def read_text(self, remote_path: Union[str, pathlib.Path], encoding: str = 'utf-8') -> str:
        """
        Read the text content of a remote file.

        :param remote_path: to the file
        :param encoding: of the text file
        :return: binary content of the file
        """
        data = self.read_bytes(remote_path=remote_path)
        return data.decode(encoding=encoding)

    def exists(self, remote_path: Union[str, pathlib.Path]) -> bool:
        """
        Check whether a file exists.

        :param remote_path: to the file
        :return: True if the file exists on the remote machine at `remote_path`
        """
        return spurplus.sftp._exists(sftp=self._sftp, remote_path=remote_path)

    def mkdir(self,
              remote_path: Union[str, pathlib.Path],
              mode: int = 0o777,
              parents: bool = False,
              exist_ok: bool = False) -> None:
        """
        Create the remote directory.

        :param remote_path: to the directory
        :param mode: directory permission mode
        :param parents: if set, creates the parent directories
        :param exist_ok: if set, ignores an existing directory.
        :return:
        """
        spurplus.sftp._mkdir(sftp=self._sftp, remote_path=remote_path, mode=mode, parents=parents, exist_ok=exist_ok)

    def remove(self, remote_path: Union[str, pathlib.Path], recursive: bool = False) -> None:
        """
        Remove a file or a directory.

        :param remote_path: to a file or a directory
        :param recursive:
            if set, removes the directory recursively. This parameter has no effect if remote_path is not a directory.
        :return:
        """
        a_stat = self.stat(remote_path=remote_path)
        if a_stat is None:
            raise FileNotFoundError("Remote file does not exist and thus can not be removed: {}".format(remote_path))

        if not stat_module.S_ISDIR(a_stat.st_mode):
            self._sftp.remove(str(remote_path))
            return

        if not recursive:
            attrs = self._sftp.listdir_attr(str(remote_path))

            if len(attrs) > 0:
                raise OSError(
                    "The remote directory is not empty and the recursive flag was not set: {}".format(remote_path))

            self._sftp.rmdir(str(remote_path))
            return

        # Remove all files in the first step, then remove all the directories in a second step
        stack1 = []  # type: List[str]
        stack2 = []  # type: List[str]

        # First step: remove all files
        stack1.append(str(remote_path))

        while stack1:
            pth = stack1.pop()
            stack2.append(pth)

            for attr in self._sftp.listdir_attr(pth):
                subpth = os.path.join(pth, attr.filename)

                if stat_module.S_ISDIR(attr.st_mode):
                    stack1.append(subpth)
                else:
                    try:
                        self._sftp.remove(path=subpth)
                    except OSError as err:
                        raise OSError("Failed to remove the remote file while recursively removing {}: {}".format(
                            remote_path, subpth)) from err

        # Second step: remove all directories
        while stack2:
            pth = stack2.pop()

            try:
                self._sftp.rmdir(path=pth)
            except OSError as err:
                raise OSError("Failed to remove the remote directory while recursively removing {}: {}".format(
                    remote_path, pth)) from err

    def chmod(self, remote_path: Union[str, pathlib.Path], mode: int) -> None:
        """
        Change the permission mode of the file.

        :param remote_path: to the file
        :param mode: permission mode
        :return:
        """
        try:
            self._sftp.chmod(path=str(remote_path), mode=mode)
        except FileNotFoundError as err:
            raise FileNotFoundError("Remote file to be chmod'ed does not exist: {}".format(remote_path)) from err

    def stat(self, remote_path: Union[str, pathlib.Path]) -> Optional[paramiko.SFTPAttributes]:
        """
        Stat the given remote path.

        :param remote_path: to the file
        :return: stats of the file; None if the file does not exist
        """
        result = None  # type: Optional[paramiko.SFTPAttributes]
        try:
            result = self._sftp.stat(path=str(remote_path))
        except FileNotFoundError:
            pass

        return result

    def is_dir(self, remote_path: Union[str, pathlib.Path]) -> bool:
        """
        Check whether the remote path is a directory.

        :param remote_path: path to the remote file or directory
        :return: True if the remote path is a directory
        :raise: FileNotFound if the remote path does not exist
        """
        a_stat = self.stat(remote_path=remote_path)
        if a_stat is None:
            raise FileNotFoundError("Remote file does not exist: {}".format(remote_path))

        return stat_module.S_ISDIR(a_stat.st_mode)

    def is_symlink(self, remote_path: Union[str, pathlib.Path]) -> bool:
        """
        Check whether the remote path is a symlink.

        :param remote_path: path to the remote file or directory
        :return: True if the remote path is a directory
        :raise: FileNotFound if the remote path does not exist
        """
        try:
            a_lstat = self._sftp.lstat(path=str(remote_path))
            return stat_module.S_ISLNK(a_lstat.st_mode)

        except FileNotFoundError as err:
            raise FileNotFoundError("Remote file does not exist: {}".format(remote_path)) from err

    def symlink(self, source: Union[str, pathlib.Path], destination: Union[str, pathlib.Path]) -> None:
        """
        Create a symbolic link to the ``source`` remote path at ``destination``.

        :param source: remote path to the source
        :param destination: remote path where to store the symbolic link
        :return:
        """
        try:
            self._sftp.lstat(str(destination))
            raise FileExistsError("The destination of the symbolic link already exists: {}".format(destination))
        except FileNotFoundError:
            pass

        try:
            self._sftp.symlink(source=str(source), dest=str(destination))
        except OSError as err:
            raise OSError("Failed to create the symbolic link to {} at {}".format(source, destination)) from err

    def chown(self, remote_path: Union[str, pathlib.Path], uid: int, gid: int) -> None:
        """
        Change the ownership of the file.

        If you only want to change the uid or gid, please stat() the file before, and re-apply the current uid or gid,
        respectively.

        :param remote_path: to the file
        :param uid: ID of the user that owns the file
        :param gid: ID of the group that owns the file
        :return:
        """
        self._sftp.chown(path=str(remote_path), uid=uid, gid=gid)

    @icontract.post(lambda result: result == result.strip(), enabled=icontract.SLOW)
    def whoami(self) -> str:
        """Execute the `whoami` command and return the user name."""
        return self.check_output(command=['whoami']).strip()

    def close(self) -> None:
        """Close the underlying spur shell and SFTP (if ``close_spur_shell`` and ``close_sftp``, respectively)."""
        if self.close_spur_shell:
            self._spur.__exit__()

        if self.close_sftp:
            self._sftp.close()

    def __enter__(self) -> 'SshShell':
        """Enter the context and give the shell prepared in the constructor."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the underlying connection upon leaving the context."""
        self.close()


class TemporaryDirectory(metaclass=icontract.DBCMeta):
    """Represent a remote temporary directory."""

    @icontract.pre(lambda prefix: prefix is None or '/' not in prefix)
    @icontract.pre(lambda suffix: suffix is None or '/' not in suffix)
    def __init__(self,
                 shell: SshShell,
                 prefix: Optional[str] = None,
                 suffix: Optional[str] = None,
                 tmpdir: Optional[Union[str, pathlib.Path]] = None) -> None:
        """
        Create a temporary directory.

        :param shell: to the remote instance
        :param prefix: if specified, prefix of the directory file name
        :param suffix: if specified, suffix of the directory file name
        :param tmpdir: if specified, base directory in which the temporary directory will be created
        """
        self.shell = shell

        cmd = ['mktemp', '--directory']

        if suffix is not None:
            cmd.append('--suffix={}'.format(suffix))

        if tmpdir is not None:
            if not shell.exists(remote_path=tmpdir):
                raise FileNotFoundError(
                    "Remote parent directory of the temporary directory does not exist: {}".format(tmpdir))

            tmpdir_str = tmpdir if isinstance(tmpdir, str) else tmpdir.as_posix()
            cmd.append('--tmpdir={}'.format(tmpdir_str))

        if prefix is not None:
            template = []  # type: List[str]
            template.append(prefix)
            template.append('XXXXXXXXXX')

            cmd.append(''.join(template))

        self.path = pathlib.Path(shell.check_output(command=cmd).strip())

    def __enter__(self) -> 'TemporaryDirectory':
        """Enter the context already prepared in the constructor."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Remove the temporary directory."""
        self.shell.run(command=['rm', '-rf', self.path.as_posix()])


@icontract.pre(lambda retries: retries >= 0)
def connect_with_retries(hostname: str,
                         username: Optional[str] = None,
                         password: Optional[str] = None,
                         port: Optional[int] = None,
                         private_key_file: Optional[Union[str, pathlib.Path]] = None,
                         connect_timeout: Optional[int] = None,
                         missing_host_key: Optional[spur.ssh.MissingHostKey] = None,
                         shell_type: Optional[spur.ssh.ShShellType] = None,
                         look_for_private_keys: Optional[bool] = True,
                         load_system_host_keys: Optional[bool] = True,
                         sock: Optional[socket.socket] = None,
                         retries: int = 12,
                         retry_period: int = 5) -> SshShell:
    """
    Try to connect to the instance and retry on failure.

    Reconnect `retries` number of times and wait for `retry_period` seconds between the retries.

    For all the arguments except `retries` and `retry_period`, the documentation was copy/pasted from
    https://github.com/mwilliamson/spur.py/blob/0.3.20/README.rst:

    You need to specify some combination of a username, password and private key to authenticate.

    :param hostname: of the instance to connect
    :param username: for authentication
    :param password: for authentication
    :param port: for connection, default is 22
    :param private_key_file: path to the private key file
    :param connect_timeout: a timeout in seconds for establishing an SSH connection. Defaults to 60 (one minute).
    :param missing_host_key:
        by default, an error is raised when a host key is missing.

        One of the following values can be used to change the behaviour when a host key is missing:
        * spur.ssh.MissingHostKey.raise_error -- raise an error
        * spur.ssh.MissingHostKey.warn -- accept the host key and log a warning
        * spur.ssh.MissingHostKey.accept -- accept the host key

    :param shell_type:
        the type of shell used by the host. Defaults to spur.ssh.ShellTypes.sh, which should be
        appropriate for most Linux distributions. If the host uses a different shell, such as simpler shells often
        found on embedded systems, try changing shell_type to a more appropriate value,
        such as spur.ssh.ShellTypes.minimal. The following shell types are currently supported:

        * spur.ssh.ShellTypes.sh -- the Bourne shell. Supports all features.
        * spur.ssh.ShellTypes.minimal -- a minimal shell. Several features are unsupported:
        * Non-existent commands will not raise spur.NoSuchCommandError.
        * The following arguments to spawn and run are unsupported unless set to their default values:
          cwd, update_env, and store_pid.

    :param look_for_private_keys:
        by default, Spur will search for discoverable private key files in ~/.ssh/.
        Set to False to disable this behaviour.

    :param load_system_host_keys:
        by default, Spur will attempt to read host keys from the user's known hosts file,
        as used by OpenSSH, and no exception will be raised if the file can't be read.
        Set to False to disable this behaviour.

    :param sock: an open socket or socket-like object to use for communication to the target host.

    :param retries: (spurplus) number of re-tries if the connection could not be established
    :param retry_period: (spurplus) how many seconds to wait between the retries

    :return: established SshShell
    """
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-arguments

    private_key_file_str = None
    if private_key_file is not None:
        if isinstance(private_key_file, str):
            private_key_file_str = private_key_file
        else:
            private_key_file_str = private_key_file.as_posix()

    ssh_retries_left = retries

    last_err = None  # type: Union[None, Exception]
    bad_host_key_err = None  # type: Optional[ConnectionError]

    while True:
        try:
            spur_ssh_shell = spur.SshShell(
                hostname=hostname,
                username=username,
                password=password,
                port=port,
                private_key_file=private_key_file_str,
                connect_timeout=connect_timeout,
                missing_host_key=missing_host_key,
                shell_type=shell_type,
                look_for_private_keys=look_for_private_keys,
                load_system_host_keys=load_system_host_keys,
                sock=sock)
            spur_ssh_shell.run(command=['sh', '-c', 'echo hello > /dev/null'])

            # "ssh_retries_left" differ from ReconnectingSFTP retries and will not be returned to the SShShell.
            # "ssh_retries_left" is a value for how many times the ssh connection will be reestablished while
            # "max_retries" of ReconnectingSFTP stands for how many time the function in the wrapper will be retried
            # before raising a ConnectionError. Therefore never set "max_retries" equal "ssh_retries_left".
            sftp = spurplus.sftp.ReconnectingSFTP(sftp_opener=spur_ssh_shell._open_sftp_client)

            shell = SshShell(spur_ssh_shell=spur_ssh_shell, sftp=sftp)

            return shell

        except spur.ssh.ConnectionError as err:
            if isinstance(err.original_error, paramiko.ssh_exception.BadHostKeyException):
                # we can not recover from BadHostKeyException; if we try to re-connect, sshguard on the remote
                # host will block us for some time.
                bad_host_key_err = ConnectionError(
                    "Bad host key: hostname: {}; is the host name in your known hosts?".format(
                        err.original_error.hostname))

                break

            last_err = err
            ssh_retries_left -= 1
            if ssh_retries_left > 0:
                time.sleep(retry_period)
            else:
                break

    if bad_host_key_err is not None:
        raise bad_host_key_err  # pylint: disable=raising-bad-type
    else:
        raise ConnectionError("Failed to connect after {} retries to {}: {}".format(retries, hostname, last_err))


@icontract.pre(lambda argc_max: argc_max > 0)
@icontract.pre(lambda arg_max: arg_max > 0)
def chunk_arguments(args: List[str], arg_max: int = 16 * 1024, argc_max=1024) -> List[List[str]]:
    """
    Split a long list of command-line arguments into chunks.

    This is needed in order not to overflow the maximum length of the command-line arguments.

    :param args: command-line arguments
    :param arg_max: maximum length of the command-line arguments (i.e. the result of ``getconf ARG_MAX``)
    :param argc_max: maximum number of command-line arguments
    :return: chunked command-line arguments
    """
    for i, arg in enumerate(args):
        if len(arg) > arg_max:
            if len(arg) > 50:
                arg_str = arg[:50] + " [...]"
            else:
                arg_str = arg

            raise ValueError("The command-line argument {} is longer than allowed maximum length {}: {}".format(
                i, arg_max, arg_str))

    chunks = []  # type: List[List[str]]
    chunk_size = 0

    # latest chunk
    chunk = []  # type: List[str]
    for arg in args:
        if len(arg) + chunk_size > arg_max or chunk_size > argc_max:
            chunks.append(chunk)
            chunk = []
            chunk_size = 0

        chunk.append(arg)
        chunk_size += len(arg)
        if len(chunk) > 1:
            chunk_size += 1  # + 1 for white-space

    if len(chunk) > 0:
        chunks.append(chunk)

    return chunks
