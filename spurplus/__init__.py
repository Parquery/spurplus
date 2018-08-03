#!/usr/bin/env python3
"""
Helps you manage remote machines via SSH.
"""
import contextlib
import pathlib
import shutil
import socket
import time
import uuid
from typing import Optional, Union, TextIO, BinaryIO, List, Dict

import paramiko
import spur
import spur.results
import spur.ssh

import spurplus.sftp

# pylint: disable=protected-access


class SshShell:
    """
    wraps a spur.SshShell instance.

    Adds typing and support for pathlib.Path and facilitates common tasks such as md5 sum computation,
    directory creation and providing one-liners for reading/writing files.
    """

    def __init__(self,
                 spur_ssh_shell: spur.SshShell,
                 sftp: Union[paramiko.SFTP, spurplus.sftp.ReconnectingSFTP],
                 close_spur_shell: bool = True,
                 close_sftp: bool = True) -> None:
        """
        Initializes the SSH wrapper with the given underlying spur SshShell and the SFTP client.

        :param spur_ssh_shell: to wrap
        :param sftp: to wrap
        :param close_spur_shell: if set, closes spur shell when the wrapper is closed
        :param close_sftp: if set, closes SFTP when this wrapper is closed
        """
        self._spur = spur_ssh_shell
        self._sftp = sftp

        self.close_spur_shell = close_spur_shell
        self.close_sftp = close_sftp

    def as_spur(self) -> spur.ssh.SshShell:
        """
        :return: the contained spur.SshShell instance;
        for example, use the contained SshShell instance if you need undocumented spur functionality.
        """
        return self._spur

    def as_sftp(self) -> Union[paramiko.SFTP, spurplus.sftp.ReconnectingSFTP]:
        """
        :return: the contained SFTP client;
        for example, use this client when you need more fine-grained operations such as retrieving
        stats of a file.
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
        runs a command on the remote instance and waits for it to complete.

        From https://github.com/mwilliamson/spur.py/blob/0.3.20/README.rst:

        :param command: to be executed
        :param cwd: change the current directory to this value before executing the command.
        :param update_env: environment variables to be set before running the command.
        If there's an existing environment variable with the same name, it will be overwritten.
        Otherwise, it is unchanged.

        :param allow_error:  If False, an exception is raised if the return code of the command is anything but 0.
        If True, a result is returned irrespective of return code.

        :param stdout:  if not None, anything the command prints to standard output during its execution will also be
        written to stdout using stdout.write.

        :param stderr: if not None, anything the command prints to standard error during its execution will also be
        written to stderr using stderr.write.

        :param encoding: if set, this is used to decode any output. By default, any output is treated as raw bytes.
        If set, the raw bytes are decoded before writing to the passed stdout and stderr arguments (if set) and before
        setting the output attributes on the result.

        :param use_pty: (undocumented in spur 0.3.20) If set, requests a pseudo-terminal from the server.

        :return: execution result
        :raise: spur.results.RunProcessError on an error if allow_error=False
        """
        # pylint: disable=too-many-arguments

        if not isinstance(encoding, str):
            raise ValueError("encoding must be specified, but got: {!r}".format(encoding))

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
        runs a command on the remote instance that is not allowed to fail and captures its output.

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
        spawns a remote process.

        From https://github.com/mwilliamson/spur.py/blob/0.3.20/README.rst:

        :param command: to be executed
        :param cwd: change the current directory to this value before executing the command.
        :param update_env: environment variables to be set before running the command.
        If there's an existing environment variable with the same name, it will be overwritten. Otherwise,
        it is unchanged.

        :param store_pid: if set to True, store the process id of the spawned process as the attribute pid on the
        returned process object.

        :param allow_error:  If False, an exception is raised if the return code of the command is anything but 0.
        If True, a result is returned irrespective of return code.

        :param stdout:  if not None, anything the command prints to standard output during its execution will also be
        written to stdout using stdout.write.

        :param stderr: if not None, anything the command prints to standard error during its execution will also be
        written to stderr using stderr.write.

        :param encoding: if set, this is used to decode any output. By default, any output is treated as raw bytes.
        If set, the raw bytes are decoded before writing to the passed stdout and stderr arguments (if set) and before
        setting the output attributes on the result.

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

    def open(self, remote_path: Union[str, pathlib.Path], mode: str = "r") -> Union[TextIO, BinaryIO]:
        """
        opens a file for reading or writing.

        By default, files are opened in text mode. Appending "b" to the mode will open the file in binary mode.

        :param remote_path: to the file
        :param mode: open mode
        :return: file descriptor
        """
        rmt_pth = remote_path if isinstance(remote_path, pathlib.Path) else pathlib.Path(remote_path)

        openerr = None  # type: Optional[Union[FileNotFoundError, PermissionError]]

        try:
            return self._spur.open(name=rmt_pth.as_posix(), mode=mode)

        except (FileNotFoundError, PermissionError) as err:
            openerr = err

        if isinstance(openerr, FileNotFoundError):
            if 'w' in mode:
                if not self.exists(remote_path=rmt_pth.parent):
                    raise FileNotFoundError(
                        "Parent directory of the file you want to open does not exist: {}".format(remote_path))

            raise FileNotFoundError("{}: {}".format(openerr, remote_path))

        elif isinstance(openerr, PermissionError):
            raise PermissionError("{}: {}".format(openerr, remote_path))

        else:
            raise NotImplementedError("Unhandled error type: {}: {}".format(type(openerr), openerr))

    def md5(self, remote_path: Union[str, pathlib.Path]) -> str:
        """
        computes MD5 checksum of the remote file. It is assumed that md5sum command is available on the remote machine.

        :param remote_path: to the file
        :return: MD5 sum
        """
        out = self.run(command=['md5sum', str(remote_path)]).output
        remote_hsh, _ = out.strip().split()

        return remote_hsh

    def md5s(self, remote_paths: List[Union[str, pathlib.Path]]) -> List[Optional[str]]:
        """
        computes MD5 checksums of multiple remote files individually. It is assumed that md5sum command is available
        on the remote machine.

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
        puts a file on the remote host.

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
        if isinstance(remote_path, str):
            rmt_pth = pathlib.Path(remote_path)
        elif isinstance(remote_path, pathlib.Path):
            rmt_pth = remote_path
        else:
            raise NotImplementedError("Unhandled type of remote path: {}".format(type(remote_path)))

        if isinstance(local_path, str):
            loc_pth_str = local_path
        elif isinstance(local_path, pathlib.Path):
            loc_pth_str = str(local_path)
        else:
            raise NotImplementedError("Unhandled type of local_path: {}".format(type(local_path)))

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
        writes the binary data to a remote file.

        :param remote_path: to the file
        :param data: to be written
        :param create_directories: if set, creates the parent directory of the remote path with mode 0o777
        :param consistent: if set, writes to a temporary remote file first, and then renames it.
        :return:
        """
        if isinstance(remote_path, str):
            rmt_pth = pathlib.Path(remote_path)
        elif isinstance(remote_path, pathlib.Path):
            rmt_pth = remote_path
        else:
            raise NotImplementedError("Unhandled type of remote path: {}".format(type(remote_path)))

        if create_directories:
            spurplus.sftp._mkdir(sftp=self._sftp, remote_path=rmt_pth.parent, mode=0o777, parents=True, exist_ok=True)

        if consistent:
            tmp_pth = rmt_pth.parent / (rmt_pth.name + ".{}".format(uuid.uuid4()))

            def cleanup() -> None:
                # pylint: disable=missing-docstring
                try:
                    self._sftp.remove(path=tmp_pth.as_posix())
                except:  # pylint: disable=bare-except
                    pass

            with contextlib.ExitStack() as exit_stack:
                exit_stack.callback(callback=cleanup)

                fid = self._sftp.open(tmp_pth.as_posix(), mode='wb')
                exit_stack.push(fid)

                fid.write(data)
                fid.flush()
                fid.close()

                self._sftp.posix_rename(oldpath=tmp_pth.as_posix(), newpath=rmt_pth.as_posix())
        else:
            with self._sftp.open(rmt_pth.as_posix(), mode='wb') as fid:
                fid.write(data)

    def write_text(self,
                   remote_path: Union[str, pathlib.Path],
                   text: str,
                   encoding: str = 'utf-8',
                   create_directories: bool = True,
                   consistent: bool = True) -> None:
        """
        writes the binary content to the remote host.

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

    def get(self,
            remote_path: Union[str, pathlib.Path],
            local_path: Union[str, pathlib.Path],
            create_directories: bool = True,
            consistent: bool = True) -> None:
        """
        gets a file from the remote host.

        :param remote_path: to the file
        :param local_path: to the file
        :param create_directories: if set, creates the parent directories of the local path with permission mode 0o777
        :param consistent: if set, copies to a temporary local file first, and then renames it.
        :return:
        """
        # pylint: disable=too-many-branches
        if isinstance(remote_path, str):
            rmt_pth_str = remote_path
        elif isinstance(remote_path, pathlib.Path):
            rmt_pth_str = remote_path.as_posix()
        else:
            raise NotImplementedError("Unhandled type of remote path: {}".format(type(remote_path)))

        if isinstance(local_path, str):
            loc_pth = pathlib.Path(local_path)
        elif isinstance(local_path, pathlib.Path):
            loc_pth = local_path
        else:
            raise NotImplementedError("Unhandled type of local path: {}".format(type(local_path)))

        if create_directories:
            loc_pth.parent.mkdir(mode=0o777, exist_ok=True, parents=True)

        with self._spur.open(rmt_pth_str, 'rb') as fsrc:
            if not consistent:
                with loc_pth.open('wb') as fdst:
                    shutil.copyfileobj(fsrc=fsrc, fdst=fdst)
            else:
                tmp_pth = loc_pth.parent / (loc_pth.name + ".{}.tmp".format(uuid.uuid4()))
                success = False

                try:
                    with tmp_pth.open('wb') as fdst:
                        shutil.copyfileobj(fsrc=fsrc, fdst=fdst)

                    tmp_pth.rename(loc_pth)
                    success = True
                finally:
                    if not success:
                        try:
                            tmp_pth.unlink()
                        except:  # pylint: disable=bare-except
                            pass

    def read_bytes(self, remote_path: Union[str, pathlib.Path]) -> bytes:
        """
        reads the binary data from a remote file.

        :param remote_path: to the file
        :return: binary content of the file
        """
        if isinstance(remote_path, str):
            rmt_pth_str = remote_path
        elif isinstance(remote_path, pathlib.Path):
            rmt_pth_str = remote_path.as_posix()
        else:
            raise NotImplementedError("Unhandled type of remote path: {}".format(type(remote_path)))

        permerr = None  # type: Optional[PermissionError]
        notfounderr = None  # type: Optional[FileNotFoundError]
        try:
            with self._spur.open(name=rmt_pth_str, mode='rb') as fid:
                return fid.read()
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
        reads the text content of a remote file.

        :param remote_path: to the file
        :param encoding: of the text file
        :return: binary content of the file
        """
        data = self.read_bytes(remote_path=remote_path)
        return data.decode(encoding=encoding)

    def exists(self, remote_path: Union[str, pathlib.Path]) -> bool:
        """
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
        creates the remote directory.

        :param remote_path: to the directory
        :param mode: directory permission mode
        :param parents: if set, creates the parent directories
        :param exist_ok: if set, ignores an existing directory.
        :return:
        """
        spurplus.sftp._mkdir(sftp=self._sftp, remote_path=remote_path, mode=mode, parents=parents, exist_ok=exist_ok)

    def chmod(self, remote_path: Union[str, pathlib.Path], mode: int) -> None:
        """
        changes the permission mode of the file.

        :param remote_path: to the file
        :param mode: permission mode
        :return:
        """
        self._sftp.chmod(path=str(remote_path), mode=mode)

    def stat(self, remote_path: Union[str, pathlib.Path]) -> Optional[paramiko.SFTPAttributes]:
        """
        stats the given remote path.

        :param remote_path: to the file
        :return: stats of the file; None if the file does not exist
        """
        result = None  # type: Optional[paramiko.SFTPAttributes]
        try:
            result = self._sftp.stat(path=str(remote_path))
        except FileNotFoundError:
            pass

        return result

    def chown(self, remote_path: Union[str, pathlib.Path], uid: int, gid: int) -> None:
        """
        changes the ownership of the file.

        If you only want to change the uid or gid, please stat() the file before, and re-apply the current uid or gid,
        respectively.

        :param remote_path: to the file
        :param uid: ID of the user that owns the file
        :param gid: ID of the group that owns the file
        :return:
        """
        self._sftp.chown(path=str(remote_path), uid=uid, gid=gid)

    def close(self) -> None:
        """ closes the shell. """
        if self.close_spur_shell:
            self._spur.__exit__()

        if self.close_sftp:
            self._sftp.close()

    def __enter__(self) -> 'SshShell':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class TemporaryDirectory:
    """ represents a remote temporary directory. """

    def __init__(self,
                 shell: SshShell,
                 prefix: Optional[str] = None,
                 suffix: Optional[str] = None,
                 tmpdir: Optional[Union[str, pathlib.Path]] = None) -> None:
        """
        creates a temporary directory.

        :param shell: to the remote instance
        :param prefix: if specified, prefix of the directory file name
        :param suffix: if specified, suffix of the directory file name
        :param tmpdir: if specified, base directory in which the temporary directory will be created
        """

        if prefix is not None and '/' in prefix:
            raise ValueError("Unexpected slash ('/') in prefix: {}".format(prefix))

        if suffix is not None and '/' in suffix:
            raise ValueError("Unexpected slash ('/') in suffix: {}".format(suffix))

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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shell.run(command=['rm', '-rf', self.path.as_posix()])


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
    tries to connect to the instance `retries` number of times and wait for `retry_period` seconds between
    the retries.

    For all the arguments except `retries` and `retry_period`, the documentation was copy/pasted from
    https://github.com/mwilliamson/spur.py/blob/0.3.20/README.rst:

    Requires a hostname. Also requires some combination of a username, password and private key,
    as necessary to authenticate.

    :param hostname: of the instance to connect
    :param username: for authentication
    :param password: for authentication
    :param port: for connection, default is 22
    :param private_key_file: path to the private key file
    :param connect_timeout: a timeout in seconds for establishing an SSH connection. Defaults to 60 (one minute).
    :param missing_host_key: by default, an error is raised when a host key is missing.
    One of the following values can be used to change the behaviour when a host key is missing:
        * spur.ssh.MissingHostKey.raise_error -- raise an error
        * spur.ssh.MissingHostKey.warn -- accept the host key and log a warning
        * spur.ssh.MissingHostKey.accept -- accept the host key

    :param shell_type: the type of shell used by the host. Defaults to spur.ssh.ShellTypes.sh, which should be
    appropriate for most Linux distributions. If the host uses a different shell, such as simpler shells often
    found on embedded systems, try changing shell_type to a more appropriate value,
    such as spur.ssh.ShellTypes.minimal. The following shell types are currently supported:

    * spur.ssh.ShellTypes.sh -- the Bourne shell. Supports all features.
    * spur.ssh.ShellTypes.minimal -- a minimal shell. Several features are unsupported:
        * Non-existent commands will not raise spur.NoSuchCommandError.
        * The following arguments to spawn and run are unsupported unless set to their default values:
          cwd, update_env, and store_pid.

    :param look_for_private_keys: by default, Spur will search for discoverable private key files in ~/.ssh/.
    Set to False to disable this behaviour.

    :param load_system_host_keys: by default, Spur will attempt to read host keys from the user's known hosts file,
    as used by OpenSSH, and no exception will be raised if the file can't be read.
    Set to False to disable this behaviour.

    :param sock: an open socket or socket-like object to use for communication to the target host.

    :param retries: (spurplus) number of re-tries if the connection could not be established
    :param retry_period: (spurplus) how many seconds to wait between the retries

    :return: established SshShell
    """
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-arguments
    if retries < 0:
        raise ValueError("Expected non-negative number of retries, got: {}".format(retries))

    private_key_file_str = None
    if private_key_file is not None:
        if isinstance(private_key_file, str):
            private_key_file_str = private_key_file
        else:
            private_key_file_str = private_key_file.as_posix()

    retries_left = retries

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
            retries_left -= 1
            if retries_left > 0:
                time.sleep(retry_period)
            else:
                break

    if bad_host_key_err is not None:
        raise bad_host_key_err  # pylint: disable=raising-bad-type
    else:
        raise ConnectionError("Failed to connect after {} retries to {}: {}".format(retries, hostname, last_err))


def chunk_arguments(args: List[str], arg_max: int = 16 * 1024, argc_max=1024) -> List[List[str]]:
    """
    splits a long list of command-line arguments into chunks in order not to overflow the maximum length of the
    command-line arguments.

    :param args: command-line arguments
    :param arg_max: maximum length of the command-line arguments (i.e. the result of ``getconf ARG_MAX``)
    :param argc_max: maximum number of command-line arguments
    :return: chunked command-line arguments
    """
    if argc_max <= 0:
        raise ValueError("Expected positive non-zero argc_max, got: {}".format(argc_max))

    if arg_max <= 0:
        raise ValueError("Expected positive non-zero arg_max, got: {}".format(arg_max))

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
