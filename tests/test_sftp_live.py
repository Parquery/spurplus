#!/usr/bin/env python3

# pylint: disable=missing-docstring,no-self-use
import contextlib
import pathlib
import unittest
from typing import List  # pylint: disable=unused-import

import paramiko
import temppathlib
import spur.ssh

import spurplus
import spurplus.sftp
import tests.common


class TestConnectionFailure(unittest.TestCase):
    """
    Close underlying sftp connection before every shell command.

    These tests run the interior functions of the wrapped ReconnectingSFTP class which are used by spurplus.
    Goal of these tests is to show stability and sustainability of ReconnectingSFTP in case of a disconnection
    before/during a function call.
    The connection should be automatically restarted and the call should be re-executed so that no exception is thrown.
    """

    def setUp(self):
        self.shell = tests.common.set_up_test_shell()
        self.reconnecting_sftp = self.shell.as_sftp()
        assert isinstance(self.reconnecting_sftp, spurplus.sftp.ReconnectingSFTP)

    def tearDown(self):
        self.shell.close()

    def test_files_differ(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_dir = local_tmpdir.path / "some-dir"
            local_pth_to_file = local_dir / "some-file"
            local_pth_to_file.parent.mkdir()
            local_pth_to_file.write_text("hello")

            remote_dir = remote_tmpdir.path / "some-dir"
            remote_pth_to_file = remote_dir / "some-file"

            self.shell.mkdir(remote_path=remote_dir)

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.shell.write_text(remote_path=remote_pth_to_file, text="zdravo")

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.shell.sync_to_remote(local_path=local_dir, remote_path=remote_dir)

            self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))
            self.assertEqual("hello", self.shell.read_text(remote_path=remote_pth_to_file))

    def test_remove(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            remote_only_dir = remote_tmpdir.path / "remote-only-dir"

            self.shell.mkdir(remote_path=remote_only_dir)

            # Diff
            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertListEqual([pathlib.Path('remote-only-dir')], dir_diff.remote_only_directories)

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.shell.remove(remote_path=remote_only_dir, recursive=True)

            # Diff
            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertListEqual([], dir_diff.remote_only_directories)

    def test_put(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth = local_tmpdir.path / 'file.txt'
            local_pth.write_text("hello")

            remote_pth = remote_tmpdir.path / 'file.txt'

            # Diff
            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertListEqual([pathlib.Path('file.txt')], dir_diff.local_only_files)

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.shell.put(local_path=local_pth, remote_path=remote_pth)

            # Diff
            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertListEqual([pathlib.Path('file.txt')], dir_diff.identical_files)

    def test_get(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth = local_tmpdir.path / 'file.txt'

            remote_pth = remote_tmpdir.path / 'file.txt'
            self.shell.write_text(remote_path=remote_pth, text="hello")
            # Diff
            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertListEqual([pathlib.Path('file.txt')], dir_diff.remote_only_files)

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.shell.get(local_path=local_pth, remote_path=remote_pth)

            # Diff
            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertListEqual([pathlib.Path('file.txt')], dir_diff.identical_files)

    def test_listdir_attr(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_dir1 = tmpdir.path / "some-dir1"
            pth_to_dir2 = tmpdir.path / "some-dir2"
            pth_to_dir3 = tmpdir.path / "some-dir3"
            pth_to_file = tmpdir.path / "some-file"

            self.shell.mkdir(remote_path=pth_to_dir1)
            self.shell.mkdir(remote_path=pth_to_dir2)
            self.shell.mkdir(remote_path=pth_to_dir3)
            self.shell.write_text(remote_path=pth_to_file, text="hello")

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            listdir = self.reconnecting_sftp.listdir_attr(path=tmpdir.path.as_posix())

            self.assertEqual(4, len(listdir))
            self.assertTrue(type(List[paramiko.sftp_attr.SFTPAttributes]))

    def test_listdir(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_dir1 = tmpdir.path / "some-dir1"
            pth_to_dir2 = tmpdir.path / "some-dir2"
            pth_to_dir3 = tmpdir.path / "some-dir3"
            pth_to_file = tmpdir.path / "some-file"

            self.shell.mkdir(remote_path=pth_to_dir1)
            self.shell.mkdir(remote_path=pth_to_dir2)
            self.shell.mkdir(remote_path=pth_to_dir3)
            self.shell.write_text(remote_path=pth_to_file, text="hello")

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            listdir = self.reconnecting_sftp.listdir(path=tmpdir.path.as_posix())

            self.assertEqual(4, len(listdir))
            self.assertListEqual(['some-dir1', 'some-dir2', 'some-dir3', 'some-file'], sorted(listdir))

    def test_posix_rename_file(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_file = tmpdir.path / "some-file"
            new_pth_to_file = tmpdir.path / "renamed-file"
            self.shell.write_text(remote_path=pth_to_file, text="hello")

            self.assertTrue(self.shell.exists(remote_path=pth_to_file))
            self.assertFalse(self.shell.exists(remote_path=new_pth_to_file))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.posix_rename(oldpath=pth_to_file.as_posix(), newpath=new_pth_to_file.as_posix())

            self.assertFalse(self.shell.exists(remote_path=pth_to_file))
            self.assertTrue(self.shell.exists(remote_path=new_pth_to_file))

    def test_mkdir(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_folder = tmpdir.path / "some-folder"

            self.assertFalse(self.shell.exists(remote_path=pth_to_folder))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.mkdir(path=pth_to_folder.as_posix())

            self.assertTrue(self.shell.exists(remote_path=pth_to_folder))

    def test_rmdir(self) -> None:
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_folder = tmpdir.path / "some-folder"
            self.reconnecting_sftp.mkdir(path=pth_to_folder.as_posix())

            self.assertTrue(self.shell.exists(remote_path=pth_to_folder))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.rmdir(path=pth_to_folder.as_posix())

            self.assertFalse(self.shell.exists(remote_path=pth_to_folder))


class TestReconnectingSFTP(unittest.TestCase):
    def test_that_it_closes(self) -> None:
        params = tests.common.params_from_environ()

        sftp = spurplus.sftp.reconnecting_sftp(
            hostname=params.hostname,
            port=params.port,
            username=params.username,
            password=params.password,
            private_key_file=params.private_key_file,
            missing_host_key=spur.ssh.MissingHostKey.accept)

        # pylint: disable=protected-access

        with contextlib.ExitStack() as exit_stack:
            exit_stack.push(sftp)
            lst = sftp.listdir('.')
            assert lst is not None
            assert isinstance(lst, list)
            self.assertIsNotNone(sftp._sftp)

        self.assertIsNone(sftp._sftp)

    def test_that_it_reconnects(self) -> None:
        params = tests.common.params_from_environ()

        with spurplus.sftp.reconnecting_sftp(
                hostname=params.hostname,
                port=params.port,
                username=params.username,
                password=params.password,
                private_key_file=params.private_key_file,
                missing_host_key=spur.ssh.MissingHostKey.accept) as sftp:
            lst = sftp.listdir('.')
            assert lst is not None
            assert isinstance(lst, list)

            # pylint: disable=protected-access

            # Simulate a disconnection
            assert sftp._sftp is not None, \
                "Expected underlying paramiko sftp to be initialized after the first command of reconnecting SFTP."

            sftp._sftp.sock.close()

            lst = sftp.listdir('.')
            assert lst is not None
            assert isinstance(lst, list)


if __name__ == '__main__':
    unittest.main()
