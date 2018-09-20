#!/usr/bin/env python3

# pylint: disable=missing-docstring

import pathlib
import unittest
from typing import Optional, List, Union  # pylint: disable=unused-import

import paramiko
import temppathlib

import spurplus
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

    def test_files_differ(self):
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

    def test_remove(self):
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

    def test_put(self):
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

    def test_get(self):
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

    def test_listdir(self):
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
            self.assertTrue('some-dir1' in listdir)
            self.assertTrue('some-dir2' in listdir)
            self.assertTrue('some-dir3' in listdir)
            self.assertTrue('some-file' in listdir)

    def test_listdir_attr(self):
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

    def test_rename_file(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_file = tmpdir.path / "some-file"
            new_pth_to_file = tmpdir.path / "renamed-file"
            self.shell.write_text(remote_path=pth_to_file, text="hello")

            self.assertTrue(self.shell.exists(remote_path=pth_to_file))
            self.assertFalse(self.shell.exists(remote_path=new_pth_to_file))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.rename(oldpath=pth_to_file.as_posix(), newpath=new_pth_to_file.as_posix())

            self.assertFalse(self.shell.exists(remote_path=pth_to_file))
            self.assertTrue(self.shell.exists(remote_path=new_pth_to_file))

    def test_mkdir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_folder = tmpdir.path / "some-folder"

            self.assertFalse(self.shell.exists(remote_path=pth_to_folder))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.mkdir(path=pth_to_folder.as_posix())

            self.assertTrue(self.shell.exists(remote_path=pth_to_folder))

    def test_rmdir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_folder = tmpdir.path / "some-folder"
            self.reconnecting_sftp.mkdir(path=pth_to_folder.as_posix())

            self.assertTrue(self.shell.exists(remote_path=pth_to_folder))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.rmdir(path=pth_to_folder.as_posix())

            self.assertFalse(self.shell.exists(remote_path=pth_to_folder))

    def test_readlink(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_dir = tmpdir.path / "some-dir"
            pth_to_file = tmpdir.path / "some-dir/some-file"
            pth_to_file_link = tmpdir.path / "some-dir/some-link-to-file"

            self.shell.mkdir(remote_path=pth_to_dir)
            self.shell.write_text(remote_path=pth_to_file, text="hello")
            self.shell.symlink(source=pth_to_file, destination=pth_to_file_link)

            self.assertTrue(self.shell.is_symlink(remote_path=pth_to_file_link))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            file_path_str = self.reconnecting_sftp.readlink(path=pth_to_file_link.as_posix())

            self.assertTrue(pth_to_file.as_posix(), file_path_str)

    def test_change_working_directory(self):  # pylint: disable=invalid-name
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_dir = tmpdir.path / "some-dir"
            pth_to_another_dir = tmpdir.path / "another-dir"
            pth_to_file = tmpdir.path / "some-dir/some-file"
            pth_to_file_link = tmpdir.path / "some-dir/some-link-to-file"

            self.shell.mkdir(remote_path=pth_to_dir)
            self.shell.mkdir(remote_path=pth_to_another_dir)
            self.shell.write_text(remote_path=pth_to_file, text="hello")
            self.shell.symlink(source=pth_to_file, destination=pth_to_file_link)

            self.assertEqual(None, self.reconnecting_sftp.getcwd())
            self.assertNotEqual(pth_to_dir.as_posix(), self.reconnecting_sftp.normalize('.'))

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.chdir(path=pth_to_dir.as_posix())

            self.assertEqual(pth_to_dir.as_posix(), self.reconnecting_sftp.getcwd())

            self.reconnecting_sftp._sftp.sock.close()  # pylint: disable=protected-access
            self.reconnecting_sftp.chdir(path=pth_to_another_dir.as_posix())

            self.assertEqual(pth_to_another_dir.as_posix(), self.reconnecting_sftp.getcwd())


if __name__ == '__main__':
    unittest.main()
