#!/usr/bin/env python3

# pylint: disable=missing-docstring
import hashlib
import io
import pathlib
import platform
import time
import unittest
import uuid
from typing import Optional, List  # pylint: disable=unused-import

import temppathlib

import spurplus
import spurplus.sftp
import tests.common

# pylint: disable=too-many-lines


class TestReconnection(unittest.TestCase):
    def test_fail_connect_with_retries(self):
        connerr = None  # type: Optional[ConnectionError]
        try:
            _ = spurplus.connect_with_retries(hostname="some-nonexisting-hostname.com", retries=2, retry_period=1)
        except ConnectionError as err:
            connerr = err

        self.assertIsNotNone(connerr)


class TestProperties(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_hostname(self):
        params = tests.common.params_from_environ()
        self.assertEqual(params.hostname, self.shell.hostname)

    def test_port(self):
        params = tests.common.params_from_environ()

        # The default port 22 is set even if None was supplied to the spur.
        if params.port is None:
            self.assertEqual(22, self.shell.port)
        else:
            self.assertEqual(params.port, self.shell.port)


class TestTemporaryDirs(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_tmpdir(self):
        parent = pathlib.Path('/tmp') / str(uuid.uuid4())
        self.shell.mkdir(remote_path=parent)

        try:
            with spurplus.TemporaryDirectory(
                    shell=self.shell, prefix='pre pre', suffix='suf suf', tmpdir=parent) as tmpdir:
                pth = tmpdir.path

                self.assertTrue(tmpdir.path.name.startswith('pre pre'))
                self.assertTrue(tmpdir.path.name.endswith('suf suf'))
                self.assertIn(parent, list(tmpdir.path.parents))

                self.assertTrue(self.shell.exists(remote_path=pth))

            self.assertFalse(self.shell.exists(remote_path=pth))

            # with default parameters
            with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
                pth = tmpdir.path
                self.assertTrue(self.shell.exists(remote_path=pth))

            self.assertFalse(self.shell.exists(remote_path=pth))

        finally:
            self.shell.run(command=['rm', '-rf', parent.as_posix()])

    def test_invalid_base_directory(self):
        parent = pathlib.Path('/tmp') / str(uuid.uuid4())
        self.shell.mkdir(remote_path=parent)
        some_nonexisting_dir = parent / "some-nonexisting-dir"
        notfounderr = None  # type: Optional[FileNotFoundError]

        try:
            with spurplus.TemporaryDirectory(
                    shell=self.shell, prefix='pre pre', suffix='suf suf', tmpdir=some_nonexisting_dir):
                pass
        except FileNotFoundError as err:
            notfounderr = err
        finally:
            self.shell.run(command=['rm', '-rf', parent.as_posix()])

        self.assertEqual(
            "Remote parent directory of the temporary directory does not exist: {}".format(some_nonexisting_dir),
            str(notfounderr))


class TestRun(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_run(self):
        self.shell.run(command=['echo', 'hello world!'])

    def test_valid_encoding(self):
        run_utf8 = self.shell.run(command=['echo', 'hello world!'], encoding="utf-8")
        self.assertEqual('hello world!\n', run_utf8.output)
        run_iso = self.shell.run(command=['echo', 'hello world!'], encoding="iso-8859-1")
        self.assertEqual('hello world!\n', run_iso.output)

    def test_invalid_encoding(self):
        lookuperr = None  # type: Optional[LookupError]
        unknown_encoding = "this encoding doesn't exist"

        try:
            _ = self.shell.run(command=['echo', 'hello world!'], encoding=unknown_encoding)
        except LookupError as err:
            lookuperr = err
        finally:
            self.assertEqual("unknown encoding: {}".format(unknown_encoding), str(lookuperr))

    def test_check_output(self):
        out = self.shell.check_output(command=['echo', 'hello world!'])
        self.assertEqual('hello world!\n', out)

    def test_stdout_redirection(self):
        with io.StringIO() as buf:
            self.shell.run(command=['echo', 'hello world!'], stdout=buf)
            self.assertEqual("hello world!\n", buf.getvalue())

    def test_spawn(self):
        with io.StringIO() as buf:
            proc = self.shell.spawn(
                command=['bash', '-c', 'for i in `seq 1 1000`; do echo hello world; sleep 0.0001; done'], stdout=buf)
            result = proc.wait_for_result()
            self.assertEqual(0, result.return_code)

            expected = ''.join(["hello world\n"] * 1000)
            self.assertEqual(expected, buf.getvalue())


class TestBasicIO(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_put_get(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "some-dir" / "oi"

            with temppathlib.TemporaryDirectory() as local_tmpdir:
                local_pth = local_tmpdir.path / "local"
                local_pth.write_text("hello")

                self.shell.put(local_path=local_pth, remote_path=pth)

                another_local_pth = local_tmpdir.path / "some-other-dir" / "another_local"
                self.shell.get(remote_path=pth, local_path=another_local_pth)

                another_inconsistent_local_pth = local_tmpdir.path / "some-other-dir" / "another_inconsistent_local"
                self.shell.get(remote_path=pth, local_path=another_inconsistent_local_pth, consistent=False)

                self.assertTrue(another_local_pth.exists())
                self.assertEqual("hello", another_local_pth.read_text())
                self.assertEqual("hello", another_inconsistent_local_pth.read_text())

    def test_put_no_permission(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir:
            with temppathlib.TemporaryDirectory() as local_tmpdir:
                local_pth = local_tmpdir.path / 'file.txt'
                local_pth.write_text("hello")

                remote_pth = remote_tmpdir.path / 'file.txt'
                self.shell.put(local_path=local_pth, remote_path=remote_pth)
                self.shell.chmod(remote_path=remote_pth, mode=0o444)

                # consistent put succeeds even though the remote path has read-only permissions.
                self.shell.put(local_path=local_pth, remote_path=remote_pth, consistent=True)

                a_stat = self.shell.stat(remote_path=remote_pth.as_posix())
                self.assertEqual(0o100444, a_stat.st_mode)

                # direct put fails since we can not write to the file.
                with self.assertRaises(PermissionError):
                    self.shell.put(local_path=local_pth, remote_path=remote_pth, consistent=False)

                # consistent put fails if we don't have write permissions to the directory
                try:
                    self.shell.chmod(remote_path=remote_tmpdir.path, mode=0o444)

                    with self.assertRaises(PermissionError):
                        self.shell.put(local_path=local_pth, remote_path=remote_pth, consistent=True)
                finally:
                    self.shell.chmod(remote_path=remote_tmpdir.path, mode=0o777)

    def test_write_read_bytes(self):

        for consistent in [True, False]:
            with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
                pth = tmpdir.path / "some-dir" / str(consistent)

                self.shell.write_bytes(remote_path=pth, data=b"hello", consistent=consistent)
                data = self.shell.read_bytes(remote_path=pth)
                self.assertEqual(b"hello", data)

    def test_read_bytes_no_permission(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir:
            pth = remote_tmpdir.path / "some-file"
            self.shell.write_bytes(remote_path=pth, data=b"hello")
            try:
                self.shell.chmod(remote_path=pth, mode=0o111)

                with self.assertRaises(PermissionError):
                    self.shell.read_bytes(remote_path=pth)
            finally:
                self.shell.chmod(remote_path=pth, mode=0o777)

    def test_read_bytes_not_found(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir:
            pth = remote_tmpdir.path / "some-file"
            notfounderr = None  # type: Optional[FileNotFoundError]

            try:
                self.shell.read_bytes(remote_path=pth)
            except FileNotFoundError as err:
                notfounderr = err

            self.assertEqual("The remote path was not found: {}".format(pth.as_posix()), str(notfounderr))

    def test_write_read_text(self):
        for consistent in [True, False]:
            with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
                pth = tmpdir.path / "some-dir" / str(consistent)

                self.shell.write_text(remote_path=pth, text="hello", consistent=consistent)
                text = self.shell.read_text(remote_path=pth)
                self.assertEqual("hello", text)


class TestMD5(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_md5(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "oi"

            self.shell.write_text(remote_path=pth, text="hello")

            md5digest = self.shell.md5(remote_path=pth)

            expected = hashlib.md5("hello".encode()).hexdigest()
            self.assertEqual(expected, md5digest)

    def test_md5_with_space(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "oi hoi"

            self.shell.write_text(remote_path=pth, text="hello")

            md5digest = self.shell.md5(remote_path=pth)

            expected = hashlib.md5("hello".encode()).hexdigest()
            self.assertEqual(expected, md5digest)

    def test_md5s(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            expected = []  # type: List[Optional[str]]

            remote_pths = []  # type: List[pathlib.Path]
            for i in range(0, 128):
                pth = tmpdir.path / "{}.txt".format(i)
                remote_pths.append(pth)

                if i % 2 == 0:
                    self.shell.write_text(remote_path=pth, text="hello")
                    expected.append(hashlib.md5("hello".encode()).hexdigest())
                else:
                    expected.append(None)

            md5s = self.shell.md5s(remote_paths=remote_pths)
            self.assertListEqual(expected, md5s)


class TestFileOps(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_exists(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "some-dir" / "oi"

            self.assertFalse(self.shell.exists(remote_path=pth))
            self.shell.write_text(remote_path=pth, text="hello")
            self.assertTrue(self.shell.exists(remote_path=pth))

    def test_mkdir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "some-dir" / "oi"
            self.shell.mkdir(remote_path=pth, mode=0o700, parents=True)

            self.assertTrue(self.shell.exists(remote_path=pth))

            # check with SFTP
            a_stat = self.shell.stat(pth.as_posix())
            self.assertEqual(0o40700, a_stat.st_mode)

            # does nothing
            self.shell.mkdir(remote_path=pth, mode=0o700, parents=True, exist_ok=True)

            # existing directory raises an error on exist_ok=False
            existserr = None  # type: Optional[FileExistsError]
            try:
                self.shell.mkdir(remote_path=pth, mode=0o700, parents=True, exist_ok=False)
            except FileExistsError as err:
                existserr = err

            self.assertEqual("The remote directory already exists: {}".format(pth), str(existserr))

            # existing directory does not raise an error on exist_ok=True
            self.shell.mkdir(remote_path=pth, mode=0o700, exist_ok=True)

    def test_mkdir_with_permission_error(self):  # pylint: disable=invalid-name
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            try:
                self.shell.chmod(remote_path=tmpdir.path, mode=0o444)

                pth = tmpdir.path / "some-dir" / "oi"

                with self.assertRaises(PermissionError):
                    self.shell.mkdir(remote_path=pth, mode=0o700, parents=True)

            finally:
                self.shell.chmod(remote_path=tmpdir.path, mode=0o777)

    def test_is_dir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_dir = tmpdir.path / "some-dir"
            pth_to_file = tmpdir.path / "some-dir/some-file"
            pth_to_file_link = tmpdir.path / "some-dir/some-link-to-file"
            pth_to_dir_link = tmpdir.path / "some-link-to-dir"
            pth_to_nonexisting = tmpdir.path / "some-non-existing-file"

            self.shell.mkdir(remote_path=pth_to_dir)
            self.shell.write_text(remote_path=pth_to_file, text="hello")
            self.shell.symlink(source=pth_to_file, destination=pth_to_file_link)
            self.shell.symlink(source=pth_to_dir, destination=pth_to_dir_link)

            self.assertTrue(self.shell.is_dir(pth_to_dir))
            self.assertTrue(self.shell.is_dir(pth_to_dir_link))

            self.assertFalse(self.shell.is_dir(pth_to_file))
            self.assertFalse(self.shell.is_dir(pth_to_file_link))

            os_err = None  # type: Optional[OSError]
            try:
                self.shell.is_dir(remote_path=pth_to_nonexisting)
            except OSError as err:
                os_err = err

            self.assertIsNotNone(os_err)
            self.assertEqual("Remote file does not exist: {}".format(pth_to_nonexisting.as_posix()), str(os_err))

    def test_is_symlink(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_dir = tmpdir.path / "some-dir"
            pth_to_file = tmpdir.path / "some-dir/some-file"
            pth_to_file_link = tmpdir.path / "some-dir/some-link-to-file"
            pth_to_dir_link = tmpdir.path / "some-link-to-dir"
            pth_to_nonexisting = tmpdir.path / "some-non-existing-file"

            self.shell.mkdir(remote_path=pth_to_dir)
            self.shell.write_text(remote_path=pth_to_file, text="hello")
            self.shell.run(command=['ln', '-s', pth_to_file.as_posix(), pth_to_file_link.as_posix()])
            self.shell.run(command=['ln', '-s', pth_to_dir.as_posix(), pth_to_dir_link.as_posix()])

            self.assertFalse(self.shell.is_symlink(pth_to_dir))
            self.assertTrue(self.shell.is_symlink(pth_to_dir_link))

            pth_to_dir.is_symlink()
            self.assertFalse(self.shell.is_symlink(pth_to_file))
            self.assertTrue(self.shell.is_symlink(pth_to_file_link))

            self.assertFalse(self.shell.is_symlink(pth_to_dir))
            self.assertTrue(self.shell.is_symlink(pth_to_dir_link))

            notfounderr = None  # type: Optional[FileNotFoundError]
            try:
                self.shell.is_symlink(remote_path=pth_to_nonexisting)
            except FileNotFoundError as err:
                notfounderr = err

            self.assertEqual("Remote file does not exist: {}".format(pth_to_nonexisting.as_posix()), str(notfounderr))

    def test_symlink(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_dir = tmpdir.path / "some-dir"
            pth_to_file = tmpdir.path / "some-file"
            pth_to_file_link = tmpdir.path / "some-link-to-file"
            pth_to_dir_link = tmpdir.path / "some-link-to-dir"

            pth_to_another_file = tmpdir.path / "another-file"

            self.shell.mkdir(remote_path=pth_to_dir)
            self.shell.write_text(remote_path=pth_to_file, text="hello")
            self.shell.write_text(remote_path=pth_to_another_file, text="zdravo")

            self.shell.symlink(source=pth_to_file, destination=pth_to_file_link)
            self.shell.symlink(source=pth_to_dir, destination=pth_to_dir_link)

            self.assertFalse(self.shell.is_symlink(pth_to_file))
            self.assertTrue(self.shell.is_symlink(pth_to_file_link))

            self.assertFalse(self.shell.is_symlink(pth_to_dir))
            self.assertTrue(self.shell.is_symlink(pth_to_dir_link))

            self.assertEqual("hello", self.shell.read_text(remote_path=pth_to_file_link))

            # Overwriting a link is not possible.
            file_exists_err = None  # type: Optional[FileExistsError]
            try:
                self.shell.symlink(source=pth_to_another_file, destination=pth_to_file_link)
            except FileExistsError as err:
                file_exists_err = err

            self.assertIsNotNone(file_exists_err)
            self.assertEqual("The destination of the symbolic link already exists: {}".format(
                pth_to_file_link.as_posix()), str(file_exists_err))

    def test_symlink_with_nonexisting(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_file_link = tmpdir.path / "some-link-to-file"

            # It is OK if the source does not exist.
            pth_to_nonexisting = tmpdir.path / "some-non-existing-file"

            self.shell.symlink(source=pth_to_nonexisting, destination=pth_to_file_link)

            # It is not OK if the directory of the destination does not exist.
            pth_to_file = tmpdir.path / "some-file"
            self.shell.write_text(remote_path=pth_to_file, text="hello")

            pth_link_in_nonexisting_dir = tmpdir.path / "some-non-existing-dir/some-link"

            os_err = None  # type: Optional[OSError]
            try:
                self.shell.symlink(source=pth_to_file, destination=pth_link_in_nonexisting_dir)
            except OSError as err:
                os_err = err

            self.assertIsNotNone(os_err)
            self.assertEqual("Failed to create the symbolic link to {} at {}".format(
                pth_to_file.as_posix(), pth_link_in_nonexisting_dir.as_posix()), str(os_err))


class TestRemove(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_file(self):
        for recursive in [True, False]:
            with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
                # try to delete nonexisting file
                notfounderr = None  # type: Optional[FileNotFoundError]
                pth_to_file = tmpdir.path / "some-file"

                try:
                    self.shell.remove(remote_path=pth_to_file, recursive=recursive)
                except FileNotFoundError as err:
                    notfounderr = err

                self.assertEqual("Remote file does not exist and thus can not be removed: {}".format(
                    pth_to_file.as_posix()), str(notfounderr))

                self.shell.write_text(remote_path=pth_to_file, text="hello")

                self.assertTrue(self.shell.exists(remote_path=pth_to_file))
                self.shell.remove(remote_path=pth_to_file, recursive=recursive)
                self.assertFalse(self.shell.exists(remote_path=pth_to_file))

    def test_symlink(self):
        for recursive in [True, False]:
            with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
                pth_to_file = tmpdir.path / "some-file"
                self.shell.write_text(remote_path=pth_to_file, text="hello")

                pth_to_file_link = tmpdir.path / "some-link-to-file"
                self.shell.symlink(source=pth_to_file, destination=pth_to_file_link)

                self.assertTrue(self.shell.exists(remote_path=pth_to_file))
                self.assertTrue(self.shell.exists(remote_path=pth_to_file_link))
                self.shell.remove(remote_path=pth_to_file_link, recursive=recursive)

                self.assertTrue(self.shell.exists(remote_path=pth_to_file))
                self.assertFalse(self.shell.exists(remote_path=pth_to_file_link))

    def test_empty_dir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_some_dir = tmpdir.path / "some-dir"

            # Use the second directory to ensure that we don't go to the parent directory unintentionally
            pth_to_another_dir = tmpdir.path / "another-dir"

            self.shell.mkdir(remote_path=pth_to_some_dir)
            self.shell.mkdir(remote_path=pth_to_another_dir)

            self.assertTrue(self.shell.exists(remote_path=pth_to_some_dir))
            self.assertTrue(self.shell.exists(remote_path=pth_to_another_dir))

            self.shell.remove(remote_path=pth_to_some_dir)

            self.assertFalse(self.shell.exists(remote_path=pth_to_some_dir))
            self.assertTrue(self.shell.exists(remote_path=pth_to_another_dir))

    def test_non_empty_dir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_some_dir = tmpdir.path / "some-dir"
            pth_to_subdir = tmpdir.path / "some-dir/some-subdir"

            self.shell.mkdir(remote_path=pth_to_subdir, parents=True)

            os_err = None  # type: Optional[OSError]
            try:
                self.shell.remove(remote_path=pth_to_some_dir)
            except OSError as err:
                os_err = err

            self.assertIsNotNone(os_err)
            self.assertEqual("The remote directory is not empty and the recursive flag was not set: {}".format(
                pth_to_some_dir.as_posix()), str(os_err))

    def test_recursive(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth_to_some_dir = tmpdir.path / "some-dir"
            pth_to_subdir = tmpdir.path / "some-dir/some-subdir"
            pth_to_file = tmpdir.path / "some-dir/some-file"
            pth_to_another_file = tmpdir.path / "some-dir/some-subdir/some-file"
            pth_to_file_link = tmpdir.path / "some-dir/some-link"

            self.shell.mkdir(remote_path=pth_to_subdir, parents=True)

            self.shell.write_text(remote_path=pth_to_file, text="hello")
            self.shell.write_text(remote_path=pth_to_another_file, text="another hello")

            self.shell.symlink(source=pth_to_file, destination=pth_to_file_link)

            self.shell.remove(remote_path=pth_to_some_dir, recursive=True)

            for pth in [pth_to_another_file, pth_to_file, pth_to_file_link, pth_to_subdir, pth_to_some_dir]:
                self.assertFalse(self.shell.exists(remote_path=pth))


class TestMirrorPermissions(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_local_permissions(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth = local_tmpdir.path / "some-dir/some-file"
            local_pth.parent.mkdir()
            local_pth.write_text("hello")
            local_pth.chmod(0o612)

            remote_pth = remote_tmpdir.path / "some-dir/some-file"
            self.shell.mkdir(remote_path=remote_pth.parent)
            self.shell.write_text(remote_path=remote_pth, text="hello")

            self.shell.mirror_local_permissions(
                relative_paths=[pathlib.Path("some-dir/some-file")],
                local_path=local_tmpdir.path,
                remote_path=remote_tmpdir.path)

    def test_local_permissions_missing_file(self):  # pylint: disable=invalid-name
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth = local_tmpdir.path / "some-dir/some-file"
            local_pth.parent.mkdir()
            local_pth.write_text("hello")
            local_pth.chmod(0o612)

            not_found_err = None  # type: Optional[FileNotFoundError]
            try:
                self.shell.mirror_local_permissions(
                    relative_paths=[pathlib.Path("some-dir/some-file")],
                    local_path=local_tmpdir.path,
                    remote_path=remote_tmpdir.path)
            except FileNotFoundError as err:
                not_found_err = err

            self.assertIsNotNone(not_found_err)
            self.assertEqual("Remote file to be chmod'ed does not exist: {}".format(
                (remote_tmpdir.path / "some-dir/some-file").as_posix()), str(not_found_err))

    def test_local_dir_permission_invalid_pth(self):  # pylint: disable=invalid-name
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:

            local_dir = local_tmpdir.path / "some-nonexisting-dir"
            notfounderr = None  # type: Optional[FileNotFoundError]

            try:
                self.shell.mirror_local_permissions(
                    relative_paths=[pathlib.Path("some-relative-dir")],
                    local_path=local_dir,
                    remote_path=remote_tmpdir.path)
            except FileNotFoundError as err:
                notfounderr = err

            self.assertEqual("Local path does not exist: {}".format(local_dir), str(notfounderr))

            # file instead of dir as local path
            local_file = local_tmpdir.path / "some-file"
            local_file.write_text("hello")

            local_direrr = None  # type: Optional[NotADirectoryError]

            try:
                self.shell.mirror_local_permissions(
                    relative_paths=[pathlib.Path("some-relative-dir")],
                    local_path=local_file,
                    remote_path=remote_tmpdir.path)
            except NotADirectoryError as err:
                local_direrr = err

            self.assertEqual("Local path is not a directory: {}".format(local_file), str(local_direrr))

            # file instead of dir as remote path
            remote_file = remote_tmpdir.path / "some-file"
            self.shell.write_text(remote_path=remote_file, text="hello")

            remote_direrr = None  # type: Optional[NotADirectoryError]

            try:
                self.shell.mirror_local_permissions(
                    relative_paths=[pathlib.Path("some-relative-dir")],
                    local_path=local_tmpdir.path,
                    remote_path=remote_file)
            except NotADirectoryError as err:
                remote_direrr = err

            self.assertEqual("Remote path is not a directory: {}".format(remote_file), str(remote_direrr))


class TestSpurplusDirectoryDiff(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_nonexisting_remote_dir(self):
        with temppathlib.TemporaryDirectory() as local_tmpdir, \
                spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir:
            # Set up local
            local_only_dir = local_tmpdir.path / "local-only-dir"
            local_only_dir.mkdir()

            local_only_file = local_only_dir / "local-only-file"
            local_only_file.write_text("hello")

            # Inproperly set up remote
            remote_tmpdir = remote_tmpdir.path / "nonexisting-remote-dir"

            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir)

            self.assertListEqual([pathlib.Path('local-only-dir/local-only-file')], dir_diff.local_only_files)
            self.assertListEqual([pathlib.Path('local-only-dir')], dir_diff.local_only_directories)
            self.assertListEqual([], dir_diff.identical_files)
            self.assertListEqual([], dir_diff.common_directories)
            self.assertListEqual([], dir_diff.remote_only_files)
            self.assertListEqual([], dir_diff.remote_only_directories)

    def test_nonexisting_local_dir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir:
            # Inproperly set up local
            local_tmpdir = pathlib.Path("nonexisting")

            # Set up remote
            remote_only_dir = remote_tmpdir.path / "remote-only-dir"
            self.shell.mkdir(remote_path=remote_only_dir)

            remote_only_pth = remote_only_dir / "remote-only-file"
            self.shell.write_text(remote_path=remote_only_pth, text="hello")

            dir_diff = self.shell.directory_diff(local_path=local_tmpdir, remote_path=remote_tmpdir.path)

            self.assertListEqual([], dir_diff.local_only_files)
            self.assertListEqual([], dir_diff.local_only_directories)
            self.assertListEqual([], dir_diff.identical_files)
            self.assertListEqual([], dir_diff.common_directories)
            self.assertListEqual([pathlib.Path('remote-only-dir/remote-only-file')], dir_diff.remote_only_files)
            self.assertListEqual([pathlib.Path('remote-only-dir')], dir_diff.remote_only_directories)

    def test_no_local_and_remote_dir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir:
            local_tmpdir = pathlib.Path("nonexisting-local-dir")
            remote_tmpdir = remote_tmpdir.path / "nonexisting-remote-dir"

            notfounderr = None  # type: Optional[FileNotFoundError]

            try:
                self.shell.directory_diff(local_path=local_tmpdir, remote_path=remote_tmpdir)
            except FileNotFoundError as err:
                notfounderr = err

            self.assertEqual("Both the local and the remote path do not exist: {} and {}".format(
                local_tmpdir, remote_tmpdir), str(notfounderr))

    def test_file_instead_of_dir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:

            local_file = local_tmpdir.path / "local-file"
            local_file.write_text("hello")

            remote_file = remote_tmpdir.path / "remote-file"
            self.shell.write_text(remote_path=remote_file, text="hello")

            local_direrr = None  # type: Optional[NotADirectoryError]
            remote_direrr = None  # type: Optional[NotADirectoryError]

            try:
                self.shell.directory_diff(local_path=local_file, remote_path=remote_tmpdir.path)
            except NotADirectoryError as err:
                local_direrr = err

            self.assertEqual("Local path is not a directory: {}".format(local_file), str(local_direrr))

            try:
                self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_file)
            except NotADirectoryError as err:
                remote_direrr = err

            a_stat = self.shell.stat(remote_path=remote_file)
            self.assertEqual("Remote path is not a directory: {} (mode: {})".format(remote_file, a_stat.st_mode),
                             str(remote_direrr))

    def test_existing_dirs(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            # Set up local
            local_only_dir = local_tmpdir.path / "local-only-dir"
            local_only_dir.mkdir()

            local_only_file = local_only_dir / "local-only-file"
            local_only_file.write_text("hello")

            local_common_dir = local_tmpdir.path / "common"
            local_common_dir.mkdir()

            local_identical_file = local_common_dir / "identical-file"
            local_identical_file.write_text("hello")

            local_different_file = local_common_dir / "different-file"
            local_different_file.write_text("hi")

            # Set up remote
            remote_only_dir = remote_tmpdir.path / "remote-only-dir"
            self.shell.mkdir(remote_path=remote_only_dir)

            remote_only_pth = remote_only_dir / "remote-only-file"
            self.shell.write_text(remote_path=remote_only_pth, text="hello")

            remote_common_dir = remote_tmpdir.path / "common"
            self.shell.mkdir(remote_path=remote_common_dir)

            remote_identical_file = remote_common_dir / "identical-file"
            self.shell.write_text(remote_path=remote_identical_file, text="hello")

            remote_different_file = remote_common_dir / "different-file"
            self.shell.write_text(remote_path=remote_different_file, text="zdravo")

            # Diff
            dir_diff = self.shell.directory_diff(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertListEqual([pathlib.Path('local-only-dir/local-only-file')], dir_diff.local_only_files)
            self.assertListEqual([pathlib.Path('local-only-dir')], dir_diff.local_only_directories)
            self.assertListEqual([pathlib.Path('common/identical-file')], dir_diff.identical_files)
            self.assertListEqual([pathlib.Path('common/different-file')], dir_diff.differing_files)
            self.assertListEqual([pathlib.Path('common')], dir_diff.common_directories)
            self.assertListEqual([pathlib.Path('remote-only-dir/remote-only-file')], dir_diff.remote_only_files)
            self.assertListEqual([pathlib.Path('remote-only-dir')], dir_diff.remote_only_directories)


class TestSpurplusSyncToRemote(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_non_existing_remote_path(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth_to_file = local_tmpdir.path / "some-dir/some-file"
            local_pth_to_file.parent.mkdir()
            local_pth_to_file.write_text("hello")

            remote_pth_to_file = remote_tmpdir.path / "some-dir/some-file"

            self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))
            self.assertEqual("hello", self.shell.read_text(remote_path=remote_pth_to_file))

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
            self.shell.write_text(remote_path=remote_pth_to_file, text="zdravo")

            self.shell.sync_to_remote(local_path=local_dir, remote_path=remote_dir)

            self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))
            self.assertEqual("hello", self.shell.read_text(remote_path=remote_pth_to_file))

    def test_files_differ_with_spaces(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_dir = local_tmpdir.path / "some dir"
            local_pth_to_file = local_dir / "some file"
            local_pth_to_file.parent.mkdir()
            local_pth_to_file.write_text("hello")

            remote_dir = remote_tmpdir.path / "some dir"
            remote_pth_to_file = remote_dir / "some file"

            self.shell.mkdir(remote_path=remote_dir)
            self.shell.write_text(remote_path=remote_pth_to_file, text="zdravo")

            self.shell.sync_to_remote(local_path=local_dir, remote_path=remote_dir)

            self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))
            self.assertEqual("hello", self.shell.read_text(remote_path=remote_pth_to_file))

    def test_local_only_file(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth_to_file = local_tmpdir.path / "some-file"
            local_pth_to_file.write_text("hello")

            self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            remote_pth_to_file = remote_tmpdir.path / "some-file"
            self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))
            self.assertEqual("hello", self.shell.read_text(remote_path=remote_pth_to_file))

    @unittest.skipIf(platform.system() == "Windows", "Symbolic links admin privileges on Windows; "
                     "requesting admin privileges for unit testing is inappropriate.")
    def test_local_only_link(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth_to_file = local_tmpdir.path / "some-file"
            local_pth_to_link = local_tmpdir.path / "some-link"

            local_pth_to_file.write_text("hello")
            local_pth_to_link.symlink_to(local_pth_to_file)

            self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            # Check that the symlinks are copied as files, not as links
            remote_pth_to_link = remote_tmpdir.path / "some-link"
            self.assertFalse(self.shell.is_symlink(remote_path=remote_pth_to_link))
            self.assertTrue(self.shell.exists(remote_path=remote_pth_to_link))
            self.assertEqual("hello", self.shell.read_text(remote_path=remote_pth_to_link))

    def test_local_only_directory(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_dir = local_tmpdir.path / "some-dir"
            local_dir.mkdir()

            self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path)

            remote_dir = remote_tmpdir.path / "some-dir"
            self.assertTrue(self.shell.exists(remote_path=remote_dir))
            self.assertTrue(self.shell.is_dir(remote_path=remote_dir))

    def test_remote_only_file_is_deleted(self):  # pylint: disable=invalid-name
        for delete in [spurplus.Delete.BEFORE, spurplus.Delete.AFTER]:
            with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                    temppathlib.TemporaryDirectory() as local_tmpdir:
                remote_pth_to_file = remote_tmpdir.path / "some-file"
                self.shell.write_text(remote_path=remote_pth_to_file, text="hello")

                self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))

                self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path, delete=delete)

                self.assertFalse(self.shell.exists(remote_path=remote_pth_to_file))

    def test_remote_only_link_is_deleted(self):  # pylint: disable=invalid-name
        for delete in [spurplus.Delete.BEFORE, spurplus.Delete.AFTER]:
            with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                    temppathlib.TemporaryDirectory() as local_tmpdir:
                remote_pth_to_file = remote_tmpdir.path / "some-file"
                self.shell.write_text(remote_path=remote_pth_to_file, text="hello")

                remote_pth_to_link = remote_tmpdir.path / "some-link"
                self.shell.symlink(source=remote_pth_to_file, destination=remote_pth_to_link)

                # Make a file in local so that only the link is deleted
                (local_tmpdir.path / "some-file").write_text("hello")

                self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))
                self.assertTrue(self.shell.exists(remote_path=remote_pth_to_link))

                self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path, delete=delete)

                self.assertTrue(self.shell.exists(remote_path=remote_pth_to_file))
                self.assertFalse(self.shell.exists(remote_path=remote_pth_to_link))

    def test_remote_only_dir_is_deleted(self):
        for delete in [spurplus.Delete.BEFORE, spurplus.Delete.AFTER]:
            with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                    temppathlib.TemporaryDirectory() as local_tmpdir:
                remote_pth_to_dir = remote_tmpdir.path / "some-dir"
                self.shell.mkdir(remote_path=remote_pth_to_dir)

                self.shell.write_text(remote_path=remote_pth_to_dir / "some-file", text="hello")

                self.assertTrue(self.shell.exists(remote_path=remote_pth_to_dir))

                self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path, delete=delete)

                self.assertFalse(self.shell.exists(remote_path=remote_pth_to_dir))

    def test_remote_subdir_is_deleted(self):
        for delete in [spurplus.Delete.BEFORE, spurplus.Delete.AFTER]:
            with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                    temppathlib.TemporaryDirectory() as local_tmpdir:
                remote_pth_to_dir = remote_tmpdir.path / "some-dir"
                self.shell.mkdir(remote_path=remote_pth_to_dir)
                self.shell.write_text(remote_path=remote_pth_to_dir / "some-file", text="hello")

                remote_pth_to_subdir = remote_tmpdir.path / "some-dir" / "some-subdir"
                self.shell.mkdir(remote_path=remote_pth_to_subdir)
                self.shell.write_text(remote_path=remote_pth_to_subdir / "some-file", text="hello")

                self.assertTrue(self.shell.exists(remote_path=remote_pth_to_dir))
                self.assertTrue(self.shell.exists(remote_path=remote_pth_to_subdir))

                self.shell.sync_to_remote(local_path=local_tmpdir.path, remote_path=remote_tmpdir.path, delete=delete)

                self.assertFalse(self.shell.exists(remote_path=remote_pth_to_dir))
                self.assertFalse(self.shell.exists(remote_path=remote_pth_to_subdir))

    @unittest.skipIf(platform.system() == "Windows", "os.chmod is not properly implemented in Windows; "
                     "see https://stackoverflow.com/q/27500067")
    def test_preserve_permissions(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            # Prepare the local
            local_only_file = local_tmpdir.path / "some-file"
            local_only_file.write_text("hello")
            local_only_file.chmod(mode=0o601)

            local_only_dir = local_tmpdir.path / "some-dir"
            local_only_dir.mkdir(mode=0o701)

            local_common_file = local_tmpdir.path / "common-file"
            local_common_file.write_text("hello")
            local_common_file.chmod(mode=0o603)

            local_different_file = local_tmpdir.path / "different-file"
            local_different_file.write_text("hello")
            local_different_file.chmod(mode=0o604)

            local_common_dir = local_tmpdir.path / "common-dir"
            local_common_dir.mkdir(mode=0o705)

            # Prepare the remote
            remote_new_file = remote_tmpdir.path / "some-file"
            remote_new_dir = remote_tmpdir.path / "some-dir"

            remote_common_file = remote_tmpdir.path / "common-file"
            self.shell.write_text(remote_path=remote_common_file, text="hello")
            self.shell.chmod(remote_path=remote_common_file, mode=0o613)

            remote_different_file = remote_tmpdir.path / "different-file"
            self.shell.write_text(remote_path=remote_different_file, text="hello")
            self.shell.chmod(remote_path=remote_different_file, mode=0o614)

            remote_common_dir = remote_tmpdir.path / "common-dir"
            self.shell.mkdir(remote_path=remote_common_dir, mode=0o715)
            self.assertEqual(0o040715, self.shell.stat(remote_path=remote_common_dir).st_mode)

            # Execute
            self.shell.sync_to_remote(
                local_path=local_tmpdir.path, remote_path=remote_tmpdir.path, preserve_permissions=True)

            self.assertEqual(0o100601, self.shell.stat(remote_path=remote_new_file).st_mode)
            self.assertEqual(0o040701, self.shell.stat(remote_path=remote_new_dir).st_mode)
            self.assertEqual(0o100603, self.shell.stat(remote_path=remote_common_file).st_mode)
            self.assertEqual(0o100604, self.shell.stat(remote_path=remote_different_file).st_mode)
            self.assertEqual(0o040705, self.shell.stat(remote_path=remote_common_dir).st_mode)

    def test_nonexisting_local_path(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth = local_tmpdir.path / "some-dir"

            notfounderr = None  # type: Optional[FileNotFoundError]

            try:
                self.shell.sync_to_remote(local_path=local_pth, remote_path=remote_tmpdir.path)
            except FileNotFoundError as err:
                notfounderr = err

            self.assertEqual("Local path does not exist: {}".format(local_pth), str(notfounderr))

    def test_local_path_is_not_a_dir(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir, \
                temppathlib.TemporaryDirectory() as local_tmpdir:
            local_pth_to_file = local_tmpdir.path / "some-file"
            local_pth_to_file.write_text("hello")

            direrr = None  # type: Optional[NotADirectoryError]

            try:
                self.shell.sync_to_remote(local_path=local_pth_to_file, remote_path=remote_tmpdir.path)
            except NotADirectoryError as err:
                direrr = err

            self.assertEqual("Local path is not a directory: {}".format(local_pth_to_file), str(direrr))


class TestWhoami(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_that_it_works(self):
        params = tests.common.params_from_environ()
        self.assertEqual(params.username, self.shell.whoami())


@unittest.skip("executed only on demand")
class TestBenchmark(unittest.TestCase):
    def setUp(self):
        self.shell = tests.common.set_up_test_shell()

    def tearDown(self):
        self.shell.close()

    def test_that_reusing_sftp_is_faster_for_big_files(self):  # pylint: disable=invalid-name
        # Spurplus is slower at copying many small files than Spur due to the added safety overhead.
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            # open/close
            spur_shell = self.shell.as_spur()

            start = time.time()
            number_of_files = 4
            size = 1024 * 1024 * 2
            content = size * "hello"

            for i in range(0, number_of_files):
                pth = tmpdir.path / '{}.txt'.format(i)
                with spur_shell.open(name=pth.as_posix(), mode='wt') as fid:
                    fid.write(content)

            their_duration = time.time() - start

            # re-use sftp client
            start = time.time()
            for i in range(0, number_of_files):
                pth = tmpdir.path / '{}.txt'.format(i)
                self.shell.write_text(remote_path=pth, text=content, consistent=False, create_directories=False)

            our_duration = time.time() - start
            speedup = their_duration / our_duration
            self.assertGreater(speedup, 10.0)

    def test_md5_versus_md5s(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            remote_pths = []  # type: List[pathlib.Path]
            for i in range(0, 128):
                pth = tmpdir.path / "{}.txt".format(i)
                remote_pths.append(pth)

                if i % 2 == 0:
                    self.shell.write_text(remote_path=pth, text="hello")

            start = time.time()
            md5s = self.shell.md5s(remote_paths=remote_pths)
            md5s_duration = time.time() - start

            # now benchmark the manual implementation
            start = time.time()
            result = []  # type: List[Optional[str]]
            for remote_pth in remote_pths:
                if self.shell.exists(remote_path=remote_pth):
                    result.append(self.shell.md5(remote_path=remote_pth))
                else:
                    result.append(None)

            manual_duration = time.time() - start

            self.assertListEqual(md5s, result)

            speedup = manual_duration / md5s_duration
            self.assertGreaterEqual(speedup, 10.0)


if __name__ == '__main__':
    unittest.main()
