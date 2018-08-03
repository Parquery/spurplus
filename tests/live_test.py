#!/usr/bin/env python3

# pylint: disable=missing-docstring
import hashlib
import io
import os
import pathlib
import tempfile
import time
import unittest
import uuid
from typing import Optional, List  # pylint: disable=unused-import

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

    if 'TEST_SSH_PASSWORD' in os.environ:
        params.password = str(os.environ['TEST_SSH_PASSWORD'])

    if 'TEST_SSH_PRIVATE_KEY_FILE' in os.environ:
        params.private_key_file = pathlib.Path(os.environ['TEST_SSH_PRIVATE_KEY_FILE'])

    return params


class TestReconnection(unittest.TestCase):
    def test_fail_connect_with_retries(self):
        connerr = None  # type: Optional[ConnectionError]
        try:
            _ = spurplus.connect_with_retries(hostname="some-nonexisting-hostname.com", retries=2, retry_period=1)
        except ConnectionError as err:
            connerr = err

        self.assertEqual(
            str(connerr),
            "Failed to connect after 2 retries to some-nonexisting-hostname.com: Error creating SSH connection\n"
            "Original error: [Errno -2] Name or service not known")


class TestSpurplus(unittest.TestCase):
    def setUp(self):
        params = params_from_environ()
        self.shell = spurplus.connect_with_retries(
            hostname=params.hostname,
            port=params.port,
            username=params.username,
            password=params.password,
            private_key_file=params.private_key_file,
            retries=2,
            retry_period=1)

    def test_run(self):
        self.shell.run(command=['echo', 'hello world!'])

    def test_check_output(self):
        out = self.shell.check_output(command=['echo', 'hello world!'])
        self.assertEqual(out, 'hello world!\n')

    def test_stdout_redirection(self):
        buf = io.StringIO()
        self.shell.run(command=['echo', 'hello world!'], stdout=buf)
        self.assertEqual(buf.getvalue(), "hello world!\n")

    def test_spawn(self):
        buf = io.StringIO()
        proc = self.shell.spawn(
            command=['bash', '-c', 'for i in `seq 1 1000`; do echo hello world; sleep 0.0001; done'], stdout=buf)
        result = proc.wait_for_result()
        self.assertEqual(result.return_code, 0)

        expected = ''.join(["hello world\n"] * 1000)
        self.assertEqual(buf.getvalue(), expected)

    def test_tmpdir(self):
        pth = None  # type: Optional[pathlib.Path]

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
            pth = None  # type: Optional[pathlib.Path]
            with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
                pth = tmpdir.path
                self.assertTrue(self.shell.exists(remote_path=pth))

            self.assertFalse(self.shell.exists(remote_path=pth))

        finally:
            self.shell.run(command=['rm', '-rf', parent.as_posix()])

    def test_open_write_read(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "oi"

            # text
            with self.shell.open(remote_path=pth, mode='wt') as fid:
                fid.write("hello")

            with self.shell.open(remote_path=pth, mode='rt') as fid:
                text = fid.read()

            self.assertEqual(text, "hello")

            # binary
            with self.shell.open(remote_path=pth, mode='wb') as fid:
                fid.write(b"hello")

            with self.shell.open(remote_path=pth, mode='rb') as fid:
                data = fid.read()
            self.assertEqual(data, b'hello')

            # non-existing parent
            notfounderr = None  # type: Optional[FileNotFoundError]
            try:
                with self.shell.open(remote_path=pathlib.Path("/some/non-existing/path"), mode='wb') as fid:
                    pass
            except FileNotFoundError as err:
                notfounderr = err

            self.assertEqual(
                str(notfounderr),
                "Parent directory of the file you want to open does not exist: /some/non-existing/path")

            # non-existing read
            notfounderr = None  # type: Optional[FileNotFoundError]
            try:
                with self.shell.open(remote_path=pathlib.Path("/some/non-existing/path"), mode='rb') as fid:
                    pass
            except FileNotFoundError as err:
                notfounderr = err

            self.assertEqual(str(notfounderr), "[Errno 2] No such file: /some/non-existing/path")

    def test_md5(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "oi"

            with self.shell.open(remote_path=pth, mode='wt') as fid:
                fid.write("hello")

            md5digest = self.shell.md5(remote_path=pth)

            expected = hashlib.md5("hello".encode()).hexdigest()
            self.assertEqual(md5digest, expected)

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
            self.assertListEqual(md5s, expected)

    def test_benchmark_md5_versus_md5s(self):
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
            self.assertGreaterEqual(speedup, 20.0)

    def test_put_get(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "some-dir" / "oi"

            with tempfile.TemporaryDirectory() as local_tmpdir:
                local_pth = pathlib.Path(local_tmpdir) / "local"
                local_pth.write_text("hello")

                self.shell.put(local_path=local_pth, remote_path=pth)

                another_local_pth = pathlib.Path(local_tmpdir) / "some-other-dir" / "another_local"
                self.shell.get(remote_path=pth, local_path=another_local_pth)

                self.assertTrue(another_local_pth.exists())
                self.assertEqual(another_local_pth.read_text(), "hello")

    def test_put_with_permission_error(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as remote_tmpdir:
            with tempfile.TemporaryDirectory() as local_tmpdir:
                local_pth = pathlib.Path(local_tmpdir) / 'file.txt'
                local_pth.write_text("hello")

                remote_pth = remote_tmpdir.path / 'file.txt'
                self.shell.put(local_path=local_pth, remote_path=remote_pth)
                self.shell.chmod(remote_path=remote_pth, mode=0o444)

                # consistent put succeeds even though the remote path has read-only permissions.
                self.shell.put(local_path=local_pth, remote_path=remote_pth, consistent=True)

                stat = self.shell.stat(remote_path=remote_pth.as_posix())
                self.assertEqual(stat.st_mode, 0o100444)

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
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "some-dir" / "oi"

            self.shell.write_bytes(remote_path=pth, data=b"hello")
            data = self.shell.read_bytes(remote_path=pth)
            self.assertEqual(data, b"hello")

    def test_write_read_text(self):
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            pth = tmpdir.path / "some-dir" / "oi"

            self.shell.write_text(remote_path=pth, text="hello")
            text = self.shell.read_text(remote_path=pth)
            self.assertEqual(text, "hello")

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
            stat = self.shell.stat(pth.as_posix())
            self.assertEqual(stat.st_mode, 0o40700)

            # does nothing
            self.shell.mkdir(remote_path=pth, mode=0o700, parents=True, exist_ok=True)

            # existing directory raises an error on exist_ok=False
            existserr = None  # type: Optional[FileExistsError]
            try:
                self.shell.mkdir(remote_path=pth, mode=0o700, parents=True, exist_ok=False)
            except FileExistsError as err:
                existserr = err

            self.assertEqual(str(existserr), "The remote directory already exists: {}".format(pth))

    def test_mkdir_with_permission_error(self):  # pylint: disable=invalid-name
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            try:
                self.shell.chmod(remote_path=tmpdir.path, mode=0o444)

                pth = tmpdir.path / "some-dir" / "oi"

                with self.assertRaises(PermissionError):
                    self.shell.mkdir(remote_path=pth, mode=0o700, parents=True)

            finally:
                self.shell.chmod(remote_path=tmpdir.path, mode=0o777)

    def test_benchmark_that_reusing_sftp_its_faster(self):  # pylint: disable=invalid-name
        with spurplus.TemporaryDirectory(shell=self.shell) as tmpdir:
            # open/close
            spur_shell = self.shell.as_spur()

            start = time.time()
            file_count = 100
            for i in range(0, file_count):
                pth = tmpdir.path / '{}.txt'.format(i)
                with spur_shell.open(name=pth.as_posix(), mode='wt') as fid:
                    fid.write("hello")

            their_duration = time.time() - start

            # re-use sftp client
            start = time.time()
            sftp = self.shell.as_sftp()
            for i in range(0, file_count):
                pth = tmpdir.path / '{}.txt'.format(i)
                with sftp.open(pth.as_posix(), 'wt') as fid:
                    fid.write("hello")

            our_duration = time.time() - start
            speedup = their_duration / our_duration

            self.assertGreater(speedup, 20.0)

    def tearDown(self):
        self.shell.close()


if __name__ == '__main__':
    unittest.main()
