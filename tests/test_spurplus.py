#!/usr/bin/env python3

# pylint: disable=missing-docstring

import unittest
from typing import Optional  # pylint: disable=unused-import

import spurplus


class TestChunkArguments(unittest.TestCase):
    def test_empty(self):
        self.assertListEqual(spurplus.chunk_arguments(args=[]), [])

    def test_single(self):
        self.assertListEqual(spurplus.chunk_arguments(args=['some-arg']), [['some-arg']])

    def test_arg_too_long(self):
        valerr = None  # type: Optional[ValueError]
        try:
            _ = spurplus.chunk_arguments(args=['some-arg'], arg_max=3)
        except ValueError as err:
            valerr = err

        self.assertEqual(str(valerr), "The command-line argument 0 is longer than allowed maximum length 3: some-arg")

    def test_small_arg_max(self):
        self.assertListEqual(
            spurplus.chunk_arguments(args=['some-arg', 'other-arg'], arg_max=10), [['some-arg'], ['other-arg']])

    def test_small_argc_max(self):
        self.assertListEqual(
            spurplus.chunk_arguments(args=['some-arg', 'other-arg'], argc_max=1), [['some-arg'], ['other-arg']])

    def test_no_split(self):
        self.assertListEqual(
            spurplus.chunk_arguments(args=['some-arg', 'other-arg'], arg_max=16 * 1024, argc_max=1024),
            [['some-arg', 'other-arg']])

    def test_chunk_with_multiple_args(self):
        self.assertListEqual(
            spurplus.chunk_arguments(args=['some-arg', 'other-arg', 'yet-another-arg'], arg_max=20),
            [['some-arg', 'other-arg'], ['yet-another-arg']])


if __name__ == '__main__':
    unittest.main()
