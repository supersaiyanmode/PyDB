import os
from nose.tools import assert_equals

from PyDB.structure.blocks import BlockStructure, DataBlock, HeaderBlock
from PyDB.utils import bytes_to_ints

class TestBlockStructure(object):
    file_path = "/tmp/block.test"

    def setup(self):
        self.f = open(self.file_path, "wb+")

    def teardown(self):
        self.f.close()
        os.unlink(self.file_path)

    def test_initialization(self):
        HeaderBlock.HEADER_BLOCK_SIZE = 16
        bs = BlockStructure(self.f, initialize=True)
        self.f.seek(0)
        got = self.f.read()
        got = bytes_to_ints(got)
        expected = [HeaderBlock.HEADER_BLOCK_SIZE, -1, -1,
                0, -1, -1, -1, -1]
        assert_equals(expected, got)

