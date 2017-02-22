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

    def test_add_header_block(self):
        HeaderBlock.HEADER_BLOCK_SIZE = 16
        bs = BlockStructure(self.f, initialize=True)
        bs.add_header_block(self.f)
        self.f.seek(0)
        content = self.f.read()
        got = bytes_to_ints(content)
        expected = [
                HeaderBlock.HEADER_BLOCK_SIZE, 32, -1, 0,
                -1, -1, -1, -1,
                HeaderBlock.HEADER_BLOCK_SIZE, -1, 0, 0,
                -1, -1, -1, -1]
        assert_equals(expected, got)

    def test_read_structure_header(self):
        HeaderBlock.HEADER_BLOCK_SIZE = 16
        bs = BlockStructure(self.f, initialize=True)
        bs.add_header_block(self.f)
        bs.add_header_block(self.f, after=bs.header_blocks[0])
        self.f.close()
        self.f = open(self.file_path, "rb+")
        bs2 = BlockStructure(self.f)

        assert_equals(3, len(bs2.header_blocks))
        assert_equals(0, len(bs2.data_blocks))

        expected = [
                (HeaderBlock.HEADER_BLOCK_SIZE, 64, -1, 0),
                (HeaderBlock.HEADER_BLOCK_SIZE, 32, 0, 0),
                (HeaderBlock.HEADER_BLOCK_SIZE, -1, 64, 0)]
        h = bs2.header_blocks
        got = [
                (h[0].size, h[0].next, h[0].prev, h[0].type),
                (h[1].size, h[1].next, h[1].prev, h[1].type),
                (h[2].size, h[2].next, h[2].prev, h[2].type)]
        assert_equals(got, expected)

