import os

from PyDB.structure.blocks import BlockStructure, Block
from PyDB.structure.blocks import BlockDataIterator
from PyDB.utils import bytes_to_ints, bytes_to_int

class TestBlockStructure(object):
    file_path = "/tmp/block.test"

    def setup(self):
        self.f = open(self.file_path, "wb+")

    def teardown(self):
        self.f.close()
        os.unlink(self.file_path)

    def test_initialization(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        self.f.seek(0)
        got = self.f.read()
        got = bytes_to_ints(got)
        expected = [Block.MAGIC_VALUE, 16, -1, -1,
                -1, -1, -1, -1]
        assert expected == got

    def test_add_block(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        bs.add_block(self.f, 16)
        self.f.seek(0)
        content = self.f.read()
        got = bytes_to_ints(content)
        expected = [
                Block.MAGIC_VALUE, 16, 32, -1,
                -1, -1, -1, -1,
                Block.MAGIC_VALUE, 16, -1, 0,
                -1, -1, -1, -1]
        assert expected == got

    def test_read_structure_header(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        bs.add_block(self.f, 16)
        bs.add_block(self.f, 16, after=bs.blocks[0])
        self.f.close()
        self.f = open(self.file_path, "rb+")
        bs2 = BlockStructure(self.f)

        assert len(bs2.blocks) == 3

        expected = [
                (16, 64, -1),
                (16, 32, 0),
                (16, -1, 64)]
        h = bs2.blocks
        got = [
                (h[0].size, h[0].next, h[0].prev),
                (h[1].size, h[1].next, h[1].prev),
                (h[2].size, h[2].next, h[2].prev)]
        assert expected == got

    def test_data_presence(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        bs.add_block(self.f, 16, fill=b'\x00\x00\x00\x10')
        bs.add_block(self.f, 16, after=bs.blocks[0],
                fill=b'\x00\x00\x00\x20')
        self.f.close()
        self.f = open(self.file_path, "rb+")
        bs2 = BlockStructure(self.f)
        got = [bytes_to_int(x[1]) for x in BlockDataIterator(self.f, bs2.blocks[0], 4)]
        expected = [-1, -1, -1, -1, 32, 32, 32, 32, 16, 16, 16, 16]

        assert expected == got

