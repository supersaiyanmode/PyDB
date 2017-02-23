import os

import pytest

from PyDB.structure.blocks import BlockStructure, Block, MultiBlockStructure
from PyDB.structure.blocks import BlockDataIterator
from PyDB.utils import bytes_to_ints, bytes_to_int, int_to_bytes
from PyDB.exceptions import PyDBIterationError, PyDBInternalError

class FileTest(object):
    file_path = "/tmp/block.test"

    def setup(self):
        self.f = open(self.file_path, "wb+")

    def teardown(self):
        self.f.close()
        os.unlink(self.file_path)

    def reopen_file(self):
        self.f.close()
        self.f = open(self.file_path, "rb+")

class TestBlockStructure(FileTest):
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
        self.reopen_file()
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

    def test_bad_magic(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        orig = Block.MAGIC_BYTES
        Block.MAGIC_BYTES = int_to_bytes(123424736, 4)
        bs.add_block(self.f, 16, fill=b'\x00\x00\x00\x10')
        Block.MAGIC_BYTES = orig
        self.reopen_file()
        with pytest.raises(PyDBInternalError):
            BlockStructure(self.f)

    def test_write_data(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        block = bs.add_block(self.f, 16, fill=b'\x00')
        block.write_data(self.f, 4, b'\x03\x05')
        self.reopen_file()
        bs2 = BlockStructure(self.f)
        got = [bytes_to_int(x[2]) for x in BlockDataIterator(self.f, bs2.blocks[0], 4)]
        expected = [-1, -1, -1, -1, 0, 50659328, 0, 0] #50659328 == \x03\x05\x00\x00
        assert expected == got

        with pytest.raises(PyDBInternalError):
            bs2.blocks[0].write_data(self.f, -1, b'')

        bs2.blocks[0].write_data(self.f, 10, b'\x00'*5)
        with pytest.raises(PyDBInternalError):
            bs2.blocks[0].write_data(self.f, 10, b'\x00'*6)

class TestDataIterator(FileTest):
    def test_data_presence(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        bs.add_block(self.f, 16, fill=b'\x00\x00\x00\x10')
        bs.add_block(self.f, 16, after=bs.blocks[0],
                fill=b'\x00\x00\x00\x20')
        self.reopen_file()
        bs2 = BlockStructure(self.f)
        got = [bytes_to_int(x[2]) for x in BlockDataIterator(self.f, bs2.blocks[0], 4)]
        expected = [-1, -1, -1, -1, 32, 32, 32, 32, 16, 16, 16, 16]

        assert expected == got

    def test_bad_chunksize(self):
        bs = BlockStructure(self.f, block_size=48, initialize=True)
        bs.add_block(self.f, 16, fill=b'\x00\x00\x00\x10')
        self.reopen_file()

        bs2 = BlockStructure(self.f)

        with pytest.raises(PyDBIterationError):
            list(BlockDataIterator(self.f, bs2.blocks[0], chunksize=10))

        with pytest.raises(PyDBIterationError):
            list(BlockDataIterator(self.f, bs2.blocks[0], chunksize=3))

class TestMultiBlockStructure(FileTest):
    def test_intialize(self):
        mbs = MultiBlockStructure(self.f, initialize=True, block_size=16)
        self.f.seek(0)
        got = bytes_to_ints(self.f.read())
        expected = [
            Block.MAGIC_VALUE, 16, -1, -1,
            -1, -1, -1, -1,
        ]

        assert expected == got

    def test_add_blocks(self):
        mbs = MultiBlockStructure(self.f, initialize=True, block_size=16)
        bs1 = mbs.add_structure(self.f, 16)
        bs2 = mbs.add_structure(self.f, 16)
        bs1.add_block(self.f, 16)
        bs2.add_block(self.f, 16)
        bs2.add_block(self.f, 16)

        self.f.seek(0)
        got = bytes_to_ints(self.f.read())
        expected = [
            Block.MAGIC_VALUE, 16, -1, -1,
            32, 64, -1, -1,
            Block.MAGIC_VALUE, 16, 96, -1,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, 128, -1,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, -1, 32,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, 160, 64,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, -1, 128,
            -1, -1, -1, -1,
        ]
        assert expected == got

