import os

import pytest

from PyDB.structure.blocks import BlockStructure, Block, MultiBlockStructure
from PyDB.structure.blocks import BlockStructureOrderedDataIO
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


class TestBlock(FileTest):
    def test_fill(self):
        b = Block(0, 16, -1, -1, 0)
        b.write_header(self.f)
        b.fill_data(self.f, b'\xff')
        self.reopen_file()

        content = self.f.read()
        got = bytes_to_ints(content)
        expected = [
            Block.MAGIC_VALUE, 16, -1, -1, 0,
            -1, -1, -1, -1
        ]

    def test_non_aligned_fill(self):
        b = Block(0, 16, -1, -1, 0)
        b.write_header(self.f)
        with pytest.raises(PyDBInternalError):
            b.fill_data(self.f, b'\xff'*3)

    def test_raw_write(self):
        b = Block(0, 16, -1, -1, 0)
        b.write_header(self.f)
        b.fill_data(self.f, b'\xff')
        b.write_data(self.f, 3, b'\x3C')
        self.reopen_file()

        content = self.f.read()
        got = bytes_to_ints(content)
        expected = [
            Block.MAGIC_VALUE, 16, -1, -1, 0,
            -196, -1, -1, -1
        ]
        assert expected == got


class TestBlockStructure(FileTest):
    def test_initialization(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        self.f.seek(0)
        got = self.f.read()
        got = bytes_to_ints(got)
        expected = [Block.MAGIC_VALUE, 16, -1, -1, 0,
                -1, -1, -1, -1]
        assert expected == got

    def test_add_block(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        bs.add_block(self.f, 16)
        self.f.seek(0)
        content = self.f.read()
        got = bytes_to_ints(content)
        expected = [
                Block.MAGIC_VALUE, 16, 36, -1, 0,
                -1, -1, -1, -1,
                Block.MAGIC_VALUE, 16, -1, 0, 0,
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
                (16, 72, -1, 0),
                (16, 36, 0, 0),
                (16, -1, 72, 0)]
        h = bs2.blocks
        got = [
                (h[0].size, h[0].next, h[0].prev, h[0].next_empty),
                (h[1].size, h[1].next, h[1].prev, h[1].next_empty),
                (h[2].size, h[2].next, h[2].prev, h[2].next_empty)]
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


class TestMultiBlockStructure(FileTest):
    def test_intialize(self):
        mbs = MultiBlockStructure(self.f, initialize=True, block_size=16)
        self.f.seek(0)
        got = bytes_to_ints(self.f.read())
        expected = [
            Block.MAGIC_VALUE, 16, -1, -1, 0,
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
            Block.MAGIC_VALUE, 16, -1, -1, 8,
            36, 72, -1, -1,
            Block.MAGIC_VALUE, 16, 108, -1, 0,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, 144, -1, 0,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, -1, 36, 0,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, 180, 72, 0,
            -1, -1, -1, -1,
            Block.MAGIC_VALUE, 16, -1, 144, 0,
            -1, -1, -1, -1,
        ]
        assert expected == got


class TestDataIterator(FileTest):
    def test_basic_data_io(self):
        msg = "Basic test."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(bs)

        io.append(self.f, msg.encode())
        self.f.flush()

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        io2 = BlockStructureOrderedDataIO(bs2)
        got = b"".join(BlockStructureOrderedDataIO(bs2).iterdata(self.f, chunk_size=1))
        expected = msg.encode()
        assert expected == got

    def test_auto_extension(self):
        msg = "This is an advanced test where multiple blocks will be dynamically added " +\
                "to the structure."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(bs)

        io.append(self.f, msg.encode())
        self.f.flush()

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        io2 = BlockStructureOrderedDataIO(bs2)
        got = b"".join(BlockStructureOrderedDataIO(bs2).iterdata(self.f, chunk_size=1))
        expected = msg.encode()
        assert expected == got

