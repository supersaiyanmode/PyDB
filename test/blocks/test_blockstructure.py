import os

import pytest

from PyDB.structure.blocks import BlockStructure, Block, MultiBlockStructure
from PyDB.structure.blocks import BlockStructureOrderedDataIO
from PyDB.utils import bytes_to_ints, bytes_to_int, int_to_bytes, string_to_bytes
from PyDB.exceptions import PyDBIterationError, PyDBInternalError

from ..base import FileBasedTest


class TestBlock(FileBasedTest):
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
        with pytest.raises(PyDBInternalError) as e:
            b.fill_data(self.f, b'\xff'*3)
        expected = "Internal Error. Possibly corrupt database. Can't fill data. Not aligned."
        assert e.value.message == expected

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


class TestBlockStructure(FileBasedTest):
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
        with pytest.raises(PyDBInternalError) as ex:
            BlockStructure(self.f)
        expected = "Internal Error. Possibly corrupt database. Not a block at start position: 0."
        assert ex.value.message == expected


class TestMultiBlockStructure(FileBasedTest):
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


class TestDataIterator(FileBasedTest):
    def test_basic_data_io(self):
        msg = "Basic test."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs)

        io.write(string_to_bytes(msg))
        self.f.flush()

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        io2 = BlockStructureOrderedDataIO(self.f, bs2)
        got = b"".join(io2.iterdata(0, chunk_size=1))
        expected = msg.encode()
        assert expected == got

    def test_auto_extension(self):
        msg = "This is an advanced test where multiple blocks will be dynamically added " +\
                "to the structure."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs, blocksize=16)

        io.write(string_to_bytes(msg))
        self.f.flush()
        assert len(bs.blocks) == len(msg) // 16 + 1

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        io2 = BlockStructureOrderedDataIO(self.f, bs2)
        got = b"".join(BlockStructureOrderedDataIO(self.f, bs2).iterdata(0, chunk_size=1))
        expected = msg.encode()
        assert expected == got

    def test_write_with_truncate(self):
        msg = "This is a not-so-basic test. I need to fill about one more block." +\
                " And I'm out of ideas."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs, blocksize=16)
        io.write(string_to_bytes(msg))
        self.f.flush()

        assert len(bs.blocks) == 6

        msg2 = "definitely a not-so-basic test."
        io.seek(8)
        io.write(string_to_bytes(msg2), truncate=True)
        self.f.flush()

        assert len(bs.blocks) == 3

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        io2 = BlockStructureOrderedDataIO(self.f, bs2)
        got = b"".join(BlockStructureOrderedDataIO(self.f, bs2).iterdata(0, chunk_size=1))
        expected = "This is definitely a not-so-basic test.".encode()
        assert expected == got

    def test_write_without_truncate(self):
        msg = "This is a not-so-basic test. I need to fill about one more block." +\
                " And I'm out of ideas."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs, blocksize=16)

        io.write(string_to_bytes(msg))

        msg2 = "definitely a not-so-basic test."
        io.seek(21)
        io.write(string_to_bytes(msg2))
        self.f.flush()

        assert len(bs.blocks) == 6

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        got = BlockStructureOrderedDataIO(self.f, bs2).read(pos=0)
        expected = (msg[:21] + msg2 + msg[21 + len(msg2):]).encode()
        assert expected == got

    def test_empty_write(self):
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs)
        io.write(b'')
        io.write(b'test.')

        self.reopen_file()

        io = BlockStructureOrderedDataIO(self.f, BlockStructure(self.f))
        assert b"tes" == io.read(3)
        assert b't.' == io.read(10)


    def test_multiple_read(self):
        msg = "A very very very very long string for no reason."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs)

        io.write(string_to_bytes(msg))
        self.f.flush()

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        io2 = BlockStructureOrderedDataIO(self.f, bs2)
        io2.seek(0)
        got = io2.read(3) + io2.read(6) + io2.read()
        expected = msg.encode()
        assert expected == got

    def test_multiple_write(self):
        msg = "A very very very very long string for no reason."
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs, blocksize=16)

        io.write(string_to_bytes(msg[:5]))
        io.write(string_to_bytes(msg[5:12]))
        io.write(string_to_bytes(msg[12:14]))
        io.write(string_to_bytes(msg[14:134]))
        self.f.flush()

        self.reopen_file()
        bs2 = BlockStructure(self.f)
        io2 = BlockStructureOrderedDataIO(self.f, bs2)
        io2.seek(0)
        got = io2.read(3) + io2.read(6) + io2.read()
        expected = msg.encode()
        assert expected == got

    def test_size(self):
        msg = "A short string"
        bs = BlockStructure(self.f, block_size=16, initialize=True)
        io = BlockStructureOrderedDataIO(self.f, bs, blocksize=16)

        io.write(string_to_bytes(msg))

        assert io.size() == len(msg)

        msg2 = "Something random, really."
        io.seek(6)
        io.write(string_to_bytes(msg2))

        assert io.size() == len(msg[:6] + msg2)

        io.seek(10)
        io.write(string_to_bytes("small string"))

        assert io.size() == len(msg[:6] + msg2)


        msg3 = "tail."
        io.seek(io.size())
        io.write(string_to_bytes(msg3))

        assert io.size() == len(msg[:6] + msg2 + msg3)
