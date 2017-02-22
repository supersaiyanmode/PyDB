import os
from itertools import takewhile

from PyDB.exceptions import PyDBOutOfSpaceError, PyDBIterationError
from PyDB.exceptions import PyDBInternalError
from PyDB.utils import int_to_bytes, bytes_to_int

class BlockDataIterator(object):
    def __init__(self, fh, block, chunksize=1):
        self.fh = fh
        self.block = block
        self.ptr = self.block.start
        if block.size % chunksize != 0:
            raise PyDBIterationError("Bad chunk size '{}' for block size '{}'".format(
                chunksize, self.block.size))
        self.chunksize = chunksize

    def __iter__(self):
        self.fh.seek(self.ptr)
        return self

    def __next__(self):
        if self.ptr < self.block.start + self.block.size:
            res = self.fh.read(self.chunksize)
            offset = self.ptr - self.block.start
            self.ptr += self.chunksize
            return self.ptr - self.start - self.chunksize, res
        raise StopIteration


class Block(object):
    """
    | SIZE | NEXT | PREV | TYPE | DATA... |
    """

    BLOCK_HEADER = 0
    BLOCK_DATA = 1

    SIZE_SIZE = 4
    SIZE_NEXT = 4
    SIZE_PREV = 4
    SIZE_TYPE = 4

    SIZE_HEADER = SIZE_SIZE + SIZE_NEXT + SIZE_PREV + SIZE_TYPE

    def __init__(self, start, size, nxt, prev, typ):
        self.start = start
        self.size = size
        self.next = nxt
        self.prev = prev
        self.type = typ

    def add_next(self, fh, block):
        self.next = block.start
        block.prev = self.start
        self.write_header(fh)

    def write_header(self, fh):
        fh.seek(self.start)
        fh.write(int_to_bytes(self.size, self.SIZE_SIZE))
        fh.write(int_to_bytes(self.next, self.SIZE_NEXT))
        fh.write(int_to_bytes(self.prev, self.SIZE_PREV))
        fh.write(int_to_bytes(self.type, self.SIZE_TYPE))

    def fill_data(self, fh, data):
        if self.size % len(data) != 0:
            raise PyDBInternalError("Can't fill data. Not aligned.")
        fh.seek(self.start + self.SIZE_HEADER)
        for _ in range(self.size // len(data)):
            fh.write(data)

    def write_data(self, fh, position, data):
        if position < 0 or position >= self.size:
            raise PyDBInternalError("Invalid position to write in.")
        fh.seek(self.start + self.SIZE_HEADER + position)
        fh.write(data)

    def __repr__(self):
        return ("Block(start={s.start}, size={s.size}, nxt={s.next}, "
                "prev={s.prev}, typ={s.type})").format(s=self)

    @staticmethod
    def read_block(fh, start=0):
        fh.seek(start)
        size = bytes_to_int(fh.read(SIZE_SIZE))
        nxt = bytes_to_int(fh.read(SIZE_NEXT))
        prev = bytes_to_int(fh.read(SIZE_PREV))
        typ = bytes_to_int(fh.read(SIZE_TYPE))

        if typ == self.BLOCK_HEADER:
            if size != HeaderBlock.HEADER_BLOCK_SIZE:
                raise PyDBInternalError("Incorrect Header block size.")
            return HeaderBlock(start, nxt, prev)
        if typ == self.BLOCK_DATA:
            return DataBlock(start, size, nxt, prev)

class DataBlock(Block):
    def __init__(self, start, size, nxt, prev):
        super().__init__(self, start, size, nxt, prev, self.BLOCK_DATA)

class HeaderBlock(Block):
    """
    | BLOCK1 | BLOCK2 | BLOCK3 | .. |
    """

    HEADER_BLOCK_SIZE = 1024

    def __init__(self, start, nxt, prev):
        size = self.HEADER_BLOCK_SIZE
        super().__init__(start, size, nxt, prev, self.BLOCK_HEADER)

    def get_next_available(self, fh):
        for offset, block in BlockDataIterator(fh, self, 4):
            block_ptr = bytes_to_int(block)
            if block_ptr == NULL:
                return offset
        raise PyDBOutOfSpaceError(self)

    def write_data(self, fh, block_ptr):
        start, _ = self.get_next(fh)
        fh.seek(start)
        fh.write(int_to_bytes(block_ptr, 4))

class BlockStructure(object):
    def __init__(self, fh, initialize=False):
        if initialize:
            self.header_blocks, self.data_blocks = self.init_structure(fh)
        else:
            self.header_blocks, self.blocks = self.read_structure(fh)

    def init_structure(self, fh):
        block = HeaderBlock(0, -1, -1)
        self.write_new_block(fh, block, fill=int_to_bytes(-1, 4))
        return [block], []

    def read_structure(self, fh):
        header_block = Block.read_block(fh)
        if not isinstance(header_block, HeaderBlock):
            raise PyDBInternalError("Expected a header block.")
        header_blocks = []
        data_blocks = []
        cur = header_block
        while cur != NULL:
            header_block.append(cur)
            it = BlockDataIterator(fh, header_block, chunksize=4)
            data = [bytes_to_int(x[1]) for x in it]
            data_blocks += list(takewhile(lambda x: x != NULL, data))
            cur = Block.read_block(cur.next)

        return header_blocks, data_blocks

    def write_new_block(self, fh, block, fill='\xFF'):
        fh.seek(1, os.SEEK_END)
        block.write_header(fh)
        block.fill_data(fh, fill)
        fh.flush()

