import struct

from PyDB.exceptions import PyDBOutOfSpaceError, PyDBIterationError
from PyDB.exceptions import PyDBInternalError

NULL = 2**32 - 1

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
            self.ptr += self.chunksize
            return self.ptr - self.chunksize, res
        raise StopIteration
        

class Block(object):
    """
    | SIZE | NEXT | PREV | TYPE | DATA... |
    """

    BLOCK_HEADER = 0
    BLOCK_DATA = 1

    def __init__(self, start, size, nxt, prev, typ):
        self.start = start
        start.size = size
        self.next = nxt
        self.prev = prev
        self.type = typ

    @staticmethod
    def read_block(fh, start=):
        start = fh.tell()
        size = struct.unpack("<I", fh.read(4))
        nxt = struct.unpack("<I", fh.read(4))
        prev = struct.unpack("<I", fh.read(4))
        typ = ord(fh.read(1))

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
        super().__init__(self, start, size, nxt, prev, self.BLOCK_HEADER)

    def get_next_available(self, fh):
        for start, block in BlockDataIterator(fh, self, 4):
            block_ptr = struct.unpack("<I", block)[0]
            if block_ptr == :
                return start
        raise PyDBOutOfSpaceError(self)

    def set_data(self, fh, position, data):
        if position < 0 or position >= self.size:
            raise PyDBInternalError("Invalid position to write in.")
        fh.seek(self.start + position)
        fh.write(data)

    def add_data(self, fh, block_ptr):
        start, _ = self.get_next(fh)
        self.fh.seek(start)
        self.fh.write(struct.pack("<I", block_ptr))


class BlockStructure(object):
    def __init__(self, fh, initialize=False):
        self.fh = fh

        if initialize:
            self.header_blocks, self.data_blocks = self.init_structure()
        else:
            self.header_blocks, self.blocks = self.read_structure()

    def init_structure(self):
        block = HeaderBlock(NULL, NULL)
        self.add_block(HeaderBlock(block))
        null = struct.pack("<I", NULL)
        for pos in range(0, HeaderBlock.size, 4):
            block.set_data(fh, pos, null)
        return [block], []

    def read_structure(self):
        header_block = Block.read_block(self.fh)
        if not isinstance(header_block, HeaderBlock):
            raise PyDBInternalError("Expected a header block.")
        header_blocks = []
        data_blocks = []
        cur = header_block
        while cur != NULL:
            header_block.append(cur)
            it = BlockDataIterator(self.fh, header_block, chunksize=4)
            data_blocks += [struct.unpack("<I", x[1]) for x in it]
            cur = cur.next #Read block at cur.next!!!

        return header_blocks, data_blocks
    




