import os
from itertools import takewhile

from PyDB.exceptions import PyDBOutOfSpaceError, PyDBIterationError
from PyDB.exceptions import PyDBInternalError
from PyDB.utils import int_to_bytes, bytes_to_int, byte_chunker


class BlockStructureOrderedDataIO(object):
    def __init__(self, block_structure, blocksize=1024):
        self.block_structure = block_structure
        self.blocksize = blocksize

    def append_data(self, fh, data):
        last_block = self.block_structure.blocks[-1]

        data_pos = 0
        data_left = len(data)
        while data_left:
            can_fit = last_block.size - last_block.next_empty

            if not can_fit:
               last_block = self.block_structure.add_block(fh, self.blocksize)
               can_fit = last_block.size - last_block.next_empty

            cur_size = min(can_fit, data_left)
            cur_data = data[data_pos:data_pos + cur_size]
            last_block.write_data(fh, last_block.next_empty, cur_data)

            last_block.next_empty += cur_size
            last_block.write_header(fh)
            data_left -= cur_size
            data_pos += cur_size

    def iterdata(self, fh, offset=0, chunk_size=1):
        #Seek to correct block
        for cur_block in self.block_structure.blocks:
            if offset < cur_block.size:
                break
        else:
            raise PyDBIterationError("Invalid offset.")

        ptr = cur_block.start + cur_block.get_header_size() + offset
        byte_data = self.iterbytes(fh, cur_block, ptr)
        for b in byte_chunker(byte_data, chunk_size=chunk_size):
            yield b

    def iterbytes(self, fh, block, ptr):
        fh.seek(ptr)
        while True:
            start = block.start + block.get_header_size()
            if start <= ptr < start + block.next_empty:
                yield fh.read(1)
                ptr += 1
            else:
                if block.next == -1:
                    break

                block = Block.read_block(fh, block.next)
                ptr = block.start + block.get_header_size()


class Block(object):
    """
    | MAGIC | SIZE | NEXT | PREV | NEXT_EMPTY | DATA... |
    """

    MAGIC_VALUE = -1208913507
    MAGIC_BYTES = int_to_bytes(MAGIC_VALUE, 4)
    SIZE_MAGIC = 4
    SIZE_SIZE = 4
    SIZE_NEXT = 4
    SIZE_PREV = 4
    SIZE_NEXT_EMPTY = 4

    SIZE_HEADER = SIZE_MAGIC + SIZE_SIZE + SIZE_NEXT + SIZE_PREV + SIZE_NEXT_EMPTY

    def __init__(self, start, size, nxt, prev, next_empty=0):
        self.start = start
        self.size = size
        self.next = nxt
        self.prev = prev
        self.next_empty = next_empty

    def get_header_size(self):
        return self.SIZE_HEADER

    def write_header(self, fh):
        fh.seek(self.start)
        fh.write(self.MAGIC_BYTES)
        fh.write(int_to_bytes(self.size, self.SIZE_SIZE))
        fh.write(int_to_bytes(self.next, self.SIZE_NEXT))
        fh.write(int_to_bytes(self.prev, self.SIZE_PREV))
        fh.write(int_to_bytes(self.next_empty, self.SIZE_NEXT_EMPTY))

    def fill_data(self, fh, data):
        if self.size % len(data) != 0:
            raise PyDBInternalError("Can't fill data. Not aligned.")
        fh.seek(self.start + self.get_header_size())
        for _ in range(self.size // len(data)):
            fh.write(data)

    def write_data(self, fh, position, data):
        """
        Writes data at the `position`, but doesn't update self.next_empty.
        """
        if position < 0 or position + len(data) > self.size:
            raise PyDBInternalError("Invalid position to write in.")
        fh.seek(self.start + self.get_header_size() + position)
        fh.write(data)

    def __repr__(self):
        return ("Block(start={s.start}, size={s.size}, nxt={s.next}, "
                "prev={s.prev})").format(s=self)

    @classmethod
    def read_block(cls, fh, start):
        fh.seek(start)
        magic = fh.read(cls.SIZE_MAGIC)
        if magic != cls.MAGIC_BYTES:
            raise PyDBInternalError("Not a block at start position: {}.".format(start))

        size = bytes_to_int(fh.read(cls.SIZE_SIZE))
        nxt = bytes_to_int(fh.read(cls.SIZE_NEXT))
        prev = bytes_to_int(fh.read(cls.SIZE_PREV))
        next_empty = bytes_to_int(fh.read(cls.SIZE_NEXT_EMPTY))
        return cls(start, size, nxt, prev, next_empty=next_empty)

class BlockStructure(object):
    def __init__(self, fh, position=0, block_size=1024, initialize=False, fill=None):
        if initialize:
            self.blocks = self.init_structure(fh, position, block_size, fill=fill)
        else:
            self.blocks = self.read_structure(fh, position)

    def init_structure(self, fh, position, block_size, fill=None):
        if fill is None:
            fill = int_to_bytes(-1, 4)

        block = Block(position, block_size, -1, -1)
        self.write_new_block(fh, block, fill)
        return [block]

    def read_structure(self, fh, position):
        blocks = []
        cur = position
        while cur !=  -1:
            block = Block.read_block(fh, cur)
            blocks.append(block)
            cur = block.next

        return blocks

    def add_block(self, fh, block_size, after=None, fill=None):
        if fill is None:
            fill = int_to_bytes(-1, 4)

        if after is None:
            after = self.blocks[-1]
        prior_block = after
        next_block = Block.read_block(fh, after.next) if after.next != -1 else None
        prior_block_pos = after.start
        next_block_pos = after.next

        pos = fh.seek(0, os.SEEK_END)
        block = Block(pos, block_size, next_block_pos, prior_block_pos)
        pos = self.write_new_block(fh, block, fill=fill)

        prior_block.next = pos
        prior_block.write_header(fh)
        block.write_header(fh)
        if next_block is not None:
            next_block.prev = block.start
            next_block.write_header(fh)

        self.blocks.append(block)
        fh.flush()
        return block

    def write_new_block(self, fh, block, fill):
        pos = fh.seek(block.start)
        block.write_header(fh)
        block.fill_data(fh, fill)
        fh.flush()
        return pos

class MultiBlockStructure(object):
    def __init__(self, fh, block_size=1024, initialize=False, fill=None):
       self.header_structure = BlockStructure(fh, block_size=block_size,
               initialize=initialize, fill=fill)
       self.header = BlockStructureOrderedDataIO(self.header_structure)
       self.super_blocks = self.read_structure(fh, self.header_structure)

    def read_structure(self, fh, header_structure):
        it = self.header.iterdata(fh, chunk_size=4)
        it = ((a, b, bytes_to_int(c)) for a, b, c in it)
        super_blocks_pos = [x[2] for x in takewhile(lambda x: x[2] != -1, it)]
        return [BlockStructure(fh, x) for x in super_blocks_pos]

    def add_structure(self, fh, block_size, fill=None):
        pos = fh.seek(0, os.SEEK_END)
        block_structure = BlockStructure(fh, position=pos, initialize=True,
                block_size=block_size, fill=fill)
        self.header.append_data(fh, int_to_bytes(pos))
        fh.flush()
        return block_structure

