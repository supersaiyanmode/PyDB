import os

from PyDB.structure.blocks import BlockStructureOrderedDataIO, BlockStructure

class FileBasedTest(object):
    file_path = "/tmp/block.test"

    def setup(self):
        self.f = open(self.file_path, "wb+")

    def teardown(self):
        self.f.close()
        os.unlink(self.file_path)

    def reopen_file(self):
        self.f.close()
        self.f = open(self.file_path, "rb+")


class BlockStructureBasedTest(FileBasedTest):
    def setup(self):
        super().setup()
        bs = BlockStructure(self.f, block_size=128, initialize=True)
        self.io = BlockStructureOrderedDataIO(self.f, bs, blocksize=128)

    def teardown(self):
        super().teardown()

    def reopen_file(self):
        super().reopen_file()
        bs = BlockStructure(self.f, block_size=128)
        self.io = BlockStructureOrderedDataIO(self.f, bs, blocksize=128)

