import os
from nose.utils import assert_equals

from PyDB.structure.blocks import BlockStructure, DataBlock, HeaderBlock

class TestBlockStructure(object):
    file_path = "/tmp/block.test"

    def setup(self):
        self.f = open(self.file_path, "wb")

    def teardown(self):
        self.f.close()
        os.unlink(self.file_path)

    def test_initialization(self):
        bs = BlockStructure(self.f)
        
