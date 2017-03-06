import pytest

from PyDB.exceptions import PyDBInternalError
from PyDB.safe_reader import SafeReader
from base import BlockStructureBasedTest

class DummyReader(object):
    def __init__(self, val):
        self.val = val

    def read(self, size):
        res = self.val[:size]
        self.val = self.val[size:]
        return res


class TestSafeReader(BlockStructureBasedTest):
    def test_next_int(self):
        io = DummyReader(b'\xBC\x19\x1C\x05')
        reader = SafeReader(io)

        assert reader.next_int() == -1139205115

    def test_next_string(self):
        io = DummyReader(b'\x00\x00\x00\x0Atesttest..')
        reader = SafeReader(io)

        assert reader.next_string(10) == 'testtest..'
    
    def test_bigger_len_in_header(self):
        io = DummyReader(b'\x00\x00\x10\x0Atesttest..')
        reader = SafeReader(io)

        with pytest.raises(PyDBInternalError):
            reader.next_string(10)

    def test_less_chars(self):
        io = DummyReader(b'\x00\x00\x00\x0Atesttest')
        reader = SafeReader(io)

        with pytest.raises(PyDBInternalError):
            reader.next_string(10)


