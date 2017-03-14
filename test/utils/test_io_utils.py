from PyDB.utils import int_to_bytes, bytes_to_int, bytes_to_ints
from PyDB.utils import byte_chunker, bytes_to_gen, gen_to_bytes
from PyDB.utils import bytes_to_string, string_to_bytes
from PyDB.utils import SafeReader
from PyDB.exceptions import PyDBInternalError
from ..base import BlockStructureBasedTest

import pytest

def test_int_to_bytes():
    assert int_to_bytes(2989135076627009422, size=8) == b'){\x8a\x83\xdc\x03O\x8e'

def test_bytes_to_int():
    assert bytes_to_int(b'){\x8a\x83\xdc\x03O\x8e') == 2989135076627009422

def test_bytes_to_ints():
    assert bytes_to_ints(b'){\x8a\x83\xdc\x03O\x8e') == [695962243, -603762802]

def test_string_to_bytes():
    assert string_to_bytes("testing ...") == b'testing ...'

def test_bytes_to_string():
    assert bytes_to_string(b'testing ...') == 'testing ...'

def test_bytes_to_gen():
    msg = b'\x02\xFA\x34\xA0\x9B\x78\x09\x88\x1C'
    arr = bytes_to_gen(msg)
    assert b''.join(arr) == msg

def test_gen_to_bytes():
    msg = b'\x02\xFA\x34\xA0\x9B\x78\x09\x88\x1C'
    arr = bytes_to_gen(msg)
    assert gen_to_bytes(arr) == msg


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

