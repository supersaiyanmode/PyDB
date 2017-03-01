from PyDB.utils import custom_import
from PyDB.utils import get_qualified_name
from PyDB.utils import int_to_bytes, bytes_to_int, bytes_to_ints
from PyDB.utils import byte_chunker, bytes_to_gen

import pytest

class Test:
    pass

def test_custom_import1():
    c = custom_import('test_utils.Test')
    assert c == Test


def test_custom_import3():
    with pytest.raises(NotImplementedError):
        c = custom_import('JustAClass')

def test_get_qualified_name():
    assert get_qualified_name(Test) == 'test_utils.Test'

def test_int_to_bytes():
    assert int_to_bytes(2989135076627009422, size=8) == b'){\x8a\x83\xdc\x03O\x8e'

def test_bytes_to_int():
    assert bytes_to_int(b'){\x8a\x83\xdc\x03O\x8e') == 2989135076627009422

def test_bytes_to_ints():
    assert bytes_to_ints(b'){\x8a\x83\xdc\x03O\x8e') == [695962243, -603762802]

def test_chunker1():
    msg = b'\x02\xFA\x34\xA0\x9B\x78\x09\x88\x1C'
    arr = bytes_to_gen(msg)
    assert list(byte_chunker(arr, 3)) == [msg[:3], msg[3:6], msg[6:]]

def test_bytes_to_gen():
    msg = b'\x02\xFA\x34\xA0\x9B\x78\x09\x88\x1C'
    arr = bytes_to_gen(msg)
    assert b''.join(arr) == msg
