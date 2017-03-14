from PyDB.utils import byte_chunker
from PyDB.utils import bytes_to_gen


def test_chunker1():
    msg = b'\x02\xFA\x34\xA0\x9B\x78\x09\x88\x1C'
    arr = bytes_to_gen(msg)
    assert list(byte_chunker(arr, 3)) == [msg[:3], msg[3:6], msg[6:]]

def test_chunker2():
    msg = b'\x02\xFA\x34\xA0\x9B\x78\x09\x88'
    arr = bytes_to_gen(msg)
    assert list(byte_chunker(arr, 3)) == [msg[:3], msg[3:6], msg[6:]]


