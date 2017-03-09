import pytest

from PyDB.datatypes import IntegerType, StringType
from PyDB.datatypes import TypeHeader
from PyDB.exceptions import PyDBTypeError, PyDBValueError, PyDBTypeConstraintError
from PyDB.utils import int_to_bytes, bytes_to_int, bytes_to_gen


class TestTypeHeader(object):
    def test_encode_header(self):
        assert TypeHeader(True, 0).encode_header() == int_to_bytes(1, 1) + int_to_bytes(0, 4)
        assert TypeHeader(False, 5).encode_header() == int_to_bytes(0, 1) + int_to_bytes(5, 4)

    def test_decode_header(self):
        th = TypeHeader.decode_header(bytes_to_gen(b'\x00\x00\x00\x04\xB3'))
        assert th.null == 0
        assert th.size == 1203


class TestGenericType(object):
    def test_check_value(self):
        it = IntegerType(required=True)
        it.check_value(5)
        with pytest.raises(PyDBValueError) as ex:
            it.check_value(None)
        assert ex.value.message == "Value is NULL for a required attribute."

        with pytest.raises(PyDBTypeError) as ex:
            it.check_value("")
        assert ex.value.message == "Expected value of type int, but got an instance of type: str"

        with pytest.raises(PyDBTypeError) as ex:
            it.check_value(TestGenericType())
        expected = "Expected value of type int, but got an instance of type: TestGenericType"
        assert ex.value.message == expected

    def test_get_type(self):
        assert IntegerType().get_type() == int

    def test_has_default(self):
        it = IntegerType(default=0)
        assert it.has_default() == True

        with pytest.raises(PyDBTypeError) as ex:
            IntegerType(default="")
        assert ex.value.message == "Expected value of type int, but got an instance of type: str"

        with pytest.raises(PyDBValueError) as ex:
            IntegerType(required=True, default=None)
        assert ex.value.message == "Value is NULL for a required attribute."

    def test_preprocess_value(self):
        it = IntegerType(default=5, required=True)

        assert it.preprocess_value(None) == 5
        assert it.preprocess_value(2) == 2

        it2 = IntegerType(required=True)

        assert it2.preprocess_value(None) is None

    def test_check_required(self):
        it = IntegerType(required=True)
        with pytest.raises(PyDBValueError) as ex:
            it.check_required(None)
        assert ex.value.message == "Value is NULL for a required attribute."

        IntegerType().check_required(None)

    def test_get_header(self):
        assert IntegerType().get_header(0).null == 0
        assert IntegerType().get_header(None).null == 1


class TestIntegerType(object):
    def test_encode(self):
        res = IntegerType().encode(56)
        assert b'\x00\x00\x00\x00\x04\x00\x00\x008' == res

    def test_encode_null(self):
        res = IntegerType(required=True).encode(None)
        assert b'\x01\x00\x00\x00\x00' == res

    def test_decode(self):
        barr = bytes_to_gen(b'\x00\x00\x00\x00\x04\x00\x00\x04\x84')
        assert IntegerType(required=True).decode(barr) == 1156

    def test_decode_null(self):
        barr = bytes_to_gen(b'\x01\x00\x00\x00\x00')
        assert IntegerType().decode(barr) is None


class TestStringType(object):
    def test_encode(self):
        res = StringType(16).encode("test everything.")
        assert b'\x00\x00\x00\x00\x10test everything.' == res

    def test_encode_null(self):
        res = StringType(16).encode(None)
        assert b'\x01\x00\x00\x00\x00' == res

    def test_decode(self):
        barr = bytes_to_gen(b'\x00\x00\x00\x00\x09test func')
        assert StringType(16).decode(barr) == 'test func'

    def test_decode_null(self):
        barr = bytes_to_gen(b'\x01\x00\x00\x00\x00')
        assert StringType(16).decode(barr) is None

    def test_constraint(self):
        with pytest.raises(PyDBTypeConstraintError) as ex:
            StringType(16).check_value("This is a rather long string.")
        assert ex.value.message == "Illegal value. Can't store value: String size exceeds 16"

