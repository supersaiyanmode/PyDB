import pytest

from PyDB.datatypes import IntegerType
from PyDB.datatypes import StringType
from PyDB.exceptions import PyDBTypeError, PyDBValueError
from PyDB.utils import int_to_bytes, bytes_to_int


class TestGenericType(object):
    def test_check_value(self):
        it = IntegerType()
        it.check_value(5)
        with pytest.raises(PyDBTypeError):
            it.check_value(None)
        with pytest.raises(PyDBTypeError):
            it.check_value("")
        with pytest.raises(PyDBTypeError):
            it.check_value(TestGenericType())

    def test_get_type(self):
        assert IntegerType().get_type() == int

    def test_has_default(self):
        it = IntegerType(default=0)
        assert it.has_default() == True

        with pytest.raises(PyDBTypeError):
            IntegerType(default="")

        with pytest.raises(PyDBTypeError):
            IntegerType(default=None)

    def test_preprocess_value(self):
        it = IntegerType(default=5, required=True)

        assert it.preprocess_value(None) == 5
        assert it.preprocess_value(2) == 2

        it2 = IntegerType(required=True)

        assert it2.preprocess_value(None) is None

    def test_check_required(self):
        it = IntegerType(required=True)
        with pytest.raises(PyDBValueError):
            it.check_required(None)

        IntegerType().check_required(None)

    def test_get_header(self):
        assert IntegerType().get_header(0).null == 0
        assert IntegerType().get_header(None).null == 1

    def test_encode_header(self):
        assert IntegerType().encode_header(0) == int_to_bytes(0, 1)
        assert IntegerType().encode_header(None) == int_to_bytes(1, 1)

