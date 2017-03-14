import pytest

from PyDB.exceptions import PyDBMetadataError, PyDBValueError
from PyDB.datatypes import IntegerType, StringType
from PyDB.exceptions import PyDBTypeError
from PyDB.store import Record

from ..base import BlockStructureBasedTest

class TempTable(Record):
    record_no = IntegerType(primary_key=True)
    first_name = StringType(50)
    last_name = StringType(50)
    ssn = IntegerType(unique=True)
    age = IntegerType(required=True)

    dummy_val = 2


class TestRecord(BlockStructureBasedTest):
    def test_basic(self):
        obj = TempTable(record_no=4, first_name="x", last_name="y", ssn=124, age=10)

        assert obj.record_no == 4
        assert obj.first_name == "x"
        assert obj.last_name == "y"
        assert obj.ssn == 124
        assert obj.age == 10

    def test_repr(self):
        obj = TempTable(record_no=4, first_name="x", last_name="y", ssn=124, age=10)
        expected = 'TempTable(first_name="x", last_name="y", record_no=4, ssn=124, age=10)'
        assert len(repr(obj)) == len(expected) #Quick and dirty, w/o relying on dict-order

    def test_extract_unexpected_attributes(self):
        with pytest.raises(PyDBValueError) as ex:
            TempTable(blah="blah")._encode_obj(self.io)
        assert ex.value.message == "Unexpected attributes: {'blah'}."

    def test_no_primary_key(self):
        with pytest.raises(PyDBValueError) as ex:
            TempTable(first_name="x", last_name="y", ssn=124, age=10)._encode_obj(self.io)
        assert ex.value.message == "Value is NULL for a required attribute."

    def test_no_required_attr(self):
        with pytest.raises(PyDBValueError) as ex:
            TempTable(record_no=1, first_name="x", last_name="y")._encode_obj(self.io)
        assert ex.value.message == "Value is NULL for a required attribute."

    def test_bad_type(self):
        with pytest.raises(PyDBTypeError) as ex:
            t = TempTable(record_no=1, first_name="x", last_name="y", ssn="bad string", age=15)
            t._encode_obj(self.io)
        assert ex.value.message == "Expected value of type int, but got an instance of type: str"

    def test_read_write_objects(self):
        objs = [
            TempTable(record_no=1, first_name="f1", last_name="l1", ssn=1234, age=10),
            TempTable(record_no=2, first_name=None, last_name="l2", ssn=2345, age=12),
            TempTable(record_no=3, first_name="f3", last_name=None, ssn=None, age=15),
        ]

        for obj in objs:
            obj._encode_obj(self.io)

        self.reopen_file()

        got = [TempTable() for _ in range(3)]
        for t in got:
            t._decode_obj(self.io)
        assert objs == got

