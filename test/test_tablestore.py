import os
import struct

import pytest

from PyDB.datatypes import GenericType, IntegerType, StringType
from PyDB.exceptions import PyDBMetadataError, PyDBValueError
from PyDB.exceptions import PyDBTypeError
from PyDB.store.tablestore import TableMetadata, TableStore
from base import BlockStructureBasedTest

class TempTable(TableStore):
    record_no = IntegerType(primary_key=True)
    first_name = StringType(50)
    last_name = StringType(50)
    ssn = IntegerType(unique=True)
    age = IntegerType(required=True)

    dummy_val = 2


class TestTableMetadata(BlockStructureBasedTest):
    def test_encode_decode(self):
        m1 = TableMetadata(TempTable)
        m1.encode_metadata(self.io)

        self.reopen_file()

        self.io.seek(0)
        m2 = TableMetadata(TempTable)
        m2.decode_metadata(self.io)

        assert m2.class_name == m1.class_name == 'test_tablestore.TempTable'
        assert m2.column_names == m1.column_names == [
            'age', 'first_name', 'last_name', 'record_no', 'ssn'
        ]
        assert m2.primary_key == m1.primary_key == 'record_no'
        assert m2.unique_keys == m1.unique_keys == ['ssn']
        assert m2.row_count == m1.row_count == 0

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

