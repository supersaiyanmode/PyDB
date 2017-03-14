import os
import struct

import pytest

from PyDB.datatypes import IntegerType, StringType
from PyDB.exceptions import PyDBMetadataError, PyDBValueError
from PyDB.exceptions import PyDBInternalError
from PyDB.store import TableMetadata, Record

from ..base import BlockStructureBasedTest

class TempTable(Record):
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

        assert m2.class_name == m1.class_name == 'test.store.test_tablemetadata.TempTable'
        assert m2.column_names == m1.column_names == [
            'age', 'first_name', 'last_name', 'record_no', 'ssn'
        ]
        assert m2.primary_key == m1.primary_key == 'record_no'
        assert m2.unique_keys == m1.unique_keys == ['ssn']
        assert m2.row_count == m1.row_count == 0

    def test_compability(self):
        prefix = "Internal Error. Possibly corrupt database. "
        m = TableMetadata(TempTable)
        with pytest.raises(PyDBMetadataError) as ex:
            m.check_compatibility("blah", None, None, None, None)
        assert ex.value.message == prefix + "Class name differs."

        with pytest.raises(PyDBMetadataError) as ex:
            cls = "test.store.test_tablemetadata.TempTable"
            cols = ["age", "first_name", "last_name", "record_no", "ssn", "extra"]
            m.check_compatibility(cls, cols, None, None, None)
        assert ex.value.message == prefix + "Column names differ."

    def test_bad_magic(self):
        m = TableMetadata(TempTable)
        self.io.write("some random message".encode())
        self.io.seek(0)
        with pytest.raises(PyDBInternalError) as ex:
            m.decode_metadata(self.io)
        expected = "Internal Error. Possibly corrupt database. Invalid metatadata."
        assert ex.value.message == expected
