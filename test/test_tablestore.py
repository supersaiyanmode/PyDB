import os
import struct

import pytest

from PyDB.datatypes import GenericType, IntegerType, StringType
from PyDB.exceptions import PyDBMetadataError
from PyDB.store.tablestore import TableMetadata, TableStore
from base import BlockStructureBasedTest

class TempTable(TableStore):
    record_no = IntegerType(primary_key=True)
    first_name = StringType(50)
    last_name = StringType(50)
    ssn = IntegerType(unique=True)

    dummy_val = 2


class TestTableMetadata(BlockStructureBasedTest):
    def test_encode_decode(self):
        m1 = TableMetadata(TempTable)
        m1.encode_metadata(self.io)

        self.reopen_file()

        self.io.seek(0)
        m2 = TableMetadata(TempTable)
        m2.decode_metadata(self.io)

        assert m2.class_name == m1.class_name == 'test_metadata.TempTable'
        assert m2.column_names == m1.column_names == [
            'first_name', 'last_name', 'record_no', 'ssn'
        ]
        assert m2.primary_key == m1.primary_key == 'record_no'
        assert m2.unique_keys == m1.unique_keys == ['ssn']
        assert m2.row_count == m1.row_count == 0

