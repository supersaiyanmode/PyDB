import os
import struct

from nose.tools import assert_equals

from PyDB.datatypes import GenericType, IntegerType, StringType
from PyDB.store.recordstore import FileTableStore



class TestTable(object):
    record_no = IntegerType(primary_key=True)
    first_name = StringType(50)
    last_name = StringType(50)
    ssn = IntegerType(unique=True)


class TestFileTableStore(object):
    db_path = "/tmp/table.test"

    def setup(self):
        if os.path.isfile(self.db_path):
            os.unlink(self.db_path)

    def teardown(self):
        os.unlink(self.db_path)

    def test_basic_metadata(self):
        fts = FileTableStore(self.db_path, TestTable)
        md = fts.get_metadata_from_class(TestTable)

        assert_equals(md.class_name, 'test_recordstore.TestTable')
        assert_equals(md.cls, TestTable)
        assert_equals(md.column_names, ['first_name', 'last_name', 
                'record_no', 'ssn'])
        assert_equals(md.column_types, [
            'PyDB.datatypes.StringType', 'PyDB.datatypes.StringType',
            'PyDB.datatypes.IntegerType', 'PyDB.datatypes.IntegerType' ])
        assert_equals(md.column_sizes, [50, 50, 4, 4])
        assert_equals(md.primary_key, 'record_no')
        assert_equals(md.unique_keys, ['ssn'])
        assert_equals(md.row_count, 0)

    def test_encode(self):
        fts = FileTableStore(self.db_path, TestTable)
        md = fts.get_metadata_from_class(TestTable)
        got = fts.encode_metadata(md)
        res = struct.unpack("<6I", got)
        assert_equals(res, (0, 4, 50, 50, 4, 4))

    def test_decode(self):
        fts = FileTableStore(self.db_path, TestTable)
        md = fts.get_metadata_from_class(TestTable)
        got = fts.encode_metadata(md)
        res = fts.decode_metadata(got)
        assert_equals(res, (0, 4, (50, 50, 4, 4)))



        
