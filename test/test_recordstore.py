import os
import struct

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

        assert md.class_name == 'test_recordstore.TestTable'
        assert md.cls == TestTable
        assert md.column_names == ['first_name', 'last_name',
                'record_no', 'ssn']
        assert md.column_types == [
            'PyDB.datatypes.StringType', 'PyDB.datatypes.StringType',
            'PyDB.datatypes.IntegerType', 'PyDB.datatypes.IntegerType' ]
        assert md.column_sizes == [50, 50, 4, 4]
        assert md.primary_key == 'record_no'
        assert md.unique_keys == ['ssn']
        assert md.row_count == 0

    def test_encode(self):
        fts = FileTableStore(self.db_path, TestTable)
        md = fts.get_metadata_from_class(TestTable)
        got = fts.encode_metadata(md)
        res = struct.unpack("<6I", got)
        assert res == (0, 4, 50, 50, 4, 4)

    def test_decode(self):
        fts = FileTableStore(self.db_path, TestTable)
        md = fts.get_metadata_from_class(TestTable)
        got = fts.encode_metadata(md)
        res = fts.decode_metadata(got)
        assert res == (0, 4, (50, 50, 4, 4))

    def test_fileio(self):
        fts = FileTableStore(self.db_path, TestTable)
        m1 = fts.metadata
        fts.close()
        
        fts = FileTableStore(self.db_path, TestTable)
        m2 = fts.metadata
        fts.close()

        assert m1.class_name == m2.class_name
        assert m1.cls == m2.cls
        assert m1.column_names == m2.column_names
        assert m1.column_types == m2.column_types
        assert m1.column_sizes == m2.column_sizes
        assert m1.primary_key == m2.primary_key
        assert m1.unique_keys == m2.unique_keys
        assert m1.row_count == m2.row_count

