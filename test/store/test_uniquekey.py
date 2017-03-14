import pytest

from PyDB.store import SmallUniqueKeyStore
from PyDB.datatypes import IntegerType, StringType
from PyDB.utils import bytes_to_int
from PyDB.exceptions import PyDBUniqueKeyViolation

from ..base import BlockStructureBasedTest


class TestTableMetadata(BlockStructureBasedTest):
    def test_read_write_header(self):
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in range(10):
            store1.insert(x)

        assert store1.count == 10

        self.reopen_file()

        store2 = SmallUniqueKeyStore(self.io, IntegerType())
        assert store2.count == 10

    def test_sorted_data(self):
        arr = [43, 23, 12, 22, -10, 48, 0, 99, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in arr:
            store1.insert(x)

        assert store1.count == 10
        assert store1.get_data() == sorted(arr)

        self.reopen_file()

        store2 = SmallUniqueKeyStore(self.io, IntegerType())
        assert store2.get_data() == sorted(arr)
        assert store2.count == 10

    def test_unique_violation(self):
        arr = [43, 23, 12, 22, -10, 47, 0, 47, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        with pytest.raises(PyDBUniqueKeyViolation) as ex:
            for x in arr:
                store1.insert(x)

        assert ex.value.message == "Unique key violation: 47"
