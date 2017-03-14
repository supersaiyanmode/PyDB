import pytest

from PyDB.store import SmallUniqueKeyStore
from PyDB.datatypes import IntegerType, StringType
from PyDB.utils import bytes_to_int
from PyDB.exceptions import PyDBUniqueKeyViolation
from PyDB.exceptions import PyDBKeyNotFoundError

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

    def test_remove_first_elem(self):
        arr = [43, 23, 12, 22, -10, 47, 0, 46, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in arr:
            store1.insert(x)

        store1.remove(-10)
        assert store1.get_data() == sorted(arr)[1:]

    def test_remove_last_elem(self):
        arr = [43, 23, 12, 22, -10, 47, 0, 46, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in arr:
            store1.insert(x)

        store1.remove(104)
        assert store1.get_data() == sorted(arr)[:-1]

    def test_remove_center_elem(self):
        arr = [43, 23, 12, 22, -10, 47, 0, 46, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in arr:
            store1.insert(x)

        store1.remove(47)
        assert store1.get_data() == [x for x in sorted(arr) if x != 47]

    def test_not_exist_smallest(self):
        arr = [43, 23, 12, 22, -10, 47, 0, 46, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in arr:
            store1.insert(x)

        with pytest.raises(PyDBKeyNotFoundError) as ex:
            store1.remove(-120)
        assert ex.value.message == "Not found: -120"

    def test_not_exist_largest(self):
        arr = [43, 23, 12, 22, -10, 47, 0, 46, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in arr:
            store1.insert(x)

        with pytest.raises(PyDBKeyNotFoundError) as ex:
            store1.remove(120)
        assert ex.value.message == "Not found: 120"

    def test_not_exist_center(self):
        arr = [43, 23, 12, 22, -10, 47, 0, 46, 104, 65]
        store1 = SmallUniqueKeyStore(self.io, IntegerType(), initialize=True)
        for x in arr:
            store1.insert(x)

        with pytest.raises(PyDBKeyNotFoundError) as ex:
            store1.remove(51)
        assert ex.value.message == "Not found: 51"

