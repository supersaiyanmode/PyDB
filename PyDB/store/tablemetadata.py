import os.path
import os
import struct
from itertools import islice

from PyDB.datatypes import GenericType
from PyDB.utils import get_qualified_name
from PyDB.utils import int_to_bytes, bytes_to_int, bytes_to_ints
from PyDB.utils import string_to_bytes, bytes_to_string
from PyDB.utils import SafeReader
from PyDB.exceptions import PyDBMetadataError, PyDBConsistencyError
from PyDB.exceptions import PyDBInternalError, PyDBValueError


class TableMetadata(object):
    MAGIC_VALUE = 2123427274
    MAGIC_BYTES = int_to_bytes(MAGIC_VALUE, 4)

    INT_BYTE_LEN = 4
    COLUMN_BYTE_LEN = 128
    CLASS_BYTE_LEN = 512

    def __init__(self, cls):
        self.class_name = get_qualified_name(cls)
        self.columns = cls._get_columns()
        self.column_names = [x[0] for x in self.columns]
        self.primary_key = ([x for x,y in self.columns if y.primary_key] + [None])[0]
        self.unique_keys = [x for x,y in self.columns if y.unique]
        self.row_count = 0
        self.check_valid()

    def check_valid(self):
        pass

    def decode_metadata(self, io):
        reader = SafeReader(io)

        magic = reader.next_int(self.INT_BYTE_LEN)

        if magic != self.MAGIC_VALUE:
            raise PyDBInternalError("Invalid metatadata.")

        class_name = reader.next_string(self.CLASS_BYTE_LEN)
        size_cols = reader.next_int(self.INT_BYTE_LEN)

        col_names = []
        for _ in range(size_cols):
            col_name = reader.next_string(self.COLUMN_BYTE_LEN)
            col_names.append(col_name)

        primary_key = reader.next_string(self.COLUMN_BYTE_LEN) or None

        unique_keys_count = reader.next_int(self.INT_BYTE_LEN)
        unique_keys = []
        for _ in range(unique_keys_count):
            unique_key = reader.next_string(self.COLUMN_BYTE_LEN)
            unique_keys.append(unique_key)

        row_count = reader.next_int(self.INT_BYTE_LEN)

        self.check_compatibility(class_name, col_names, row_count, primary_key, unique_keys)

        #Updating attributes that might have changed.
        self.row_count = row_count

    def encode_metadata(self, io, pos=0):
        io.seek(pos)
        io.write(self.MAGIC_BYTES)

        io.write(int_to_bytes(len(self.class_name)))
        io.write(string_to_bytes(self.class_name))

        io.write(int_to_bytes(len(self.column_names)))

        for x in self.column_names:
            io.write(int_to_bytes(len(x)))
            io.write(string_to_bytes(x))

        io.write(int_to_bytes(len(self.primary_key)))
        io.write(string_to_bytes(self.primary_key))

        io.write(int_to_bytes(len(self.unique_keys)))

        for x in self.unique_keys:
            io.write(int_to_bytes(len(x)))
            io.write(string_to_bytes(x))

        io.write(int_to_bytes(self.row_count))

    def check_compatibility(self, class_name, col_names, row_count, primary_key, unique_keys):
        if class_name != self.class_name:
            raise PyDBMetadataError("Class name differs.")
        if col_names != self.column_names:
            raise PyDBMetadataError("Column names differ.")
        if primary_key != self.primary_key:
            raise PyDBMetadataError("Primary key differs.")
        if unique_keys != self.unique_keys:
            raise PyDBMetadataError("Unique keys differ.")

