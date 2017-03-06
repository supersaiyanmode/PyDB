import os.path
import os
import struct
from itertools import islice

from PyDB.datatypes import GenericType
from PyDB.utils import custom_import, get_qualified_name
from PyDB.utils import int_to_bytes, bytes_to_int, bytes_to_ints
from PyDB.utils import string_to_bytes, bytes_to_string
from PyDB.safe_reader import SafeReader
from PyDB.exceptions import PyDBMetadataError, PyDBConsistencyError
from PyDB.exceptions import PyDBInternalError


class TableMetadata(object):
    MAGIC_VALUE = 2123427274
    MAGIC_BYTES = int_to_bytes(MAGIC_VALUE, 4)

    INT_BYTE_LEN = 4
    COLUMN_BYTE_LEN = 128
    CLASS_BYTE_LEN = 512

    def __init__(self, class_name, column_names, row_count,
            primary_key, unique_keys):
        self.class_name = class_name
        self.cls = custom_import(class_name)
        self.column_names = column_names
        self.primary_key = primary_key
        self.unique_keys = unique_keys
        self.row_count = row_count
        self.check_valid()

    def check_valid(self):
        if self.primary_key is None or self.primary_key not in self.column_names:
            raise PyDBMetadataError("Primary Key can not be located in metadata.")

        if any(x not in self.column_names for x in self.unique_keys):
            raise PyDBMetadataError("One or more unique keys can not be located" +
                                " in the metadata.")

    @classmethod
    def from_class(cls, clsObj):
        def get_columns():
                cols = []
                for x in dir(clsObj):
                    typ = getattr(clsObj, x)
                    if isinstance(typ, GenericType):
                        cols.append((x, typ))
                return cols

        class_name = get_qualified_name(clsObj)
        columns = get_columns()
        column_names = [x[0] for x in columns]
        primary_key = ([x for x,y in columns if y.primary_key] + [None])[0]
        unique_keys = [x for x,y in columns if y.unique]
        column_types = [get_qualified_name(x[1].__class__) for x in columns]

        return TableMetadata(class_name, column_names, 0, primary_key, unique_keys)

    @classmethod
    def decode(cls, io):
        reader = SafeReader(io)

        magic = reader.next_int(cls.INT_BYTE_LEN)

        if magic != cls.MAGIC_VALUE:
            raise PyDBInternalError("Invalid metatadata.")

        class_name = reader.next_string(cls.CLASS_BYTE_LEN)
        size_cols = reader.next_int(cls.INT_BYTE_LEN)

        col_names = []
        for _ in range(size_cols):
            col_name = reader.next_string(cls.COLUMN_BYTE_LEN)
            col_names.append(col_name)

        primary_key = reader.next_string(cls.COLUMN_BYTE_LEN)

        unique_keys_count = reader.next_int(cls.INT_BYTE_LEN)
        unique_keys = []
        for _ in range(unique_keys_count):
            unique_key = reader.next_string(cls.COLUMN_BYTE_LEN)
            unique_keys.append(unique_key)

        row_count = reader.next_int(cls.INT_BYTE_LEN)

        return cls(class_name, col_names, row_count, primary_key, unique_keys)

    def encode(self, io, pos=0):
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

