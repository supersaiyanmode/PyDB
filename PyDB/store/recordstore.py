import os.path
import struct

from PyDB.datatypes import GenericType
from PyDB.utils import custom_import, get_qualified_name
from PyDB.exceptions import PyDBMetadataError

class TableMetadata(object):
    def __init__(self, class_name, column_names, column_types, column_sizes,
            row_count, primary_key, unique_keys):
        self.class_name = class_name
        self.cls = custom_import(class_name)
        self.column_names = column_names
        self.column_types = column_types
        self.column_sizes = column_sizes
        self.primary_key = primary_key
        self.unique_keys = unique_keys
        self.row_count = row_count
        self.check_valid()

    def check_valid(self):
        if not (len(self.column_names) == len(self.column_types) ==
                len(self.column_sizes)):
            raise PyDBMetadataError("Lengths of column types/names/sizes" +
                                                        " must be equal")
        if self.primary_key is None or \
                self.primary_key not in self.column_names:
            raise PyDBMetadataError("Primary Key can not be located in metadata.")
        
        if any(x not in self.column_names for x in self.unique_keys):
            raise PyDBMetadataError("One or more unique keys can not be located" +
                                " in the metadata.")


class TableStore(object):
    pass


class FileTableStore(TableStore):
    def __init__(self, path, cls):
        self.columns = self.get_columns(cls)
        if os.path.isfile(path):
            self.file = open(path, "r+b")
        else:
            self.file = open(path, "w+b")
            self.init_structure()

    def get_columns(self, cls):
        cols = []
        for x in dir(cls):
            typ = getattr(cls, x)
            if isinstance(typ, GenericType):
                cols.append((x, typ))
        return cols

    def init_structure(self):
        pass

    def get_metadata_from_class(self, cls):
        class_name = get_qualified_name(cls)
        column_names = [x[0] for x in self.columns]
        column_types = [x[1] for x in self.columns]
        column_sizes = [x.get_binary_size() for x in column_types]
        primary_key = ([x for x,y in zip(column_names, column_types) 
                                    if y.primary_key] + [None])[0]
        unique_keys = [x for x,y in zip(column_names, column_types) if y.unique]
        column_types = [get_qualified_name(x.__class__) for x in column_types]

        return TableMetadata(class_name, column_names, column_types, column_sizes, 
                    0, primary_key, unique_keys)

    def get_metadata_from_file(self):
        return TableMetadata(**self.read_file_headers())

    def encode_metadata(self, md):
        #row_count|col_count|col_sizes|
        col_size = len(md.column_names)
        row_size = sum(md.column_sizes)
        res = struct.pack("<II", md.row_count, col_size)
        res += struct.pack("<{}I".format(col_size), *md.column_sizes)
        return res 
    
    def decode_metadata(self, arr):
        row_count, col_size = struct.unpack("<II", arr[:2*4])
        col_sizes = struct.unpack("<" + "I"*col_size, arr[2*4:])
        return row_count, col_size, col_sizes

    def read_file_metadata(self):
        self.file.seek(0)
        size = struct.unpack("<I", self.file.read(4))
        return self.file.read(size).strip(b'\x00')
        

    def write_file_metadata(self, arr):
        size = 1024
        pad = size - len(arr)
        return struct.pack("<I", 1024) + res + b'\x00'*pad

