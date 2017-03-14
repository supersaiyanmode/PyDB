from PyDB.datatypes import GenericType
from PyDB.exceptions import PyDBInternalError, PyDBValueError
from .tablemetadata import TableMetadata

class Record(object):
    def __init__(self, **kwargs):
        self._values = {}
        self._metadata = TableMetadata(self.__class__)
        self._values = self._extract_column_values(**kwargs)

    def __getattribute__(self, key):
        self_values = object.__getattribute__(self, '_values')
        if key in self_values:
            return self_values[key]
        return super().__getattribute__(key)

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self._values == other._values

    def __repr__(self):
        params = ", ".join("{}={}".format(x, repr(y)) for x, y in self._values.items())
        return "{}({})".format(self.__class__.__name__, params)

    def _extract_column_values(self, **kwargs):
        expected_attrs = set(self._metadata.column_names)
        got_attrs = set(kwargs)

        extra_attrs = got_attrs - expected_attrs
        if extra_attrs:
            raise PyDBValueError("Unexpected attributes: {}.".format(extra_attrs))

        res = {}
        for attr_name, attr_type in self._metadata.columns:
            if attr_name in kwargs:
                res[attr_name] = kwargs[attr_name]
        return res

    def _check_values(self, values):
        for attr_name, attr_type in self._metadata.columns:
            attr_type.check_value(values.get(attr_name))

    def _encode_obj(self, io, pos=-1):
        self._check_values(self._values)

        if pos >= 0:
            io.seek(pos)

        for col_name, col_type in self._metadata.columns:
            io.write(col_type.encode(self._values[col_name]))

    def _decode_obj(self, io, pos=-1):
        if pos >= 0:
            io.seek(pos)

        res = {}
        for col_name, col_type in self._metadata.columns:
            res[col_name] = col_type.decode(io.iterdata())

        self._check_values(res)
        self._values = res

    @classmethod
    def _get_columns(cls):
        cols = []
        for x in dir(cls):
            typ = getattr(cls, x)
            if isinstance(typ, GenericType):
                cols.append((x, typ))
        return cols

class RecordStore(object):
    def __init__(self, io, cls):
        self.io = io
        self.cls = cls

    def get_record(self, pos):
        res = self.cls()
        res._decode_obj(self.io, pos=pos)
        return res

    def add_record(self, obj):
        self.io.seek(self.io.size())
        obj._encode_obj(self.io)


