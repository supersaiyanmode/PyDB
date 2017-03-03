from itertools import islice

from PyDB.utils import int_to_bytes, bytes_to_int

from PyDB.exceptions import PyDBTypeError, PyDBValueError
from PyDB.exceptions import PyDBTypeConstraintError

class DefaultDummyType:
    pass

DEFAULT_VALUE = DefaultDummyType()

class TypeHeader(object):
    SIZE_NULL = 1
    SIZE_SIZE = 4

    SIZE_TOTAL = SIZE_NULL + SIZE_SIZE

    def __init__(self, null, size):
        self.null = int(null)
        self.size = size

    def encode_header(self):
        res = int_to_bytes(self.null, self.SIZE_NULL)
        res += int_to_bytes(self.size, self.SIZE_SIZE)
        return res

    @classmethod
    def decode_header(cls, barr):
        arr = b''.join(islice(barr, 0, TypeHeader.SIZE_TOTAL))
        null = bytes_to_int(arr[0:cls.SIZE_NULL])
        size = bytes_to_int(arr[cls.SIZE_NULL: cls.SIZE_NULL + cls.SIZE_SIZE])
        return TypeHeader(null, size)


class GenericType(object):
    def __init__(self, primary_key=False, unique=False, required=False,
            default=DEFAULT_VALUE):
        self.primary_key = primary_key
        self.unique = unique
        self.required = required
        if default is DEFAULT_VALUE:
            self.default = DEFAULT_VALUE
        else:
            self.check_value(default)
            self.default = default

    def has_default(self):
        return self.default is not DEFAULT_VALUE

    def preprocess_value(self, val):
        if val is None and self.has_default():
            return self.default
        return val

    def check_value(self, val):
        self.check_required(val)
        self.check_type(val)
        self.check_constraints(val)

    def check_type(self, val):
        if not isinstance(val, self.get_type()):
            raise PyDBTypeError(self.get_type(), val)

    def check_required(self, val):
        if val is None and self.required:
            raise PyDBValueError("Value is NULL for a required attribute.")

    def check_constraints(self, val):
        pass

    def get_header(self, val):
        return TypeHeader(val is None, 0)

    def encode(self, val):
        header = self.get_header(val)
        if header.null == 0:
            data = self.encode_value(val)
            header.size = len(data)
        else:
            data = b''
            header.size = 0
        return header.encode_header() + data

    def decode(self, gen):
        th = TypeHeader.decode_header(gen)
        if th.null:
            return None
        arr = b''.join((islice(gen, 0, th.size)))
        val = self.decode_value(arr)
        self.check_value(val)
        return val


class IntegerType(GenericType):
    def __init__(self, size=4, **kwargs):
        super().__init__(**kwargs)
        self.size = size

    def get_type(self):
        return int

    def encode_value(self, val):
        return int_to_bytes(val, self.size)

    def decode_value(self, val):
        return bytes_to_int(val)


class StringType(GenericType):
    def __init__(self, size, **kwargs):
        super().__init__(**kwargs)
        self.max_length = size

    def check_constraints(self, val):
        if len(val) >= self.max_length:
            raise PyDBTypeConstraintError("String size exceeds {}".format(
                self.max_length))

    def get_type(self):
        return str

    def encode_value(self, val):
        return str.encode(val)

    def decode_value(self, val):
        return bytes.decode(val, 'ascii')

