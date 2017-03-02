from PyDB.utils import int_to_bytes, bytes_to_int

from PyDB.exceptions import PyDBTypeError, PyDBValueError
from PyDB.exceptions import PyDBTypeConstraintError

class DefaultDummyType:
    pass

DEFAULT_VALUE = DefaultDummyType()

class TypeHeader(object):
    def __init__(self, null):
        self.null = int(null)


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

    def check_required(self, val):
        if val is None and self.required:
            raise PyDBValueError("Value is NULL for a required attribute.")

    def get_header(self, val):
        return TypeHeader(val is None)

    def encode_header(self, val):
        header = self.get_header(val)
        return int_to_bytes(header.null, 1)

    def decode_header(self, barr):
        return TypeHeader(bytes_to_int(barr[:1]))


class IntegerType(GenericType):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def check_value(self, val):
        self.check_required(val)
        if not isinstance(val, int):
            raise PyDBTypeError(int, val)

    def get_type(self):
        return int

    def encode(self, val):
        return struct.pack(">I", val)

    def decode(self, val):
        return struct.unpack(">I", val)

    def get_binary_size(self):
        return 4


class StringType(GenericType):
    def __init__(self, size, **kwargs):
        super().__init__(**kwargs)
        self.max_length = size

    def check_value(self, val):
        if len(val) >= self.max_length:
            raise PyDBTypeConstraintError("String size exceeds {}".format(
                self.max_length))
        return isinstance(val, str)

    def get_type(self, val):
        return str

    def encode(self, val):
        return str.encode(val)

    def decode(self, val):
        return bytearray.decode(val)

    def get_binary_size(self):
        return self.max_length

