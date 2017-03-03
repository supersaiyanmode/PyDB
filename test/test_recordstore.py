import os
import struct

from PyDB.datatypes import GenericType, IntegerType, StringType
from PyDB.store.recordstore import FileTableStore



class TestTable(object):
    record_no = IntegerType(primary_key=True)
    first_name = StringType(50)
    last_name = StringType(50)
    ssn = IntegerType(unique=True)

