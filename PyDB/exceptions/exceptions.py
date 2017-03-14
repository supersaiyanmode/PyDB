class PyDBError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.__class__.__name__ + ": " + str(self.message)

class PyDBTypeError(PyDBError):
    def __init__(self, expected, val):
        msg = "Expected value of type {}, but got an instance of type: {}" \
                .format(expected.__name__, val.__class__.__name__)
        super().__init__(msg)

class PyDBValueError(PyDBError):
    def __init__(self, msg):
        super().__init__(msg)

class PyDBTypeConstraintError(PyDBError):
    def __init__(self, msg):
        msg = "Illegal value. Can't store value: " + msg
        super().__init__(msg)

class PyDBUniqueKeyViolation(PyDBError):
    def __init__(self, msg):
        msg = "Unique key violation: " + str(msg)
        super().__init__(msg)

class PyDBKeyNotFoundError(PyDBError):
    def __init__(self, msg):
        msg = "Not found: " + str(msg)
        super().__init__(msg)

class PyDBInternalError(PyDBError):
    def __init__(self, msg):
        msg = "Internal Error. Possibly corrupt database. " + msg
        super().__init__(msg)

class PyDBMetadataError(PyDBInternalError):
    def __init__(self, msg):
        super().__init__(msg)

class PyDBConsistencyError(PyDBError):
    def __init__(self, msg):
        super().__init__("Inconsistent file state. " + msg)

class PyDBIterationError(PyDBError):
    def __init__(self, msg):
        super().__init__("Iteration error: " + msg)

class PyDBOutOfSpaceError(PyDBError):
    def __init__(self, obj):
        super().__init__("Out of space in " + str(obj))
