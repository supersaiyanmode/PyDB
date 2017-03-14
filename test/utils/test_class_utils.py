import pytest

from PyDB.utils import custom_import
from PyDB.utils import get_qualified_name


class Test:
    pass

def test_custom_import1():
    c = custom_import('test.utils.test_class_utils.Test')
    assert c == Test


def test_custom_import3():
    with pytest.raises(NotImplementedError):
        c = custom_import('JustAClass')

def test_get_qualified_name():
    assert get_qualified_name(Test) == 'test.utils.test_class_utils.Test'


