# Tests that any methods/classes exposed through __init__.py are still available.

def test_exposed_methods():
    from cads_adaptors import AbstractAdaptor
    from cads_adaptors import DummyAdaptor
    from cads_adaptors import AbstractCdsAdaptor
    from cads_adaptors import LegacyCdsAdaptor
    from cads_adaptors import DirectMarsCdsAdaptor
    from cads_adaptors import MarsCdsAdaptor
    from cads_adaptors import UrlCdsAdaptor
    from cads_adaptors import get_adaptor_class

