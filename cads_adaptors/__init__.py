"""CADS data retrieve utilities to be used by adaptors."""

# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    # NOTE: the `version.py` file must not be present in the git repository
    #   as it is generated by setuptools at install time
    from .version import __version__
except ImportError:  # pragma: no cover
    # Local copy or not installed with setuptools
    __version__ = "999"

from cads_adaptors.adaptors import AbstractAdaptor, DummyAdaptor
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.adaptors.insitu import (
    InsituDatabaseCdsAdaptor,
    InsituGlamodCdsAdaptor,
)
<<<<<<< HEAD
from cads_adaptors.adaptors.legacy import LegacyCdsAdaptor
from cads_adaptors.adaptors.mars import DirectMarsCdsAdaptor, MarsCdsAdaptor
from cads_adaptors.adaptors.url import UrlCdsAdaptor

=======
from cads_adaptors.adaptor_multi import MultiAdaptor
>>>>>>> cc46973 (adding MultiAdaptor to __init__)
from .tools.adaptor_tools import get_adaptor_class

__all__ = [
    "__version__",
    "get_adaptor_class",
    "AbstractAdaptor",
    "AbstractCdsAdaptor",
    "DirectMarsCdsAdaptor",
    "DummyAdaptor",
    "InsituDatabaseCdsAdaptor",
    "InsituGlamodCdsAdaptor",
    "LegacyCdsAdaptor",
    "MarsCdsAdaptor",
    "UrlCdsAdaptor",
    "MultiAdaptor",
]
