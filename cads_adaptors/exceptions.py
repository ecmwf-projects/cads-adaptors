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


class ParameterError(TypeError):
    """Raised when a request parameter is invalid."""


class InvalidRequest(ValueError):
    """Raised when an invalid request is sent to the adaptor."""


class CdsConfigError(InvalidRequest):
    """Raised when a dataset is badly configured."""


class MarsRuntimeError(RuntimeError):
    """Raised when a MARS request fails."""


class MarsNoDataError(InvalidRequest):
    """Raised when a MARS request returns no data."""


class MarsSystemError(SystemError):
    """Raised when a MARS request fails due to a system error."""


class UrlNoDataError(InvalidRequest):
    """Raised when a MARS request returns no data."""


class MultiAdaptorNoDataError(InvalidRequest):
    """Raised when a MultiAdaptor request returns no data."""


class CadsObsRuntimeError(RuntimeError):
    """Raised when a CADS-observation repository request fails."""


class CadsObsConnectionError(RuntimeError):
    """Raised when a CADS-observation repository request fails."""


class RoocsRuntimeError(RuntimeError):
    """Raised when a ROOCS request fails."""


class RoocsValueError(ValueError):
    """Raised when a ROOCS request fails due to a value error."""


class CdsFormatConversionError(RuntimeError):
    """Raised when a CDS post-processing request fails."""


class CdsConfigurationError(ValueError):
    """Raised when a CDS request fails due to a configuration error."""


class ArcoDataLakeNoDataError(InvalidRequest):
    """Raised when a ARCO Data Lake request returns no data."""
