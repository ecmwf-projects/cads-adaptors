
import re
import pytest
import logging
from copy import deepcopy
from contextlib import nullcontext

from cads_adaptors.adaptors import Context
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.exceptions import CdsConfigurationError, InvalidRequest

logger = logging.getLogger(__name__)


def test_no_area():
    """Test it doesn't fail if no area supplied"""

    # Without config
    adp = AbstractCdsAdaptor(form=None)
    req = adp.normalise_request({})
    assert req == {}

    # With config
    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1
            }
        }
    }
    adp = AbstractCdsAdaptor(form=None, **config)
    req = adp.normalise_request({})
    assert req == {}


def test_bad_config():
    """Check invalid config raises appropriate exception"""

    config = {'snap_area': {}}
    adp = AbstractCdsAdaptor(form=None, **config)
    with pytest.raises(CdsConfigurationError):
        adp.normalise_request({'area': []})


def test_bad_areas():
    """Check exceptions and error messages for invalid areas"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1
            }
        }
    }

    # Too few items
    for area in ([], '', [''], '1/2/3', ['1/2/3'], [1, 2, 3], ['1', '2', '3'],
                 dict(a=1, b=2, c=3, d=4, e=5)):
        _exec(area, config, etype=InvalidRequest,
              eregex=r"request\['area'\]: .+: list has too few items. Should have "
                     "exactly 4")

    # Too many items
    for area in ('1/2/3/4/5', [1, 2, 3, 4, 5]):
        _exec(area, config, etype=InvalidRequest,
              eregex=r"request\['area'\]: .+: list has too many items. Should have "
              "exactly 4")

    # Invalid number
    for area in ('1/2/3/a', [1, 2, 3, 'a']):
        _exec(area, config, etype=InvalidRequest,
              eregex=r"request\['area'\]\[3\]: 'a' is not a valid number")

    # Lat > 90
    for area in ('100/-10/-90/10', ):
        _exec(area, config, etype=InvalidRequest,
              eregex=r"request\['area'\]\[0\]: 100.0 is greater than the maximum "
                     "of 90.0")

    # Lat < -90
    for area in ('90/-10/-91/10', ):
        _exec(area, config, etype=InvalidRequest,
              eregex=r"request\['area'\]\[2\]: -91.0 is less than the minimum "
                     "of -90.0")


def test_global_areas():
    """Check global areas stay global"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1
            }
        }
    }
                 
    _exec(' 90/-180/-90/ 180', config, [ '90.0', '-180.0', '-90.0',  '180.0'])
    _exec('-90/-180/ 90/ 180', config, ['-90.0', '-180.0',  '90.0',  '180.0'])
    _exec(' 90/ 180/-90/-180', config, [ '90.0',  '180.0', '-90.0', '-180.0'])
    _exec('-90/ 180/ 90/-180', config, ['-90.0',  '180.0',  '90.0', '-180.0'])

    _exec('90/0/-90/360', config, ['90.0', '0.0', '-90.0', '360.0'])
    _exec('90/0/-90/720', config, ['90.0', '0.0', '-90.0', '720.0'])
    _exec('90/0/-90/-360', config, ['90.0', '0.0', '-90.0', '-360.0'])
    _exec('90/0/-90/-360', config, ['90.0', '0.0', '-90.0', '-360.0'])
    _exec('90/0/-90/-720', config, ['90.0', '0.0', '-90.0', '-720.0'])
    _exec('90/360/-90/-720', config, ['90.0', '360.0', '-90.0', '-720.0'])
    _exec('90/360/-90/0', config, ['90.0', '360.0', '-90.0', '0.0'])
    _exec('90/720/-90/-360', config, ['90.0', '720.0', '-90.0', '-360.0'])
    _exec('90/1/-90/361', config, ['90.0', '1.0', '-90.0', '361.0'])
    _exec('90/-1/-90/359', config, ['90.0', '-1.0', '-90.0', '359.0'])


def test_zero_width():
    """Check zero-width areas stay zero-width"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1
            }
        }
    }

    _exec('90/0/-90/0', config, ['90.0', '0.0', '-90.0', '0.0'])
    _exec('90/10/-90/10', config, ['90.0', '10.0', '-90.0', '10.0'])
    _exec('90/360/-90/360', config, ['90.0', '360.0', '-90.0', '360.0'])


def test_zero_area():
    """Test zero-width & zero-height areas"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1
            }
        }
    }

    # Zero-area aligned with the grid should work
    _exec('1/2/1/2', config, ['1.0', '2.0', '1.0', '2.0'])

    # Zero-area off grid should fail
    _exec('1.01/2.01/1.01/2.01', config, etype=InvalidRequest,

          eregex="request area contains no grid points")

def test_zero_area_with_offset():
    """Test zero-width & zero-height areas with an offset grid"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1,
                'lon0': 0.05,
                'lat0': 0.05
            }
        }
    }

    # Zero-area aligned with the grid should work
    _exec('1.05/2.05/1.05/2.05', config, ['1.05', '2.05', '1.05', '2.05'])

    # Zero-area off grid should fail
    _exec('1.01/2.01/1.01/2.01', config, etype=InvalidRequest,
          eregex="request area contains no grid points")


def test_no_change():
    """Test area & grid combinations that should result in no snap"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1
            }
        }
    }

    _exec('0.1/0.2/0.3/0.4', config, ['0.1', '0.2', '0.3', '0.4'])
    _exec('-0.1/-0.2/-0.3/-0.4', config, ['-0.1', '-0.2', '-0.3', '-0.4'])
    _exec('80.1/-170.2/-80.3/170.4', config, ['80.1', '-170.2', '-80.3', '170.4'])


def test_no_change_with_offset():
    """Test area & offset-grid combinations that should result in no snap"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1,
                'lon0': 0.05,
                'lat0': 0.05
            }
        }
    }

    _exec('0.15/0.25/0.35/0.45', config, ['0.15', '0.25', '0.35', '0.45'])
    _exec('-0.15/-0.25/-0.35/-0.45', config, ['-0.15', '-0.25', '-0.35', '-0.45'])
    _exec('80.15/-170.25/-80.35/170.45', config, ['80.15', '-170.25', '-80.35', '170.45'])


def test_general():
    """Test general snapping"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.1
            }
        }
    }

    # All round down
    _exec('10.123/-20.345/-30.456/40.789', config,
          ['10.1', '-20.3', '-30.4', '40.7'])

    # N/W round up, E/W round down
    _exec('-10.123/20.345/-30.456/40.789', config,
          ['-10.2', '20.4', '-30.4', '40.7'])

    # N/W round down, E/W round up
    _exec('80.123/-120.345/30.456/-40.789', config,
          ['80.1', '-120.3', '30.5', '-40.8'])


def test_general_with_offset():
    """Test general snapping with an offset grid"""

    config = {
        'snap_area': {
            'grid': {
                'delta_lon': 0.1,
                'delta_lat': 0.2,
                'lon0': 0.05,
                'lat0': 0.1
            }
        }
    }

    # All round down
    _exec('10.123/-20.345/-30.456/40.789', config,
          ['10.1', '-20.25', '-30.3', '40.75'])

    # N/W round up, E/W round down
    _exec('-10.123/20.345/-30.456/40.789', config,
          ['-10.3', '20.35', '-30.3', '40.75'])

    # N/W round down, E/W round up
    _exec('80.123/-120.345/30.456/-40.789', config,
          ['80.1', '-120.25', '30.5', '-40.85'])


def _exec(area_in, config, area_out=None, etype=None, eregex=None):
    """Helper function to run AbstractCdsAdaptor.normalise_request and check
       output/exception is as expected"""

    adp = AbstractCdsAdaptor(form=None,
                             context=Context(logger=logger),
                             **deepcopy(config))

    if etype:
        cmgr = pytest.raises(InvalidRequest)
    else:
        cmgr = nullcontext()
    with cmgr as einfo:
        req = adp.normalise_request({'area': area_in})

    if eregex:
        assert etype
        assert re.search(eregex, einfo.value.args[0]), einfo.value.args[0]

    if not etype:
        assert req['area'] == area_out, \
            f"area_in={area_in!r}, area_out={req['area']!r}, expected={area_out!r}"
