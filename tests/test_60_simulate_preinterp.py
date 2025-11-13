import logging
import re
from copy import deepcopy

import pytest

from cads_adaptors.adaptors import Context
from cads_adaptors.adaptors.mars import MarsCdsAdaptor
from cads_adaptors.exceptions import CdsConfigError, InvalidRequest

logger = logging.getLogger(__name__)

KEYNAME = "simulate_preinterpolation"


def test_no_area():
    """Test grid is added if no area supplied."""
    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.2}}}
    adp = MarsCdsAdaptor(form=None, **config)
    req = adp.simulate_preinterpolation({})
    assert req == {"grid": ["0.1", "0.2"]}


def test_bad_config():
    """Check invalid config raises appropriate exception."""
    config = {KEYNAME: {"random_key": "random_value"}}
    adp = MarsCdsAdaptor(form=None, **config)
    with pytest.raises(CdsConfigError):
        adp.normalise_request({"area": []})


def test_bad_areas():
    """Check exceptions and error messages for invalid areas."""
    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.1}}}

    # Too few items
    for area in (
        [],
        "",
        [""],
        "1/2/3",
        ["1/2/3"],
        [1, 2, 3],
        ["1", "2", "3"],
        dict(a=1, b=2, c=3, d=4, e=5),
    ):
        check_invalid_area(
            area, config,
            r"request\['area'\]: .+: list has too few items. Should have exactly 4",
        )

    # Too many items
    for area in ("1/2/3/4/5", [1, 2, 3, 4, 5]):
        check_invalid_area(
            area, config,
            r"request\['area'\]: .+: list has too many items. Should have exactly 4",
        )

    # Invalid number
    for area in ("1/2/3/a", [1, 2, 3, "a"]):
        check_invalid_area(
            area, config,
            r"request\['area'\]\[3\]: 'a' is not a valid number",
        )

    # Lat > 90
    check_invalid_area(
        "100/-10/-90/10", config,
        r"request\['area'\]\[0\]: 100.0 is greater than the maximum of 90.0",
    )

    # Lat < -90
    check_invalid_area(
        "90/-10/-91/10", config,
        r"request\['area'\]\[2\]: -91.0 is less than the minimum of -90.0",
    )


def test_global_areas():
    """Check global areas stay global."""

    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.1}}}

    # Global areas with N-S, E-W mixed up
    check_snap(" 90/-180/-90/ 180", config, ["90.0", "-180.0", "-90.0", "180.0"])
    check_snap("-90/-180/ 90/ 180", config, ["-90.0", "-180.0", "90.0", "180.0"])
    check_snap(" 90/ 180/-90/-180", config, ["90.0", "180.0", "-90.0", "-180.0"])
    check_snap("-90/ 180/ 90/-180", config, ["-90.0", "180.0", "90.0", "-180.0"])

    # Non-standard global areas
    check_snap("90/0/-90/360", config, ["90.0", "0.0", "-90.0", "360.0"])
    check_snap("90/0/-90/720", config, ["90.0", "0.0", "-90.0", "720.0"])
    check_snap("90/0/-90/-360", config, ["90.0", "0.0", "-90.0", "-360.0"])
    check_snap("90/0/-90/-360", config, ["90.0", "0.0", "-90.0", "-360.0"])
    check_snap("90/0/-90/-720", config, ["90.0", "0.0", "-90.0", "-720.0"])
    check_snap("90/360/-90/-720", config, ["90.0", "360.0", "-90.0", "-720.0"])
    check_snap("90/360/-90/0", config, ["90.0", "360.0", "-90.0", "0.0"])
    check_snap("90/720/-90/-360", config, ["90.0", "720.0", "-90.0", "-360.0"])
    check_snap("90/1/-90/361", config, ["90.0", "1.0", "-90.0", "361.0"])
    check_snap("90/-1/-90/359", config, ["90.0", "-1.0", "-90.0", "359.0"])

    # Global areas not aligned with grid
    check_snap("90.0/0.05/-90/360.05", config, ["90.0", "0.1", "-90.0", "360.0"])
    check_snap("90.0/-0.05/-90/359.95", config, ["90.0", "0.0", "-90.0", "359.9"])
    check_snap("90.0/-179.05/-90/180.05", config, ["90.0", "-179.0", "-90.0", "180.0"])


def test_zero_width():
    """Check zero-width areas stay zero-width."""
    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.1}}}

    check_snap("90/0/-90/0", config, ["90.0", "0.0", "-90.0", "0.0"])
    check_snap("90/10/-90/10", config, ["90.0", "10.0", "-90.0", "10.0"])
    check_snap("90/360/-90/360", config, ["90.0", "360.0", "-90.0", "360.0"])


def test_zero_area():
    """Test zero-width & zero-height areas."""
    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.1}}}

    # Zero-area aligned with the grid should work
    check_snap("1/2/1/2", config, ["1.0", "2.0", "1.0", "2.0"])

    # Zero-area off grid should fail
    check_invalid_area(
        "1.01/2.01/1.01/2.01",
        config,
        "request area contains no grid points",
    )


def test_zero_area_with_offset():
    """Test zero-width & zero-height areas with an offset grid."""
    config = {
        KEYNAME: {
            "grid": {"delta_lon": 0.1, "delta_lat": 0.1, "lon0": 0.05, "lat0": 0.05}
        }
    }

    # Zero-area aligned with the grid should work
    check_snap("1.05/2.05/1.05/2.05", config, ["1.05", "2.05", "1.05", "2.05"])

    # Zero-area off grid should fail
    check_invalid_area(
        "1.01/2.01/1.01/2.01",
        config,
        "request area contains no grid points",
    )


def test_no_change():
    """Test area & grid combinations that should result in no snap."""
    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.1}}}

    check_snap("0.1/0.2/0.3/0.4", config, ["0.1", "0.2", "0.3", "0.4"])
    check_snap("-0.1/-0.2/-0.3/-0.4", config, ["-0.1", "-0.2", "-0.3", "-0.4"])
    check_snap("80.1/-170.2/-80.3/170.4", config, ["80.1", "-170.2", "-80.3", "170.4"])


def test_no_change_with_offset():
    """Test area & offset-grid combinations that should result in no snap."""
    config = {
        KEYNAME: {
            "grid": {"delta_lon": 0.1, "delta_lat": 0.1, "lon0": 0.05, "lat0": 0.05}
        }
    }

    check_snap("0.15/0.25/0.35/0.45", config, ["0.15", "0.25", "0.35", "0.45"])
    check_snap("-0.15/-0.25/-0.35/-0.45", config, ["-0.15", "-0.25", "-0.35", "-0.45"])
    check_snap(
        "80.15/-170.25/-80.35/170.45", config, ["80.15", "-170.25", "-80.35", "170.45"]
    )


def test_snapping():
    """Test general snapping."""
    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.1}}}

    # All round down
    check_snap("10.123/-20.345/-30.456/40.789", config, ["10.1", "-20.3", "-30.4", "40.7"])

    # N/W round up, E/W round down
    check_snap("-10.123/20.345/-30.456/40.789", config, ["-10.2", "20.4", "-30.4", "40.7"])

    # N/W round down, E/W round up
    check_snap("80.123/-120.345/30.456/-40.789", config, ["80.1", "-120.3", "30.5", "-40.8"])


def test_snapping_with_offset():
    """Test general snapping with an offset grid."""
    config = {
        KEYNAME: {
            "grid": {"delta_lon": "0.1", "delta_lat": "0.2",
                     "lon0": "0.05", "lat0": 0.1}
        }
    }

    # All round down
    check_snap("10.123/-20.345/-30.456/40.789", config, ["10.1", "-20.25", "-30.3", "40.75"])

    # N/W round up, E/W round down
    check_snap("-10.123/20.345/-30.456/40.789", config, ["-10.3", "20.35", "-30.3", "40.75"])

    # N/W round down, E/W round up
    check_snap(
        "80.123/-120.345/30.456/-40.789", config, ["80.1", "-120.25", "30.5", "-40.85"]
    )


def test_case_insensitive():
    """Test case insensitivity of area keyword"""
    config = {KEYNAME: {"grid": {"delta_lon": 0.2, "delta_lat": 0.1}}}
    adp = MarsCdsAdaptor(
        form=None, context=Context(logger=logger), **deepcopy(config)
    )

    for key in ["area", "AREA", " AreA "]:
        req_in={key: [10.123, -20.345, -30.456, 40.789]}
        req_out = adp.simulate_preinterpolation(req_in)
        assert req_out == {key: ["10.1", "-20.2", "-30.4", "40.6"],
                           "grid": ["0.2", "0.1"]}, req_out


def test_grid_suppresses_snap():
    """Test presence of the grid keyword suppresses snapping"""

    config = {KEYNAME: {"grid": {"delta_lon": 0.1, "delta_lat": 0.1}}}
    adp = MarsCdsAdaptor(
        form=None, context=Context(logger=logger), **deepcopy(config)
    )
    area = "10.123/-20.345/-30.456/40.789"

    # Check area snaps without grid present
    req_in = {"area": area}
    req_out = adp.simulate_preinterpolation(req_in)
    assert req_out == {"area": ["10.1", "-20.3", "-30.4", "40.7"],
                       "grid": ["0.1", "0.1"]}

    # Check it doesn't snap if grid is present, and doesn't override a user-provided 
    # grid either
    for key in ["grid", "GRID", "  GriD  "]:
        req_in = {"area": area, key: "2/2"}
        req_out = adp.simulate_preinterpolation(deepcopy(req_in))
        assert req_out == {"area": area, key: "2/2"}, req_out


def check_invalid_area(area, config, regex):
    """Check the input area fails the schema check"""

    adp = MarsCdsAdaptor(
        form=None, context=Context(logger=logger), **deepcopy(config)
    )

    req_in={"area": area}

    with pytest.raises(InvalidRequest) as einfo:
        adp.simulate_preinterpolation(req_in)

    if not re.search(regex, einfo.value.args[0]):
        raise Exception(f"Exception message {einfo.value.args[0]!r} does not match "
                        f"regex: {regex!r}")


def check_snap(area_in, config, area_out):
    """Check the input area snaps as expected"""

    adp = MarsCdsAdaptor(
        form=None, context=Context(logger=logger), **deepcopy(config)
    )

    req_in={"area": area_in}
    req_out = adp.simulate_preinterpolation(req_in)
    pre_grid = config[KEYNAME]["grid"]
    assert (
        req_out == {"area": area_out, "grid": [str(pre_grid["delta_lon"]),
                                               str(pre_grid["delta_lat"])]}
    ), req_out
    
