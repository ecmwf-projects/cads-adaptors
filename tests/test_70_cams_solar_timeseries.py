from os import environ

from cads_adaptors import CamsSolarRadiationTimeseriesAdaptor
from cads_adaptors.adaptors.cams_solar_rad.functions import encode, verify

environ["CAMS_SOLAR_SECRET_STRING"] = "whatever"


def test_user_id_normal():
    """Test encoding of the ADS user ID for Vaisala in the case that the user is not a
    multi-user downstream service or the user has not provided a downstream service
    user ID.
    """
    # Test with and without downstream service config
    for cfg in [
        {},
        {"downstream_services": {}},
        {"downstream_services": {"wekeo": "wekeo_user_id"}},
    ]:
        # Test with and without a user-provided downstream service ADS user ID
        for req in [{}, {"_user_id": "downstream_user_id"}]:
            # Note that the ADS user ID does not match the downstream service, if
            # configured.
            adp = CamsSolarRadiationTimeseriesAdaptor(
                form=[], user_uid="ads_user_id", **cfg
            )

            # All the configs and requests should result in the same user ID being
            # passed to Vaisala
            username = encode(adp._user_id(req))
            assert (
                username
                == "72bd0fb92fc1f6fbcbc53cb83d98d7e0df0bd0a833d0b8d296fe4ac6cfb63b39"
            )
            assert verify(username)


def test_user_id_service():
    """Test encoding of the ADS user ID for Vaisala in the case that the user is a
    multi-user downstream service.
    """
    # The current ADS user is Wekeo and Wekeo is configured as a downstream service
    adp = CamsSolarRadiationTimeseriesAdaptor(
        form=[], user_uid="foobar", downstream_services={"wekeo": "foobar"}
    )

    # If the request doesn't provide a downstream user ID they are treated as a normal
    # ADS user (no prefix on username created by encode())
    request = {}
    username = encode(adp._user_id(request))
    assert (
        username == "347d6ccec325d104fd800e9c47f23337e804655a6b31d07a6b3ccb88b1663523"
    )
    assert verify(username)

    # If the request does provide a downstream user ID then the user ID passed to
    # Vaisala is different and the service name is prepended to it. It still passes the
    # verify function used by Vaisala.
    request = {"_user_id": "downstream_user"}
    username = encode(adp._user_id(request))
    assert (
        username
        == "wekeo_77fb31d9c04bb7c6118c4de41968bc82a84157ef21a7852759fe61155717df30"
    )
    assert verify(username)


def test_user_id_service_multi():
    """As the test above but now a single service is allowed >1 ADS user ID."""
    wekeo_ads_user_ids = ["foobar", "hotpot"]

    # Try both of the known Wekeo ADS user IDs
    for ads_user in wekeo_ads_user_ids:
        adp = CamsSolarRadiationTimeseriesAdaptor(
            form=[],
            user_uid=ads_user,
            downstream_services={"wekeo": wekeo_ads_user_ids, "mondas": ["wibble"]},
        )

        # Both ADS user IDs are recognised as Wekeo and give the same ID to Vaisala
        request = {"_user_id": "downstream_user"}
        username = encode(adp._user_id(request))
        assert (
            username
            == "wekeo_77fb31d9c04bb7c6118c4de41968bc82a84157ef21a7852759fe61155717df30"
        )
        assert verify(username)


def test_verify_fails():
    """Check that verify returns False for strings that have not been created
       with encode()"""

    assert not verify("")
    assert not verify("a")
    x = encode("foo")
    assert verify(x)
    assert not verify(x+"a")
    assert not verify(x[0:-1])
    assert not verify(x[0:-1]+" ")
