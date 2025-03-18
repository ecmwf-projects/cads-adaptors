from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from cads_adaptors.exceptions import InvalidRequest

# Assuming the function is in a module named `embargo_handler`
from cads_adaptors.tools.date_tools import implement_embargo


def test_implement_embargo_no_embargo():
    requests = [{"date": ["2025-03-01"]}]
    embargo = {}
    out_requests, cacheable = implement_embargo(requests, embargo)
    assert out_requests == requests
    assert cacheable is True


def test_implement_embargo_with_days():
    requests = [{"date": ["2025-03-01", datetime.now().strftime("%Y-%m-%d")]}]
    embargo = {"days": 5}
    out_requests, cacheable = implement_embargo(requests, embargo)
    assert out_requests == [{"date": ["2025-03-01"]}]
    assert cacheable is False


def test_implement_embargo_with_months():
    requests = [{"date": ["2025-02-01", datetime.now().strftime("%Y-%m-%d")]}]
    embargo = {"months": 1}  # Should translate to roughly 30 days
    out_requests, cacheable = implement_embargo(requests, embargo)
    assert out_requests == [{"date": ["2025-02-01"]}]
    assert cacheable is False


def test_implement_embargo_all_filtered():
    requests = [{"date": ["2025-03-01", datetime.now().strftime("%Y-%m-%d")]}]
    embargo = {"days": 10}
    with pytest.raises(
        InvalidRequest, match="None of the data you have requested is available yet"
    ):
        with patch("cads_adaptors.tools.date_tools.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2025, 3, 2)
            implement_embargo(requests, embargo)


def test_implement_embargo_filter_timesteps():
    requests = [{"date": ["2025-03-01", "2025-03-02"], "time": ["00:00", "12:00"]}]
    embargo = {"days": 0, "hours": 6}  # DEFAULT: "filter_timesteps": True}
    with patch("cads_adaptors.tools.date_tools.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2025, 3, 2, 16, tzinfo=UTC)
        out_requests, cacheable = implement_embargo(requests, embargo)
    assert len(out_requests) == 2
    assert out_requests[0] == {"date": ["2025-03-01"], "time": ["00:00", "12:00"]}
    assert out_requests[1] == {"date": ["2025-03-02"], "time": ["00:00"]}
    assert cacheable is False

    # Check the case with filter_timesteps=False
    requests = [{"date": ["2025-03-01", "2025-03-02"], "time": ["00:00", "12:00"]}]
    embargo = {"days": 0, "hours": 6, "filter_timesteps": False}
    with patch("cads_adaptors.tools.date_tools.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2025, 3, 2, 16, tzinfo=UTC)
        out_requests, cacheable = implement_embargo(requests, embargo)
    assert len(out_requests) == 1
    assert out_requests[0] == {
        "date": ["2025-03-01", "2025-03-02"],
        "time": ["00:00", "12:00"],
    }
    assert cacheable is True
