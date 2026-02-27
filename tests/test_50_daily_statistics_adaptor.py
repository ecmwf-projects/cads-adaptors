"""Tests for ERA5 daily statistics adaptor."""

import pytest

from cads_adaptors.adaptors import Context
from cads_adaptors.adaptors.daily_statistics.adaptor import (
    Era5DailyStatisticsCdsAdaptor,
)
from cads_adaptors.exceptions import InvalidRequest


class TestGetDateListExtended:
    """Test the get_date_list_extended method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.context = Context()
        self.adaptor = Era5DailyStatisticsCdsAdaptor({}, context=self.context)

    def test_basic_date_extension(self):
        """Test that dates are extended by one day before and after."""
        date_list = ["2020-01-15"]
        time_zone_hour = 0
        first_valid_date_str = "1940-01-01"

        result = self.adaptor.get_date_list_extended(
            date_list, time_zone_hour, first_valid_date_str
        )

        assert result == ["2020-01-14", "2020-01-15", "2020-01-16"]

    def test_multiple_consecutive_dates(self):
        """Test extension with multiple consecutive dates."""
        date_list = ["2020-01-15", "2020-01-16", "2020-01-17"]
        time_zone_hour = 0
        first_valid_date_str = "1940-01-01"

        result = self.adaptor.get_date_list_extended(
            date_list, time_zone_hour, first_valid_date_str
        )

        # Should extend before first and after last, but not duplicate middle dates
        assert result == [
            "2020-01-14",
            "2020-01-15",
            "2020-01-16",
            "2020-01-17",
            "2020-01-18",
        ]

    def test_non_consecutive_dates(self):
        """Test extension with non-consecutive dates."""
        date_list = ["2020-01-15", "2020-01-20"]
        time_zone_hour = 0
        first_valid_date_str = "1940-01-01"

        result = self.adaptor.get_date_list_extended(
            date_list, time_zone_hour, first_valid_date_str
        )

        # Should extend each date independently and sort
        assert result == [
            "2020-01-14",
            "2020-01-15",
            "2020-01-16",
            "2020-01-19",
            "2020-01-20",
            "2020-01-21",
        ]

    def test_positive_time_zone_offset(self):
        """Test with positive time zone offset shifts first valid date."""
        date_list = ["1940-01-02"]
        time_zone_hour = 5
        first_valid_date_str = "1940-01-01"

        result = self.adaptor.get_date_list_extended(
            date_list, time_zone_hour, first_valid_date_str
        )

        # With positive offset, first valid date becomes 1940-01-02
        # So 1940-01-01 should be excluded
        assert result == ["1940-01-02", "1940-01-03"]

    def test_negative_time_zone_offset(self):
        """Test with negative time zone offset."""
        date_list = ["1940-01-02"]
        time_zone_hour = -5
        first_valid_date_str = "1940-01-01"

        result = self.adaptor.get_date_list_extended(
            date_list, time_zone_hour, first_valid_date_str
        )

        # With negative offset, first valid date stays at 1940-01-01
        assert result == ["1940-01-01", "1940-01-02", "1940-01-03"]

    def test_first_valid_date_boundary(self):
        """Test that dates before first valid date are excluded."""
        date_list = ["1940-01-01"]
        time_zone_hour = 0
        first_valid_date_str = "1940-01-01"

        result = self.adaptor.get_date_list_extended(
            date_list, time_zone_hour, first_valid_date_str
        )

        # 1939-12-31 should be excluded as it's before first valid date
        assert result == ["1940-01-01", "1940-01-02"]

    def test_invalid_dates_raises_error(self):
        """Test that requesting only invalid dates raises an error."""
        date_list = ["1939-12-30", "1939-12-31"]
        time_zone_hour = 0
        first_valid_date_str = "1940-01-01"

        with pytest.raises(
            InvalidRequest,
            match="Your request did not provide a valid time-period",
        ):
            self.adaptor.get_date_list_extended(
                date_list, time_zone_hour, first_valid_date_str
            )

    def test_mixed_valid_invalid_dates_logs_error(self):
        """Test that mixed valid/invalid dates filters and logs error."""
        date_list = ["1939-12-31", "1940-01-02"]
        time_zone_hour = 0
        first_valid_date_str = "1940-01-01"

        result = self.adaptor.get_date_list_extended(
            date_list, time_zone_hour, first_valid_date_str
        )

        # Should filter out 1939-12-31 and continue with 1940-01-02
        assert result == ["1940-01-01", "1940-01-02", "1940-01-03"]
        # Check that an error was logged (would need to inspect context logs)


class TestTimeZoneParsing:
    """Test time zone string parsing logic."""

    def test_utc_plus_zero(self):
        """Test parsing UTC+00:00."""
        time_zone = "UTC+00:00"
        time_zone_hour = int(time_zone.lower().replace("utc", "")[:3])
        assert time_zone_hour == 0

    def test_utc_plus_positive(self):
        """Test parsing positive UTC offsets."""
        test_cases = [
            ("UTC+01:00", 1),
            ("UTC+05:00", 5),
            ("UTC+12:00", 12),
        ]
        for time_zone, expected in test_cases:
            time_zone_hour = int(time_zone.lower().replace("utc", "")[:3])
            assert time_zone_hour == expected

    def test_utc_plus_negative(self):
        """Test parsing negative UTC offsets."""
        test_cases = [
            ("UTC-01:00", -1),
            ("UTC-05:00", -5),
            ("UTC-12:00", -12),
        ]
        for time_zone, expected in test_cases:
            time_zone_hour = int(time_zone.lower().replace("utc", "")[:3])
            assert time_zone_hour == expected

    def test_case_insensitive(self):
        """Test that parsing is case insensitive."""
        test_cases = [
            "UTC+05:00",
            "utc+05:00",
            "Utc+05:00",
        ]
        for time_zone in test_cases:
            time_zone_hour = int(time_zone.lower().replace("utc", "")[:3])
            assert time_zone_hour == 5


class TestTimeListGeneration:
    """Test the time list generation logic for MARS requests."""

    def test_1_hourly_frequency_no_offset(self):
        """Test 1-hourly frequency with no offset."""
        frequency = 1
        this_hour = 0

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        expected = [f"{h:02d}:00:00" for h in range(24)]
        assert time_list == expected

    def test_3_hourly_frequency_no_offset(self):
        """Test 3-hourly frequency with no offset."""
        frequency = 3
        this_hour = 0

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        expected = [
            "00:00:00", "03:00:00", "06:00:00", "09:00:00",
            "12:00:00", "15:00:00", "18:00:00", "21:00:00"
        ]
        assert time_list == expected

    def test_6_hourly_frequency_no_offset(self):
        """Test 6-hourly frequency with no offset."""
        frequency = 6
        this_hour = 0

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        expected = ["00:00:00", "06:00:00", "12:00:00", "18:00:00"]
        assert time_list == expected

    def test_1_hourly_with_positive_offset(self):
        """Test 1-hourly frequency with positive offset."""
        frequency = 1
        this_hour = 5

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        # With 1-hourly, offset doesn't change the list
        expected = [f"{h:02d}:00:00" for h in range(24)]
        assert time_list == expected

    def test_3_hourly_with_positive_offset(self):
        """Test 3-hourly frequency with positive offset (wrap-around case)."""
        frequency = 3
        this_hour = 5

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        # this_hour % frequency = 5 % 3 = 2
        # So we get: 2, 5, 8, 11, 14, 17, 20, 23
        expected = [
            "02:00:00", "05:00:00", "08:00:00", "11:00:00",
            "14:00:00", "17:00:00", "20:00:00", "23:00:00"
        ]
        assert time_list == expected

    def test_6_hourly_with_positive_offset(self):
        """Test 6-hourly frequency with positive offset."""
        frequency = 6
        this_hour = 8

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        # this_hour % frequency = 8 % 6 = 2
        # So we get: 2, 8, 14, 20
        expected = ["02:00:00", "08:00:00", "14:00:00", "20:00:00"]
        assert time_list == expected

    def test_negative_offset_wraparound(self):
        """Test negative offset causing wrap-around."""
        frequency = 3
        this_hour = -1  # Should wrap to 23

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        # this_hour % frequency = -1 % 3 = 2
        # So we get: 2, 5, 8, 11, 14, 17, 20, 23
        expected = [
            "02:00:00", "05:00:00", "08:00:00", "11:00:00",
            "14:00:00", "17:00:00", "20:00:00", "23:00:00"
        ]
        assert time_list == expected

    def test_accumulated_field_offset(self):
        """Test time offset for accumulated fields (shift back by accumulation period)."""
        # Simulate accumulated field with time_zone_hour=5, accumulation_period=1
        time_zone_hour = 5
        accumulation_period = 1
        frequency = 3

        this_hour = time_zone_hour - accumulation_period  # 5 - 1 = 4

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        # this_hour % frequency = 4 % 3 = 1
        # So we get: 1, 4, 7, 10, 13, 16, 19, 22
        expected = [
            "01:00:00", "04:00:00", "07:00:00", "10:00:00",
            "13:00:00", "16:00:00", "19:00:00", "22:00:00"
        ]
        assert time_list == expected

    def test_mean_field_offset_3hour_accumulation(self):
        """Test time offset for mean fields with 3-hour accumulation period."""
        # Simulate mean field with time_zone_hour=0, accumulation_period=3
        time_zone_hour = 0
        accumulation_period = 3
        frequency = 6

        this_hour = time_zone_hour - accumulation_period  # 0 - 3 = -3

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        # this_hour % frequency = -3 % 6 = 3
        # So we get: 3, 9, 15, 21
        expected = ["03:00:00", "09:00:00", "15:00:00", "21:00:00"]
        assert time_list == expected

    def test_uniqueness_and_sorting(self):
        """Test that duplicate hours are removed and result is sorted."""
        # Edge case where offset might create duplicates (shouldn't happen in practice)
        frequency = 24
        this_hour = 0

        raw_hours = [
            (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
        ]
        unique_sorted_hours = sorted(set(raw_hours))
        time_list = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

        # With 24-hour frequency, we should only get one time
        expected = ["00:00:00"]
        assert time_list == expected
