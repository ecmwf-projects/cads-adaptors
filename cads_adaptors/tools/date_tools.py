# copied from cdscommon

import re
from datetime import datetime, timedelta
from typing import Any

from cads_adaptors import exceptions

# Character used to separate start and end dates in compressed form
separator = "/"

# Make a regular expression to match "current", "current+-N",
# "other_date/current+-N/other_date", etc.
_re_offset = r"(-|\+)[0-9]+(\.[0-9]*)?"
re_current = re.compile(r"(?P<prefix> *)current(?P<offset>" + _re_offset + ")?")


def compress_dates_list(date_strings, date_format=None):
    """Compress any lists of consecutive dates in the input list to
    start/end form.
    """
    if date_format is None and date_strings:
        for string in date_strings:
            date_format = guess_date_format(string)
            if date_format is not None:
                break
        else:
            raise Exception(
                "Cannot determine format of any of these dates: " + repr(date_strings)
            )

    # Convert to datetime objects, expanding any existing compressed ranges
    try:
        dates = expand_dates_list(date_strings, as_datetime=True)
    except ValueError:
        raise Exception("Malformatted date strings?: " + repr(date_strings))

    # Get number of seconds between consecutive pairs
    dates = sorted(list(set(dates)))
    diffs = [(d1 - d2).total_seconds() for d1, d2 in zip(dates[1:], dates[0:-1])]

    # Find chunks of consecutive dates
    i0 = 0
    compressed_dates = []
    while i0 < len(dates):
        i1 = i0
        while i1 < len(dates) - 1 and diffs[i1] == 86400:
            i1 += 1
        if i0 == i1:
            items = [dates[i0]]
        else:
            items = [dates[i0], dates[i1]]
        compressed_dates.append(
            separator.join([d.strftime(date_format) for d in items])
        )
        i0 = i1 + 1

    return compressed_dates


def expand_dates_list(dates_in, as_datetime=False):
    """Expand any compressed date-range items in the input list to the full
    list.
    """
    if not dates_in:
        return []
    if not isinstance(dates_in, list):
        dates_in = [dates_in]

    dates = []
    for date in dates_in:
        items = date.split(separator)
        if len(items) == 1:
            # Not a compressed list
            if as_datetime:
                dates.append(string_to_datetime(date))
            elif "current" in date:
                # Since "current" is translated in ranges it would be
                # inconsistent if it weren't translated when on its own, but
                # we have to get a format to convert it to first
                for date in dates_in:
                    fmt = guess_date_format(date)
                    if fmt is not None:
                        break
                if fmt is None:
                    fmt = "%Y-%m-%d"
                dates.append(string_to_datetime(date).strftime(fmt))
            else:
                dates.append(date)
        elif len(items) > 2:
            raise Exception("Do not know how to expand " + date + " yet")
        else:
            date1, date2 = items
            date1, fmt1 = string_to_datetime_with_format(date1)
            date2, fmt2 = string_to_datetime_with_format(date2)
            fmt = (
                fmt1 if fmt1 is not None else (fmt2 if fmt2 is not None else "%Y-%m-%d")
            )
            ndays = int((date2 - date1).total_seconds()) // 86400
            dates_dt = [(date1 + timedelta(days=i)) for i in range(ndays + 1)]
            if as_datetime:
                dates.extend(dates_dt)
            else:
                dates.extend([d.strftime(fmt) for d in dates_dt])

    return dates


def string_to_datetime_with_format(string):
    """Return the string parsed into a datetime object and the datetime format
    used. If the string is of the form "current[+/-offset]" then the format
    will be None.
    """
    assert isinstance(string, str), "Input is not a string: " + repr(type(string))

    if "current" in string:
        return (current_to_datetime(string), None)

    else:
        for fmt in ["%Y-%m-%d", "%Y%m%d"]:
            if len(string) < len(fmt):
                continue
            try:
                dt = datetime.strptime(string, fmt)
            except ValueError:
                pass
            else:
                return (dt, fmt)

    raise ValueError("Could not determine date format of " + repr(string))


def guess_date_format(string):
    """Guess the date format of the input string. It may be a compressed list of
    the form "date1/date2", in which case the format of the first date
    (which is not of the form "current+/offset") is returned. If both are
    of this form then None is returned.
    """
    for string in string.split(separator):
        dt, fmt = string_to_datetime_with_format(string)
        if fmt is not None:
            return fmt
    return None


def guess_time_format(string):
    for regex, fmt in (("\\d\\d:\\d\\d", "%H:%M"), ("\\d\\d\\d\\d", "%H%M")):
        if re.fullmatch(regex, string):
            return fmt
    raise ValueError("Unrecognised time format: " + string)


def string_to_datetime(string):
    """Convert the date string to a datetime object."""
    dt, fmt = string_to_datetime_with_format(string)
    return dt


def current_to_datetime(string):
    """Assuming the string matches the regex for the "current" date +/- an
    offset, return the resulting date as a datetime object.
    """
    match = re.match(re_current, string)
    if not match:
        raise Exception("Unrecognised date format: " + string)

    dt = datetime.utcnow()
    if match.group("offset"):
        dt += timedelta(float(match.group("offset")))

    # Note that rounding off the hours, minutes, etc after appying the offset
    # allows non-integer offsets to increment the date at times other than
    # midnight
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def replace_current(string, date_format, as_datetime=False):
    """Any instance of "current" or "current(+|-)N" is replaced with
    (UTC) today's date with the appropriate offset. Note that N can be
    non-integer, in which case the time of day at which the replacement date
    increments will not be midnight.
    """
    if "current" not in string:
        return string
    match = re.match(re_current, string)
    if not match:
        return string
    xdate = datetime.utcnow()
    if match.group("offset"):
        xdate += timedelta(float(match.group("offset")))
    return re.sub(
        re_current, r"\g<prefix>" + xdate.strftime(date_format), string, count=1
    )


def to_string(date, date_format="%Y-%m-%d"):
    """Convert integer/float (possibly other types in future) to a string. If
    the input is already a string it is returned unaltered.
    """
    if isinstance(date, str):
        return date
    else:
        return datetime(
            int(date / 10000), int((date / 100) % 100), int(date % 100)
        ).strftime(date_format)


def reformat_date(date, outformat, informat=None):
    """Reformat a date to the specified format. Numeric input (e.g. 20030101)
    is allowed.
    """
    if isinstance(date, (int, float)):
        out = to_string(date, outformat)
    else:
        if informat is None:
            informat = guess_date_format(date)
        if informat == outformat:
            out = date
        else:
            dates = []
            for d in date.split(separator):
                if "current" in d:
                    dates.append(d)
                else:
                    dates.append(datetime.strptime(d, informat).strftime(outformat))
            out = separator.join(dates)
    return out


def ensure_and_expand_list_items(thing, split_string=None):
    """
    Method to ensure that an object is a list, and that all internal
    strings are expanded to lists if they are defined as strings with separator,
    e.g. / in Mars requests.
    """
    if isinstance(thing, list):
        output_things = []
        for _thing in thing:
            output_things += ensure_and_expand_list_items(
                _thing, split_string=split_string
            )
        return output_things
    elif isinstance(thing, str) and split_string is not None:
        return thing.split(split_string)
    else:
        return [thing]


def months_to_days(n_months, now_date):
    """Calculate the number of days from now_date to a set number of months in the past."""
    from calendar import monthrange
    from datetime import UTC, datetime

    if n_months == 0:
        return 0
    elif n_months > 12:
        raise exceptions.CdsConfigurationError(
            "Cannot handle embargos greater than 12 months"
        )
    now_month = now_date.month
    then_year = now_date.year
    then_day = now_date.day
    then_month = int(now_month - n_months)
    if then_month < 1:
        then_month = 12 + then_month
        then_year = then_year - 1
    then_date = datetime(
        then_year,
        then_month,
        min(then_day, monthrange(then_year, then_month)[1]),
        tzinfo=UTC,
    )
    delta = now_date - then_date
    return delta.days


def time2seconds(time):
    """Return a time value parsed into integer seconds."""
    if isinstance(time, (int, float)):
        return int(time) * 3600

    else:
        time_regex1 = r"^(?P<H>\d?\d)(:(?P<M>\d\d)(:(?P<S>\d\d))?)?$"
        time_regex2 = r"^(?P<H>\d?\d)((?P<M>\d\d)((?P<S>\d\d))?)?$"
        if ":" in time:
            match = re.match(time_regex1, time)
        else:
            match = re.match(time_regex2, time)
        if not match:
            raise ValueError("Unrecognised time format: " + repr(time), "")
        hour = int(match.group("H"))
        minute = int(match.group("M")) if match.group("M") else 0
        second = int(match.group("S")) if match.group("S") else 0
        if hour > 23 or minute > 59 or second > 59:
            raise ValueError("Invalid time string: " + time, "")

        return hour * 3600 + minute * 60 + second


def implement_embargo(
    requests: list[dict[str, Any]], embargo: dict[str, Any], cacheable=True
) -> tuple[list[dict[str, Any]], bool]:
    """
    Implement any embargo defined in the adaptor.yaml. The embargo should be
    defined as the kwargs for the datetime.timedelta method, for example:
    ```
    embargo:
       days: 6
       hours: 12
    OR
    embargo:
       months: 1
       days: 5
       hours: 0
    ```
    If months key is present in embargo:
    convert months to days, taking into account a number of days in a given month,
    then remove months key from embargo.
    """
    from datetime import UTC, datetime, timedelta

    from dateutil.parser import parse as dtparse

    embargo.setdefault("days", 0)
    embargo["days"] += months_to_days(embargo.pop("months", 0), datetime.now(UTC))
    embargo_error_time_format: str = embargo.pop("error_time_format", "%Y-%m-%d %H:00")
    embargo_datetime = datetime.now(UTC) - timedelta(**embargo)
    out_requests = []
    for req in requests:
        _out_dates = []
        _extra_requests = []
        for date in req.get("date", []):
            this_date = dtparse(str(date)).date()
            if this_date < embargo_datetime.date():
                _out_dates.append(date)
            elif this_date == embargo_datetime.date():
                # Request has been effected by embargo, therefore should not be cached
                cacheable = False
                # create a new request for data on embargo day
                embargo_hour = embargo_datetime.hour
                # Times must be in correct list format to see if in or outside of embargo
                times = ensure_and_expand_list_items(req.get("time", []), "/")
                try:
                    times = [t for t in times if time2seconds(t) / 3600 <= embargo_hour]
                except Exception:
                    raise exceptions.InvalidRequest(
                        "Your request straddles the last date available for this dataset, therefore the time "
                        "period must be provided in a format that is understandable to the CDS/ADS "
                        "pre-processing. Please revise your request and, if necessary, use the cdsapi sample "
                        "code provided on the catalogue entry for this dataset."
                    )
                # Only append embargo days request if there is at least one valid time
                if len(times) > 0:
                    extra_request = {**req, "date": [date], "time": times}
                    _extra_requests.append(extra_request)

        if len(_out_dates) > 0:
            req["date"] = _out_dates
            out_requests.append(req)

        # append any extra requests to the end
        out_requests += _extra_requests

    if len(out_requests) == 0 and len(requests) >= 1:
        raise exceptions.InvalidRequest(
            "None of the data you have requested is available yet, please revise the period requested. "
            "The latest date available for this dataset is: "
            f"{embargo_datetime.strftime(embargo_error_time_format)}",
        )
    elif len(out_requests) != len(requests):
        # Request has been effected by embargo, therefore should not be cached
        cacheable = False
    return out_requests, cacheable
