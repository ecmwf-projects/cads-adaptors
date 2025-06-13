# Based on legacy code in cdscompute/.../mapping.py

import copy
import datetime
from typing import Any

from cads_adaptors import exceptions

DATE_KEYWORD_CONFIGS = [
    {
        "date_keyword": "date",
        "year_keyword": "year",
        "month_keyword": "month",
        "day_keyword": "day",
        "format_keyword": "date_format",
    },
    {
        "date_keyword": "hdate",
        "year_keyword": "hyear",
        "month_keyword": "hmonth",
        "day_keyword": "hday",
        "format_keyword": "hdate_format",
    },
]


def julian_to_ymd(jdate):
    # only integer julian dates are supported for now, as inherited
    try:
        jdate = int(jdate)
    except ValueError:
        raise exceptions.InvalidRequest(f"Invalid julian date: {jdate}")

    x = 4 * jdate - 6884477
    y = (x // 146097) * 100
    e = x % 146097
    d = e // 4

    x = 4 * d + 3
    y = (x // 1461) + y
    e = x % 1461
    d = e // 4 + 1

    x = 5 * d - 3
    m = x // 153 + 1
    e = x % 153
    d = e // 5 + 1

    if m < 11:
        month = m + 2
    else:
        month = m - 10

    day = d
    year = y + m // 11

    return year, month, day


def julian_to_date(jdate):
    y, m, d = julian_to_ymd(jdate)
    return y * 10000 + m * 100 + d


def julian_to_sdate(jdate):
    return "%d-%02d-%02d" % julian_to_ymd(jdate)


def ymd_to_julian(year, month, day):
    if month > 2:
        m1 = month - 3
        y1 = year
    else:
        m1 = month + 9
        y1 = year - 1

    a = 146097 * (y1 // 100) // 4
    b = 1461 * (y1 % 100) // 4
    c = (153 * m1 + 2) // 5 + day + 1721119
    return a + b + c


def date_is_valid(date):
    ymd = parse_date(date)
    return julian_to_date(date_to_julian(ymd)) == ymd


def date_to_julian(ddate):
    year = ddate // 10000
    ddate %= 10000
    month = ddate // 100
    ddate %= 100
    day = ddate

    return ymd_to_julian(year, month, day)


def parse_date(date):
    try:
        if isinstance(date, str) and "-" in date:
            y, m, d = date.split("-")
            output = int(y) * 10000 + int(m) * 100 + int(d)
        else:
            output = int(date)
    except Exception:
        raise exceptions.InvalidRequest(
            f'Invalid date string: "{date}". Should be ' "yyyymmdd or yyyy-mm-dd"
        )
    return output


def date_to_format(date, date_format=None):
    jdate = date_to_julian(parse_date(date))
    if date_format is None:
        return julian_to_date(jdate)
    else:
        return datetime.date(*julian_to_ymd(jdate)).strftime(date_format)


def date_range(start_date, end_date, step=1, date_format=None):
    """Get all the dates in the interval defined by start_date and end_date.

    Args:
        start_date (str): Start date of the interval.
        end_date (str): End date of the interval.
        step (int): Spacing, in days, between dates. Defaults to 1.

    Raises
    ------
        exceptions.InvalidRequest: If the input dates cannot be parsed.

    Returns
    -------
        Generator returning one date at a time.

    Example:
        >>> list(date_range(20110101, 20110105))
        >>> [20110101, 20110102, 20110103, 20110104, 20110105]


    """
    s = date_to_julian(parse_date(start_date))
    e = date_to_julian(parse_date(end_date))

    while s <= e:
        if date_format is None:
            yield julian_to_date(s)
        else:
            yield datetime.date(*julian_to_ymd(s)).strftime(date_format)
        s += step


def date_from_years_months_days(years, months, days, date_format):
    for y in years:
        for m in months:
            for d in days:
                try:
                    dt = datetime.date(y, m, d)
                except ValueError:
                    continue
                yield dt.strftime(date_format)


def days_since_epoch(date, epoch):
    return date_to_julian(parse_date(date)) - date_to_julian(parse_date(epoch))


def seconds_since_epoch(date, epoch):
    return days_since_epoch(date, epoch) * 24 * 3600


###########################################################################################
# Code from mapping.py


def integer_list(request, name):
    items = request.get(name, [])
    if not isinstance(items, list):
        items = [items]
    integers = []
    for item in items:
        try:
            integers.append(int(item))
        except Exception:
            raise exceptions.InvalidRequest(f"Invalid integer for {name}: {item!r}")
    return integers


def to_interval(x):
    if "/" not in x:
        return "%s/%s" % (x, x)


def expand_dates(r, request, date, year, month, day, date_format):
    if date in r:
        newdates = set()
        dates = r[date]
        if not isinstance(dates, list):
            dates = [dates]
        dates = [str(d) for d in dates]
        # Expand intervals
        for d in dates:
            if "/" in d:
                items = [_.strip() for _ in d.split("/")]
                if len(items) != 2 or not items[0] or not items[1]:
                    raise exceptions.InvalidRequest(
                        f'Date ranges must be of the form "start_date/end_date": "{d}"'
                    )
                newdates.update(date_range(*items, date_format=date_format))
            elif d:
                newdates.add(date_to_format(d, date_format))
            else:
                newdates.add(d)

        r[date] = sorted(newdates)

    else:
        years = integer_list(request, year)
        months = integer_list(request, month)
        days = integer_list(request, day)

        if years and months and days:
            r[date] = sorted(
                set(date_from_years_months_days(years, months, days, date_format))
            )

            if len(r[date]) == 0:
                raise exceptions.InvalidRequest(
                    f"No valid dates from year={years} month={months} day={days}"
                )

            for k in (year, month, day):
                if k in r:
                    del r[k]


def apply_mapping(request: dict[str, Any], mapping: dict[str, Any]):
    request = copy.deepcopy(request)
    mapping = copy.deepcopy(mapping)

    options = mapping.get("options", {})
    force = mapping.get("force", {})
    defaults = mapping.get("defaults", {})

    # Set defaults

    for name, values in defaults.items():
        request.setdefault(name, values)

    for name in options.get("wants_lists", []):
        if name in request:
            if not isinstance(request[name], list):
                request[name] = [request[name]]

    # Remap values first

    remapping = mapping.get("remap", {})
    for name, remap in remapping.items():
        oldvalues = request.get(name)

        if oldvalues is not None:
            if isinstance(oldvalues, list):
                request[name] = [remap.get(v, v) for v in oldvalues]
            else:
                request[name] = remap.get(oldvalues, oldvalues)

    if "area" in request and "area_as_mapping" in options:
        # If area is a mapping, we need to apply it
        area_mapping = options["area_as_mapping"]
        area = request["area"]

        if not isinstance(area_mapping, dict):
            raise exceptions.CdsConfigurationError(
                "Invalid area_as_mapping option, should be a string or a dict"
            )

        mapped_values: dict[list[str]] = {}
        for latlon_mapping in area_mapping:
            _lat = latlon_mapping.get("latitude")
            _lon = latlon_mapping.get("longitude")
            if _lat < area[0] and _lon > area[1] and _lat > area[2] and _lon < area[3]:
                _keys = [_k for _k in latlon_mapping.keys() if _k not in ("latitude", "longitude")]
                for _key in _keys:
                    if _key not in mapped_values:
                        mapped_values[_key] = [latlon_mapping[_key]]
                    else:
                        mapped_values[_key].append(latlon_mapping[_key])

        for key, values in mapped_values.items():
            if key in request:
                if isinstance(request[key], list):
                    request[key].extend(values)
                else:
                    request[key] = [request[key]] + values
            else:
                request[key] = values

    r = {}

    # Apply patches

    patches = mapping.get("patches", [])
    for p in patches:
        source = p["from"]
        target = p["to"]
        transform = p["mapping"]
        values = request[source]
        if isinstance(values, list):
            r[target] = [transform.get(v, v) for v in values]
        else:
            r[target] = transform.get(values, values)

    # remaps param names

    rename = mapping.get("rename", {})

    for name, values in request.items():
        r[rename.get(name, name)] = values

    # Add force values to request as some may be used in date expansion
    r.update(force)
    request.update(force)

    date_keyword_configs = options.get("date_keyword_config", DATE_KEYWORD_CONFIGS)
    if isinstance(date_keyword_configs, dict):
        date_keyword_configs = [date_keyword_configs]

    # Loop over potential date keyword configs:
    for date_keyword_config in date_keyword_configs:
        date_key = date_keyword_config.get("date_keyword", "date")
        year_key = date_keyword_config.get("year_keyword", "year")
        month_key = date_keyword_config.get("month_keyword", "month")
        day_key = date_keyword_config.get("day_keyword", "day")
        format_key = date_keyword_config.get("format_keyword", "date_format")

        # Transform year/month/day in dates
        if options.get("wants_dates", False):
            expand_dates(
                r,
                request,
                date_key,
                year_key,
                month_key,
                day_key,
                options.get(format_key, "%Y-%m-%d"),
            )

        if options.get("wants_intervals", False):
            if date_key in r:
                r[date_key] = [to_interval(d) for d in r[date_key]]

        epoch = options.get("seconds_since_epoch")
        if epoch is not None:
            extra = options.get("add_hours_to_date", 0) * 3600
            oldvalues = r[date_key]
            if isinstance(oldvalues, list):
                r[date_key] = [
                    str(seconds_since_epoch(v, epoch) + extra) for v in oldvalues
                ]
            else:
                r[date_key] = str(seconds_since_epoch(oldvalues, epoch) + extra)

    return r
