# copied from cds-forms-scripts/cds/mapping
from typing import List
import copy
import datetime

from cads_adaptors.tools import ensure_list

DATE_OPTIONS = [
    {
        "date_key": "date",
        "year_key": "year",
        "month_key": "month",
        "day_key": "day",
        "date_format": "%Y-%m-%d",
    },
    {
        "date_key": "hdate",
        "year_key": "hyear",
        "month_key": "hmonth",
        "day_key": "hday",
        "date_format": "%Y%m%d",
    },
]


def julian_to_ymd(jdate):
    # only integer julian dates are supported for now, as inherited
    try:
        jdate = int(jdate)
    except ValueError:
        raise TypeError("Invalid julian date")

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


# def julian_to_sdate(jdate):
#     return "%d-%02d-%02d" % julian_to_ymd(jdate)


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
    if isinstance(date, str):
        if "-" in date:
            y, m, d = date.split("-")
            return int(y) * 10000 + int(m) * 100 + int(d)
    return int(date)


def date_range(start_date, end_date, step=1):
    """Get all the dates in the interval defined by start_date and end_date.

    Args:
    ----
        start_date (str): Start date of the interval.
        end_date (str): End date of the interval.
        step (int): Spacing, in days, between dates. Defaults to 1.

    Raises
    ------
        ValueError: If the input dates cannot be parsed.

    Returns
    -------
        Generator returning one date at a time.

    Example:
    -------
        >>> list(date_range(20110101, 20110105))
        >>> [20110101, 20110102, 20110103, 20110104, 20110105]


    """
    s = date_to_julian(parse_date(start_date))
    e = date_to_julian(parse_date(end_date))

    while s <= e:
        yield julian_to_date(s)
        s += step



def date_from_years_months_days(
    years: List, months: List, days: List, date_format: str ="%Y-%m-%d"
):

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


def to_interval(x):
    if "/" not in x:
        return "%s/%s" % (x, x)


def apply_mapping(request, mapping):
    request = copy.deepcopy(request)

    options = mapping.get("options", {})
    force = mapping.get("force", {})
    defaults = mapping.get("defaults", {})
    selection_limit = mapping.get("selection_limit")
    selection_limit_ignore = mapping.get("selection_limit_ignore", [])

    # Set defaults
    for name, values in defaults.items():
        request.setdefaults(name, values)

    # Enforce forced values:
    request.update(force)

    for name in options.get("wants_lists", []):
        if name in request:
            request[name] = ensure_list(request[name])

    # Remap values first
    remapping = mapping.get("remap", {})
    for name, remap in remapping.items():
        oldvalues = request.get(name)

        if oldvalues is not None:
            if isinstance(oldvalues, list):
                request[name] = [remap.get(v, v) for v in oldvalues]
            else:
                request[name] = remap.get(oldvalues, oldvalues)

    # This request starts with an empty list
    this_request = {}

    # Apply patches, TODO: check if this is required, no datasets use patches
    patches = mapping.get("patches", [])
    for p in patches:
        source = p["from"]
        target = p["to"]
        transform = p["mapping"]
        values = request[source]
        if isinstance(values, list):
            this_request[target] = [transform.get(v, v) for v in values]
        else:
            this_request[target] = transform.get(values, values)

    # remaps param names
    rename = mapping.get("rename", {})

    for name, values in request.items():
        this_request[rename.get(name, name)] = values

    date_options = ensure_list(options.get("date_options", DATE_OPTIONS))

    # Transform year/month/day in dates
    for date_opt in date_options:
        wants_dates = date_opt.get("wants_dates", options.get("wants_dates", False))
        date_key = date_opt.get("date_keyword", "date")
        year_key = date_opt.get("year_keyword", "year")
        month_key = date_opt.get("month_keyword", "month")
        day_key = date_opt.get("day_keyword", "day")

        if wants_dates:
            this_request = expand_dates(
                this_request,
                request,
                date_key=date_key,
                year_key=year_key,
                month_key=month_key,
                day_key=day_key,
            )
        print('DEBUG apply mapping', this_request, date_opt)

    # TODO: is this required? not used in any dataset
    if options.get("wants_intervals", False):
        if date_key in this_request:
            this_request[date_key] = [to_interval(d) for d in ensure_list(this_request[date_key])]

    # TODO: is this required? not used in any dataset
    epoch = options.get("seconds_since_epoch")
    if epoch:
        extra = options.get("add_hours_to_date", 0) * 3600
        oldvalues = this_request[date_key]
        if isinstance(oldvalues, list):
            this_request[date_key] = [
                str(seconds_since_epoch(v, epoch) + extra) for v in oldvalues
            ]
        else:
            this_request[date_key] = str(seconds_since_epoch(oldvalues, epoch) + extra)

    # Set forced values
    this_request.update(force)

    # Ensure that all values are lists:
    this_request = {k: ensure_list(v) for k, v in this_request.items()}

    if selection_limit:
        count = 1
        for key, values in this_request.items():
            if key not in selection_limit_ignore:
                count *= len(values)

        # print("ITEM count %s limit %s" % (count, selection_limit))

        if count > selection_limit:
            raise ValueError(
                "Request too large. Requesting %s items, limit is %s"
                % (count, selection_limit),
                "",
            )

    print(this_request)
    return this_request


def expand_dates(
    this_request,
    request,
    date_key="date",
    year_key="year",
    month_key="month",
    day_key="day",
):
    if date_key in this_request:
        newdates = set()
        dates = this_request[date_key]
        if not isinstance(dates, list):
            dates = [dates]
        # Expand intervals
        for d in dates:
            if "/" in d:
                start, end = d.split("/")
                for e in date_range(start, end):
                    newdates.add(e)
            else:
                newdates.add(d)

        this_request[date_key] = sorted(newdates)

    else:
        years = [int(x) for x in ensure_list(request.get(year_key, []))]
        print(years)
        months = [int(x) for x in ensure_list(request.get(month_key, []))]
        print(months)
        days = [int(x) for x in ensure_list(request.get(day_key, []))]
        print(days)

        if years and months and days:
            this_request[date_key] = sorted(
                set(date_from_years_months_days(years, months, days))
            )

            for k in (year_key, month_key, day_key):
                if k in this_request:
                    del this_request[k]
    
    return this_request
