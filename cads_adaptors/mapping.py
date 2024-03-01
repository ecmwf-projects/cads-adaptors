# copied from cds-forms-scripts/cds/mapping

import copy

DATE_KEYWORD_CONFIGS = [
    {
        "date_keyword": "date",
        "year_keyword": "year",
        "month_keyword": "month",
        "day_keyword": "day",
    },
    {
        "date_keyword": "hdate",
        "year_keyword": "hyear",
        "month_keyword": "hmonth",
        "day_keyword": "hday",
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


def date_from_years_month_days(years, months, days):
    for y in years:
        for m in months:
            for d in days:
                julian = ymd_to_julian(y, m, d)
                y1, m1, d1 = julian_to_ymd(julian)
                if (y, m, d) == (y1, m1, d1):
                    yield "%d-%02d-%02d" % (y, m, d)


def days_since_epoch(date, epoch):
    return date_to_julian(parse_date(date)) - date_to_julian(parse_date(epoch))


def seconds_since_epoch(date, epoch):
    return days_since_epoch(date, epoch) * 24 * 3600


###########################################################################################
# Code from mapping.py


def as_list(r, name, force):
    x = force.get(name, r.get(name, []))
    if not isinstance(x, list):
        return [x]
    return x


def to_interval(x):
    if "/" not in x:
        return "%s/%s" % (x, x)


def apply_mapping(request, mapping, embargo: dict[str: int] = dict()):
    request = copy.deepcopy(request)

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

    r = {}

    # remaps param names

    rename = mapping.get("rename", {})

    for name, values in request.items():
        r[rename.get(name, name)] = values

    date_keyword_configs = options.get("date_keyword_config", DATE_KEYWORD_CONFIGS)
    if isinstance(date_keyword_configs, dict):
        date_keyword_configs = [date_keyword_configs]

    # Loop over potential date keyword configs:
    for date_keyword_config in date_keyword_configs:
        date = date_keyword_config.get("date_keyword", "date")
        year = date_keyword_config.get("year_keyword", "year")
        month = date_keyword_config.get("month_keyword", "month")
        day = date_keyword_config.get("day_keyword", "day")

        if date in r:
            newdates = set()
            dates = r[date]
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


        # check if all Y,M,D are in request or force values
        elif all(
            [(thing in request) or (thing in force) for thing in [year, month, day]]
        ):
            years = [int(x) for x in as_list(request, year, force)]
            months = [int(x) for x in as_list(request, month, force)]
            days = [int(x) for x in as_list(request, day, force)]

            if years and months and days:
                newdates = set(date_from_years_month_days(years, months, days))

        # implement embargo

        # Transform year/month/day in dates
        if options.get("wants_dates", False):
            r[date] = sorted(newdates)
            for k in (year, month, day):
                if k in r:
                    del r[k]
                if k in force:
                    del force[k]

        if options.get("wants_intervals", False):
            if date in r:
                r[date] = [to_interval(d) for d in r[date]]

        epoch = options.get("seconds_since_epoch")
        if epoch is not None:
            extra = options.get("add_hours_to_date", 0) * 3600
            oldvalues = r[date]
            if isinstance(oldvalues, list):
                r[date] = [
                    str(seconds_since_epoch(v, epoch) + extra) for v in oldvalues
                ]
            else:
                r[date] = str(seconds_since_epoch(oldvalues, epoch) + extra)

    # Set forced values

    r.update(force)

    return r



def months_to_days(n_months, now_date):
    """
    Calculate the number of days from now_date to a set number of months in the past
    """
    from datetime import datetime
    from calendar import monthrange
    if n_months==0:
        return 0
    elif n_months>12:
        raise ValueError("Cannot handle embargos greater than 12 months")
    now_month = now_date.month
    then_year = now_date.year
    then_day = now_date.day
    then_month = int(now_month-n_months)
    if then_month <1:
        then_month = 12+then_month
        then_year = then_year-1
    then_date = datetime(then_year, then_month, min(then_day, monthrange(then_year, then_month)[1]))
    delta = now_date - then_date
    return delta.days



def implement_embargo(dates, embargo, cacheable=True):
    '''
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
    '''
    from datetime import datetime, timedelta
    embargo.setdefault("days", 0)
    embargo['days'] += months_to_days(embargo.pop("months", 0), datetime.utcnow())
    embargo_error_time_format = embargo.pop("error_time_format", "%Y-%m-%d %H:00")
    embargo_datetime = datetime.utcnow() - timedelta(
        **embargo
    )
    out_requests = []
    for date in dates:
            this_date = dtparse(date).date()
            if this_date<embargo_datetime.date():
                _out_dates.append(date)
            elif this_date==embargo_datetime.date():
                # Request has been effected by embargo, therefore should not be cached
                cacheable = False
                # create a new request for data on embargo day
                embargo_hour = embargo_datetime.hour
                # Times must be in correct list format to see if in or outside of embargo
                times = ensure_and_expand_list_items(req.get("time", []), "/")
                try:
                    times = [
                        t for t in times if time2seconds(t)/3600<=embargo_hour
                    ]
                except:
                    raise BadRequestException(
                        "Your request straddles the last date available for this dataset, therefore the time "
                        "period must be provided in a format that is understandable to the CDS/ADS "
                        "pre-processing. Please revise your request and, if necessary, use the cdsapi sample "
                        "code provided on the catalogue entry for this dataset."
                    )
                # Only append embargo days request if there is at least one valid time
                if len(times)>0:
                    extra_request = {
                        **req,
                        'date': [date],
                        'time': times
                    }
                    _extra_requests.append(extra_request)

        if len(_out_dates)>0:
            req['date'] = _out_dates
            out_requests.append(req)
        
        # append any extra requests to the end
        out_requests += _extra_requests

    if len(out_requests)==0 and len(requests)>=1:
        raise ValueError(
            "None of the data you have requested is available yet, please revise the period requested. "
            "The latest date available for this dataset is: "
            f"{embargo_datetime.strftime(embargo_error_time_format)}", ""
        )
    elif len(out_requests) != len(requests):
        # Request has been effected by embargo, therefore should not be cached
        cacheable = False
    return out_requests, cacheable

