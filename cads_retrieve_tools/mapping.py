# copied from cds-forms-scripts/cds/mapping

import copy


try:
    from cdsinf.runner.mappingengines.mappingengine import MappingEngine
    from cdsinf.exceptions import BadRequestException
except ImportError:
    MappingEngine = object
    BadRequestException = Exception


def julian_to_ymd(jdate):
    # only integer julian dates are supported for now, as inherited
    try:
        jdate = int(jdate)
    except ValueError:
        raise TypeError('Invalid julian date')

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
        if '-' in date:
            y, m, d = date.split('-')
            return int(y) * 10000 + int(m) * 100 + int(d)
    return int(date)


def date_range(start_date, end_date, step=1):
    """Get all the dates in the interval defined by start_date and end_date.

    Args:
        start_date (str): Start date of the interval.
        end_date (str): End date of the interval.
        step (int): Spacing, in days, between dates. Defaults to 1.

    Raises:
        ValueError: If the input dates cannot be parsed.

    Returns:
        Generator returning one date at a time.

    Example:
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
    if '/' not in x:
        return '%s/%s' % (x, x)


def apply_mapping(request, mapping):

    request = copy.deepcopy(request)

    options = mapping.get("options", {})
    force = mapping.get("force", {})
    defaults = mapping.get("defaults", {})
    selection_limit = mapping.get("selection_limit")

    # Set defaults

    for name, values in defaults.items():
        request.setdefaults(name, values)

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

    date = options.get("date_keyword", "date")

    # Transform year/month/day in dates

    if options.get("wants_dates", False):

        if date in r:
            newdates = set()
            dates = r[date]
            if not isinstance(dates, list):
                dates = [dates]
            # Expand intervals
            for d in dates:

                if '/' in d:
                    start, end = d.split('/')
                    for e in date_range(start, end):
                        newdates.add(e)
                else:
                    newdates.add(d)

            r[date] = sorted(newdates)

        else:

            years = [int(x) for x in as_list(request, "year", force)]
            months = [int(x) for x in as_list(request, "month", force)]
            days = [int(x) for x in as_list(request, "day", force)]

            if years and months and days:
                r[date] = sorted(set(date_from_years_month_days(years, months, days)))

                for k in ('year', 'month', 'day'):
                    if k in r:
                        del r[k]
                    if k in force:
                        del force[k]

    if options.get("wants_intervals", False):
        if date in r:
            r[date] = [to_interval(d) for d in r[date]]

    epoch = options.get("seconds_since_epoch")
    if epoch:
        date = options.get("date_keyword", "date")
        extra = options.get("add_hours_to_date", 0) * 3600
        oldvalues = r[date]
        if isinstance(oldvalues, list):
            r[date] = [str(seconds_since_epoch(v, epoch) + extra) for v in oldvalues]
        else:
            r[date] = str(seconds_since_epoch(oldvalues, epoch) + extra)

    # Set forced values

    r.update(force)

    if selection_limit:
        count = 1
        for _, values in r.items():
            if isinstance(values, list):
                count *= len(values)

        # print("ITEM count %s limit %s" % (count, selection_limit))

        if count > selection_limit:
            raise BadRequestException("Request too large. Requesting %s items, limit is %s" % (count, selection_limit), '')

    print(r)
    return r
