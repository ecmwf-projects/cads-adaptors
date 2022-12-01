# copied from cdscommon

import re
from datetime import datetime, timedelta

# Character used to separate start and end dates in compressed form
separator = '/'

# Make a regular expression to match "current", "current+-N",
# "other_date/current+-N/other_date", etc.
_re_offset = r'(-|\+)[0-9]+(\.[0-9]*)?'
re_current = re.compile(r'(?P<prefix> *)current(?P<offset>' + _re_offset + ')?')


def compress_dates_list(date_strings, date_format=None):
    """Compress any lists of consecutive dates in the input list to
       start/end form."""

    if date_format is None and date_strings:
        for string in date_strings:
            date_format = guess_date_format(string)
            if date_format is not None:
                break
        else:
            raise Exception('Cannot determine format of any of these dates: ' + \
                            repr(date_strings))

    # Convert to datetime objects, expanding any existing compressed ranges
    try:
        dates = expand_dates_list(date_strings, as_datetime=True)
    except ValueError:
        raise Exception('Malformatted date strings?: ' + repr(date_strings))

    # Get number of seconds between consecutive pairs
    dates = sorted(list(set(dates)))
    diffs = [(d1 - d2).total_seconds()
             for d1, d2 in zip(dates[1:], dates[0:-1])]

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
        compressed_dates.append(separator.join([d.strftime(date_format)
                                                for d in items]))
        i0 = i1 + 1

    return compressed_dates


def expand_dates_list(dates_in, as_datetime=False):
    """Expand any compressed date-range items in the input list to the full
       list."""

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
            elif 'current' in date:
                # Since "current" is translated in ranges it would be
                # inconsistent if it weren't translated when on its own, but
                # we have to get a format to convert it to first
                for date in dates_in:
                    fmt = guess_date_format(date)
                    if fmt is not None:
                        break
                if fmt is None:
                    fmt = '%Y-%m-%d'
                dates.append(string_to_datetime(date).strftime(fmt))
            else:
                dates.append(date)
        elif len(items) > 2:
            raise Exception('Do not know how to expand ' + date + ' yet')
        else:
            date1, date2 = items
            date1, fmt1 = string_to_datetime_with_format(date1)
            date2, fmt2 = string_to_datetime_with_format(date2)
            fmt = fmt1 if fmt1 is not None else \
                  (fmt2 if fmt2 is not None else '%Y-%m-%d')
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
       will be None."""

    assert isinstance(string, str), \
        'Input is not a string: ' + repr(type(string))

    if 'current' in string:
        return (current_to_datetime(string), None)

    else:
        for fmt in ['%Y-%m-%d', '%Y%m%d']:
            if len(string) < len(fmt):
                continue
            try:
                dt = datetime.strptime(string, fmt)
            except ValueError:
                pass
            else:
                return (dt, fmt)

    raise ValueError('Could not determine date format of ' +
                     repr(string))


def guess_date_format(string):
    """Guess the date format of the input string. It may be a compressed list of
       the form "date1/date2", in which case the format of the first date
       (which is not of the form "current+/offset") is returned. If both are
       of this form then None is returned."""

    for string in string.split(separator):
        dt, fmt = string_to_datetime_with_format(string)
        if fmt is not None:
            return fmt
    return None


def guess_time_format(string):
    for regex, fmt in (('\d\d:\d\d', '%H:%M'),
                       ('\d\d\d\d', '%H%M')):
        if re.fullmatch(regex, string):
            return fmt
    raise ValueError('Unrecognised time format: ' + string)


def string_to_datetime(string):
    """Convert the date string to a datetime object"""

    dt, fmt = string_to_datetime_with_format(string)
    return dt


def current_to_datetime(string):
    """Assuming the string matches the regex for the "current" date +/- an
       offset, return the resulting date as a datetime object."""

    match = re.match(re_current, string)
    if not match:
        raise Exception('Unrecognised date format: ' + string)

    dt = datetime.utcnow()
    if match.group('offset'):
        dt += timedelta(float(match.group('offset')))

    # Note that rounding off the hours, minutes, etc after appying the offset
    # allows non-integer offsets to increment the date at times other than
    # midnight
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def replace_current(string, date_format, as_datetime=False):
    """Any instance of "current" or "current(+|-)N" is replaced with
       (UTC) today's date with the appropriate offset. Note that N can be
       non-integer, in which case the time of day at which the replacement date
       increments will not be midnight."""

    if 'current' not in string:
        return string
    match = re.match(re_current, string)
    if not match:
        return string
    xdate = datetime.utcnow()
    if match.group('offset'):
        xdate += timedelta(float(match.group('offset')))
    return re.sub(re_current, r'\g<prefix>' + xdate.strftime(date_format),
                  string, count=1)


def to_string(date, date_format='%Y-%m-%d'):
    """Convert integer/float (possibly other types in future) to a string. If
       the input is already a string it is returned unaltered."""
    if isinstance(date, str):
        return date
    else:
        return datetime(int(date / 10000),
                        int((date / 100) % 100),
                        int(date % 100)).strftime(date_format)


def reformat_date(date, outformat, informat=None):
    """Reformat a date to the specified format. Numeric input (e.g. 20030101)
       is allowed."""
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
                if 'current' in d:
                    dates.append(d)
                else:
                    dates.append(
                        datetime.strptime(d, informat).strftime(outformat))
            out = separator.join(dates)
    return out


if __name__ == '__main__':
    #print(replace_current('xxxx current-1 current yyy', '%Y-%m-%d'))
    #print(expand_dates_list(['2020-01-01/current']))
    print(replace_current(' current-0.66 yyy', '%Y-%m-%d'))
