
from itertools import product
from datetime import datetime, timedelta

from .formats import Formats


def nc_request_groups(context, reqs, info):
    """Chop up requests that cannot be converted to NetCDF as a whole. Group
       the resulting requests that can be converted as a whole."""

    def split_and_group(reqs, split_on):
        """Chop up and group requests by the keywords in split_on"""
        groups = {}
        for req in reqs:
            for splitvals in product(*[req[k] for k in split_on]):
                r = req.copy()
                r.update({k: [v] for k, v in zip(split_on, splitvals)})
                splitvals = tuple(splitvals)
                if splitvals not in groups:
                    groups[splitvals] = []
                groups[splitvals].append(r)
        return groups

    # Chop up and group requests by model and type
    groups = split_and_group(reqs, ['model', 'type'])

    # date is a special case because we only want to split on date if the
    # validity times overlap so that we can't have a single time dimension and
    # we're using a converter that can't handle >1 time dimension
    groups2 = {}
    for group_id, reqs in groups.items():
        split_by_date = (info['format'] == Formats.netcdf and
                         overlapping_vtimes(reqs))
        if split_by_date:
            for id, rs in split_and_group(reqs, ['date']).items():
                groups2[group_id + id] = rs
        else:
            groups2[group_id] = reqs
    groups = groups2

    context.info('For NetCDF conversion requests have been ' +
                 'split/grouped as following...')
    for id, grp in groups.items():
        context.info('    ' + '_'.join(id) + ': ' + repr(grp))

    return groups


def overlapping_vtimes(reqs):
    """Return True if validity times overlap between successive data times as
       expressed in the requests"""

    # Get the validity times associated with each data time
    vtimes = {}
    for req in reqs:
        for dtime in [datetime.strptime(d + ' ' + t, '%Y-%m-%d %H%M')
                      for d, t in product(req['date'], req['time'])]:
            if dtime not in vtimes:
                vtimes[dtime] = set()
            vtimes[dtime].update([dtime + timedelta(hours=int(s))
                                  for s in req['step']])

    # Sort
    dtimes = sorted(list(vtimes.keys()))
    for dtime in dtimes:
        vtimes[dtime] = sorted(list(vtimes[dtime]))

    # Does the last validity time from one forecast overlap with the first one
    # from the next?
    for ii in range(len(dtimes)-1):
        if vtimes[dtimes[ii]][-1] >= vtimes[dtimes[ii+1]][0]:
            return True

    return False
