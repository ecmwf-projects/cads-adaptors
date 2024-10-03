import os
import time
import random
import shutil
from datetime import datetime

from cds_common import hcube_tools
from cds_common.message_iterators import grib_file_iterator

from .grib2request import grib2request


def which_fields_in_file(reqs, grib_file, context):
    """Compare the requests with the contents of the grib file and return
       two lists of requests, representing those which are in the file and
       those which are not."""

    if grib_file is None:
        msg_iterator = []
    else:
        msg_iterator = grib_file_iterator(grib_file)

    # Read the grib file to find out which fields were retrieved
    try:
        reqs_infile = [{k: [v] for k, v in grib2request(msg).items()}
                       for msg in msg_iterator]
    except Exception:
        # Sometimes we have problems here. Copy the file somewhere so
        # we can investigate later.
        tmp = '/tmp/cams-europe-air-quality-forecasts/debug/' + \
              'problem_file.' + datetime.now().strftime('%Y%m%d.%H%M%S') + \
              '.' + str(random.randint(0, 2**128)) + '.grib'
        context.info('Encountered error when reading grib file. Copying ' +
                     'to ' + tmp + ' for offline investigation')
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        shutil.copyfile(grib_file, tmp)
        raise
    hcube_tools.hcubes_merge(reqs_infile)

    # Subtract retrieved fields from the full list of those requested
    # to get the list of uncached fields.
    t0 = time.time()
    _, reqs_missing, _ = hcube_tools.hcubes_intdiff2(reqs, reqs_infile)
    if time.time() - t0 > 10:
        context.warning('Took a long time for reqs=' + repr(reqs) +
                        ', reqs_infile=' + repr(reqs_infile))

    if hcube_tools.count_fields(reqs_infile) + \
       hcube_tools.count_fields(reqs_missing) != \
       hcube_tools.count_fields(reqs):
        raise Exception('Failure to separate present from missing fields.' +
                        '\n  reqs=' + repr(reqs) +
                        '\n  reqs_infile=' + repr(reqs_infile) +
                        '\n  reqs_missing=' + repr(reqs_missing))

    return (reqs_infile, reqs_missing)


