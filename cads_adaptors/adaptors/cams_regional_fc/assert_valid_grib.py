import os
import random
from datetime import datetime

from eccodes import codes_is_defined
from cds_common.message_iterators import grib_bytes_iterator
from cds_common.url2.downloader import ResponseException
from .grib2request import grib2request


def assert_valid_grib(req, response, context):
    """Raise a ResponseException if the request response indicates success but
       the content is not a valid grib message"""

    if response.status_code == 200:

        # Decode the grib messages, counting the fields
        count = 0
        try:
            # Check the messages are decodable by eccodes
            for msg in grib_bytes_iterator(response.content):
                count += 1

                # Check the message is a recognisable field by asking for
                # equivalent request dict
                grib2request(msg)

                # Check it has other required keys
                for k in ['latitudeOfFirstGridPoint',
                          'longitudeOfFirstGridPoint',
                          'latitudeOfLastGridPoint',
                          'longitudeOfLastGridPoint',
                          'jDirectionIncrement',
                          'iDirectionIncrement',
                          'Nj',
                          'Ni',
                          'scanningMode',
                          'values']:
                    if not codes_is_defined(msg, k):
                        raise Exception('Message has no "' + k + '" key')

        except Exception as e:

            # Write bad grib to file for investigation?
            if datetime.now() < datetime(2021, 10, 31, 0):
                rn = random.randint(0,2**128)
                file = f'/tmp/cams-europe-air-quality-forecasts/debug/badgrib_{context.request_id}.{rn}.grib'
                context.info(f'Writing bad grib to {file}: {req["url"]}')
                os.makedirs(os.path.dirname(file), exist_ok=True)
                with open(file, 'wb') as f:
                    f.write(response.content)

            raise ResponseException(
                'Request did not return valid grib: ' +
                '{}: {}'.format(e, req['req']))
            #+ '. First 100 bytes: ' +
            #    str(response.content[0:min(len(response.content), 100)]))

        if count == 0:
            raise ResponseException('Request returned no data: ' +
                                    repr(req['req']))

    return response
