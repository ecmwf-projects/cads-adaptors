import os
import re
import stat
import jinja2
import socket
import threading
from os.path import dirname
from tempfile import NamedTemporaryFile

from eccodes import codes_get_message, codes_write

from cds_common.url2.caching import NotInCache
from cds_common.hcube_tools import hcube_intdiff, hcubes_intdiff2, count_fields
from cds_common.message_iterators import grib_bytes_iterator
from .remote_copier import RemoteCopier
from .grib2request import grib2request


class Cacher:
    """Class to look after cache storage and retrieval"""

    def __init__(self, context, no_put=False):
        self.context = context
        self.temp_cache_root = '/cache/downloads/' + \
                               'cams-europe-air-quality-forecasts'
        self.remote_user = 'cds'
        self.no_put = no_put
        self.lock = threading.Lock()

        # Fields which should be cached permanently (on the datastore). All
        # other fields will be cached in temporary locations.
        self.permanent_fields = [{'model': ['ENS'],
                                  'level': ['0']}]

        # Get a list of the compute node names
        self.compute_nodes = []
        with open('/etc/hosts') as f:
            for x in [l.split()[1:] for l in f.readlines()
                      if not l.startswith('#')]:
                if x and x[0].startswith('compute-'):
                    self.compute_nodes.append(x[0].strip())
        self.compute_nodes = sorted(self.compute_nodes)
        self.compute_dns = {n: n for n in self.compute_nodes}

        # For when testing/debugging on local desktop
        if os.environ.get('CDS_UNIT_TESTING'):
            self.compute_nodes = ['feldenak']
            self.compute_dns = {'feldenak': 'feldenak.ecmwf.int:8080'}
            self.temp_cache_root = os.environ['SCRATCH'] + '/test_ads_cacher'
            self.remote_user = os.environ['USER']
            if not os.path.exists(self.temp_cache_root):
                os.makedirs(self.temp_cache_root)

        # Compute node we're running on
        self.host = socket.gethostname().split('.')[0]

        self._remote_copier = None
        self.templates = {}

        context.debug('CACHER: host is ' + self.host)
        context.debug('CACHER: compute nodes are ' + repr(self.compute_nodes))

    def done(self):
        if self._remote_copier is not None:
            self._remote_copier.done()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.done()

    def put(self, req):
        """Write grib fields from a request into the cache"""

        if self.no_put:
            return

        # Do not cache sub-area requests when the sub-area is done by the Meteo
        # France backend. With the current code below they would be cached
        # without the area specification in the path and would get confused for
        # full-area fields.
        if 'north' in req['req']:
            return

        request = {k: str(v).split(',') for k, v in req['req'].items()}

        # Loop over grib messages in the data
        data = req['data'].content()
        assert len(data) > 0
        try:
            for msg in grib_bytes_iterator(data):

                # Figure out the request values that correspond to this field
                req1field = grib2request(msg)

                # Sanity-check that req1field was part of the request
                intn, _, _ = hcube_intdiff(request,
                                           {k: [v] for k, v in
                                            req1field.items()})
                nmatches = count_fields(intn)
                if nmatches == 0:
                    raise Exception('Got unexpected field ' + repr(req1field) +
                                    ' from request ' + repr(req['req']))
                assert nmatches == 1

                # If no_cache was in the request then insert it into req1field.
                # This means it will appear in the cache file name, which is
                # useful for regression testing. It means multiple tests that
                # request the same field can share a unique no_cache value so
                # the field is retrieved from the backend the first time but
                # from cache on subsequent attempts.
                if 'no_cache' in req['req']:
                    req1field['no_cache'] = req['req']['no_cache']

                # Write to cache
                self._put_msg(msg, req1field)
        except Exception:
            # Temporary code for debugging
            from random import randint
            from datetime import datetime
            unique_string = datetime.now().strftime('%Y%m%d%H%M%S.') + \
                            str(randint(0,99999))
            with open(f'{self.temp_cache_root}/{unique_string}'
                      '.actually_bad.grib', 'wb') as f:
                f.write(data)
            raise

    def _put_msg(self, msg, req1field):
        """Write one grib message into the cache"""

        host, path, _ = self.cache_file_location(req1field)

        # It's easier if the cache host is the current host
        if host == self.host:
            self.context.debug('CACHER: writing to local file: ' + path)
            dname = dirname(path)
            try:
                os.makedirs(dname)
            except FileExistsError:
                pass
            # Write to a temporary file and then atomically rename in case
            # another process is trying to write the same file
            with NamedTemporaryFile(dir=dname, delete=False) as tmpfile:
                codes_write(msg, tmpfile)
            os.chmod(tmpfile.name,
                     stat.S_IRUSR | stat.S_IWUSR | \
                     stat.S_IRGRP | stat.S_IWGRP | \
                     stat.S_IROTH)
            os.rename(tmpfile.name, path)
        else:
            self.context.debug('CACHER: writing to remote file: ' +
                               repr((host, path)))
            with self.lock:
                if self._remote_copier is None:
                    self._remote_copier = RemoteCopier(logger=self.context)
            self._remote_copier.copy(codes_get_message(msg),
                                     self.remote_user, host, path)

    def get(self, req):
        """Get a file from the cache or raise NotInCache if it doesn't exist"""

        # This is the method called by the URL2 code to see if the data is in
        # the cache before it attempts to download it from the Meteo France
        # backend. When the Meteo France API was changed from giving single
        # fields at a time to being able to return a hypercube of fields,
        # retrieval from the cache was moved to a separate section of code
        # that accesses the cache directly, so now we don't attempt it here.
        raise NotInCache()

    def cache_file_location(self, field):
        """Return the host, path and url of the cache file for the given
           field"""

        # Is this a field which should be stored in a permanent location? If
        # the field contains an area specification then it isn't because only
        # full-area fields are stored permanently. The "no_cache" key is set to
        # a random string to defeat the system cache when testing so make sure
        # that's not stored permanently.
        if 'north' not in field and 'no_cache' not in field:
            permanent, _, _ = hcubes_intdiff2(
                {k: [v] for k, v in field.items()},
                self.permanent_fields)
        else:
            permanent = []

        if permanent:
            host, path, url = self.permanent_location(field)
        else:
            host, path, url = self.temporary_location(field)

        return (host, path, url)

    def permanent_location(self, field):
        """Return the host, path and url of the permanent cache file for the
           given field"""

        host = 'datastore'
        xpath = 'cataloguedata-cams/cams50_europe_air_quality_forecasts/' + \
                self.cache_field_path(field)
        path = '/home/datastore/data/' + xpath
        url = 'http://datastore.copernicus-climate.eu/' + xpath

        return (host, path, url)

    def temporary_location(self, field):
        """Return the host, path and url of the temporary cache file for the
           given field"""

        # Distribute the cache across the compute nodes by field day-of-month
        day = int(field['date'].split('-')[-1])
        assert day >= 1 and day <= 31
        if len(self.compute_nodes):
            host = self.compute_nodes[(day - 1) % len(self.compute_nodes)]
        else:
            host = "localhost"

        path = self.temp_cache_root + '/' + self.cache_field_path(field)
        #url = 'http://' + socket.gethostbyname(host) + path
        if host != "localhost":
            url = 'http://' + self.compute_dns[host] + path
        else:
            url = path

        return (host, path, url)

    def cache_field_path(self, field):
        """Return the field-specific end part of the path of the cache file
           for the given field"""

        # Set the order we'd like the keys to appear in the filename. Area
        # keys will be last.
        order1 = ['model', 'type', 'variable', 'level', 'time', 'step']
        order2 = ['north', 'south', 'east', 'west']
        def key_order(k):
            if k in order1:
                return str(order1.index(k))
            elif k in order2:
                return 'ZZZ' + str(order2.index(k))
            else:
                return k

        # Get a jinja2 template for these keys
        keys = tuple(sorted(list(field.keys())))
        if keys not in self.templates:
            # Form a Jinja2 template string for the cache files. "_backend" not
            # used; organised by date; area keys put at the end.
            path_template = '{{ date }}/' + \
                '_'.join(['{k}={{{{ {k} }}}}'.format(k=k)
                          for k in sorted(keys, key=key_order)
                          if k not in ['date', '_backend']])
            self.templates[keys] = jinja2.Template(path_template)

        # Safety check to make sure no dodgy characters end up in the filename
        regex = '^[\w.:-]+$'
        for k, v in field.items():
            assert re.match(regex, k), 'Bad characters in key: ' + repr(k)
            assert re.match(regex, str(v)), 'Bad characters in value for ' + \
                                            k + ': ' + repr(v)

        path = self.templates[keys].render(field)

        return path

    def cache_file_url(self, field):
        _, _, url = self.cache_file_location(field)
        return url


def default_to(value, default):
    if value is None:
        return default
    else:
        return value
