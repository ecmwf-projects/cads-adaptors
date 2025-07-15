import concurrent.futures
import io
import logging
import os
import queue
import re
import threading
import time
import traceback
from urllib.parse import urlparse

import boto3
import jinja2
from cds_common.atomic_write import AtomicWrite
from cds_common.hcube_tools import count_fields, hcube_intdiff, hcubes_intdiff2
from cds_common.message_iterators import grib_bytes_iterator
from cds_common.url2.caching import NotInCache
from eccodes import codes_get_message

from . import DEFAULT_NO_CACHE_KEY
from .grib2request import grib2request
from .mem_safe_queue import MemSafeQueue


class AbstractCacher:
    """Abstract class for looking after cache storage and retrieval. This class
    defines the interface.
    """

    def __init__(
        self,
        integration_server,
        logger=None,
        no_put=False,
        permanent_fields=None,
        no_cache_key=None,
    ):
        self.integration_server = integration_server
        self.logger = logging.getLogger(__name__) if logger is None else logger
        self.no_put = no_put
        self._close_was_called = False

        # The name of a key which, if present in the original request, will be
        # inserted into the field description dictionary when writing to the
        # cache, which means it will appear in the cached filename. Its presence
        # will also mean corresponding files are always written to temporary
        # space. It is used for optionally avoiding the cache provided by this
        # class.
        self.no_cache_key = no_cache_key or DEFAULT_NO_CACHE_KEY

        # Fields which should be cached permanently (on the datastore). All
        # other fields will be cached in temporary locations.
        if permanent_fields is None:
            permanent_fields = [{"model": ["ENS"], "level": ["0"]}]
        self.permanent_fields = permanent_fields

    def close(self):
        if self._close_was_called:
            raise Exception("close() called twice?")
        self._close_was_called = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def put(self, req):
        """Write grib fields from a request into the cache."""
        if self._close_was_called:
            raise Exception("Attempt to write another field after close() called")

        # Do not cache sub-area requests when the sub-area is done by the Meteo
        # France backend. With the current code below they would be cached
        # without the area specification in the path and would get confused for
        # full-area fields.
        if "north" in req["req"]:
            return

        self._write_fields(req)

    def get(self, req):
        """Get a file from the cache or raise NotInCache if it doesn't exist."""
        # This is the method called by the URL2 code to see if the data is in
        # the cache before it attempts to download it from the Meteo France
        # backend. When the Meteo France API was changed from giving single
        # fields at a time to being able to return a hypercube of fields,
        # retrieval from the cache was moved to a separate section of code
        # that accesses the cache directly, so now we don't attempt it here.
        raise NotInCache()

    def cache_file_url(self, fieldinfo):
        """Return the URL of the specified field in the cache."""
        raise Exception("Needs to be overloaded by a child class")

    def _write_fields(self, req):
        """Write fields to the cache."""
        raise Exception("Needs to be overloaded by a child class")

    def _cache_permanently(self, field):
        """Return True if this field should be put in the permanent cache, False
        otherwise.
        """
        # Is this a field which should be stored in a permanent location? If the
        # field contains an area specification then it isn't because only
        # full-area fields are stored permanently. The self.no_cache_key key is
        # set to a random string to defeat the system cache when testing so make
        # sure that's not stored permanently.
        if "north" not in field and self.no_cache_key not in field:
            permanent, _, _ = hcubes_intdiff2(
                {k: [v] for k, v in field.items()}, self.permanent_fields
            )
        else:
            permanent = []

        return bool(permanent)

    def _cache_file_path(self, fieldinfo):
        """Return a field-specific path or the given field. Can be used by a
        child class to determine server-side cache location.
        """
        # Set the order we'd like the keys to appear in the filename. Area
        # keys will be last.
        order1 = ["model", "type", "variable", "level", "time", "step"]
        order2 = ["north", "south", "east", "west"]

        def key_order(k):
            if k in order1:
                return str(order1.index(k))
            elif k in order2:
                return "ZZZ" + str(order2.index(k))
            else:
                return k

        # Get a jinja2 template for these keys
        keys = tuple(sorted(list(fieldinfo.keys())))
        if keys not in self._templates:
            # Form a Jinja2 template string for the cache files. "_backend" not
            # used; organised by date; area keys put at the end.
            path_template = "{{ date }}/" + "_".join(
                [
                    "{k}={{{{ {k} }}}}".format(k=k)
                    for k in sorted(keys, key=key_order)
                    if k not in ["date", "_backend"]
                ]
            )
            self._templates[keys] = jinja2.Template(path_template)

        # Safety check to make sure no dodgy characters end up in the filename
        regex = r"^[\w.:-]+$"
        for k, v in fieldinfo.items():
            assert re.match(regex, k), "Bad characters in key: " + repr(k)
            assert isinstance(v, (str, int)), f"Unexpected type for {k}: {type(v)}"
            assert re.match(regex, str(v)), (
                "Bad characters in value for " + k + ": " + repr(v)
            )

        dir = "permanent" if self._cache_permanently(fieldinfo) else "temporary"
        # Data from the integration server should not mix with the production data
        if self.integration_server:
            dir += "_esuite"

        return f"{dir}/" + self._templates[keys].render(fieldinfo)

    def _field_iter(self, req):
        """Yield a descriptive dict and the associated data for each of the
        fields in req.
        """
        # Archive-backend requests can retrieve a hypercube of fields. Parse the
        # comma-separated lists into proper lists.
        request = {k: str(v).split(",") for k, v in req["req"].items()}

        # The req dict may contain the field data in memory or a path to a file
        # that contains the data. Get the data.
        if "data" in req:
            data = req["data"]
            if not isinstance(data, bytes):
                # Get bytes array from a ResponseObject
                data = data.content()
        else:
            with open(req["path"], "rb") as f:
                data = f.read()
        assert len(data) > 0

        # If there is one field in the request we can assume that the data is
        # of that single field, so we can make this shortcut.
        if count_fields(request) == 1:
            yield (req["req"], data)
            return

        # Loop over the grib fields
        for msg in grib_bytes_iterator(data):
            # Figure out the request values that correspond to this field
            fieldinfo = grib2request(msg)

            # Sanity-check that fieldinfo was part of the request
            intn, _, _ = hcube_intdiff(request, {k: [v] for k, v in fieldinfo.items()})
            nmatches = count_fields(intn)
            if nmatches == 0:
                raise Exception(
                    f"Got unexpected field {fieldinfo!r} from request "
                    + repr(req["req"])
                )
            assert nmatches == 1

            # If self.no_cache_key was in the request then insert it into
            # fieldinfo. This means it will appear in the cache file name,
            # which is useful for regression testing. It means multiple
            # tests that request the same field can share a unique no-cache
            # value so the field is retrieved from the backend the first
            # time but from cache on subsequent attempts.
            if self.no_cache_key in req["req"]:
                fieldinfo[self.no_cache_key] = req["req"][self.no_cache_key]

            yield (fieldinfo, codes_get_message(msg))


class AbstractAsyncCacher(AbstractCacher):
    """Augment the AbstractCacher class to add asynchronous cache puts. This
    class is still abstract since it does not do the actual data copy. It
    can be sub-classed in order to give asynchronous, and optionally also
    parallel, functionality to synchronous caching code.
    """

    def __init__(
        self,
        *args,
        logger=None,
        nthreads=None,
        max_mem=None,
        tmpdir=None,
        timeout=None,
        **kwargs,
    ):
        """The number of fields that will be written concurrently to the cache
        is determined by nthreads. Note that even if nthreads=1 it will still
        be the case that the fields will be cached asynchronously, even if
        not concurrently, and so a cacher.put() will not hold up the thread
        in which it is executed.
        Fields will be buffered in memory while waiting to be written until
        the memory usage exceeds max_mem bytes, at which point fields will be
        temporarily written to disk (in tmpdir) to avoid excessive memory
        usage.
        """
        super().__init__(*args, logger=logger, **kwargs)
        self.nthreads = 10 if nthreads is None else nthreads
        self.timeout = 3600 if timeout is None else timeout
        self._lock1 = threading.Lock()
        self._lock2 = threading.Lock()
        self._templates = {}
        self._futures = {}
        self._fatal_exception = None
        self._start_time = None
        self._queue = MemSafeQueue(
            100000000 if max_mem is None else max_mem, tmpdir=tmpdir, logger=logger
        )

    def _start_copy_threads(self):
        """Start the threads that will do the remote copies."""
        exr = concurrent.futures.ThreadPoolExecutor(max_workers=self.nthreads)
        self._start_time = time.time()
        self._processing_item = [False] * self.nthreads
        self._futures = {exr.submit(self._copier, i): i for i in range(self.nthreads)}
        exr.shutdown(wait=False)

    def close(self):
        """Close the queue and wait for threads to finish."""
        super().close()

        # This try-except will catch KeyboardInterrupts, which are only sent to
        # the main thread and would otherwise leave the other threads still
        # running. It signals those to stop by setting
        # self._fatal_exception.
        if self._futures:
            try:
                self._wait_for_threads()
            except BaseException as e:
                self._fatal_exception = self._fatal_exception or e
                raise

    def _wait_for_threads(self):
        close_time = time.time()

        # Wait for each thread to complete and check if any raised an
        # exception
        for future in concurrent.futures.as_completed(self._futures):
            ithread = self._futures[future]
            try:
                future.result(
                    timeout=max(self.timeout - (time.time() - self._start_time), 0)
                )
            except Exception as e:
                self._fatal_exception = self._fatal_exception or e
                # This won't cancel running futures, but it's better than
                # nothing
                [f.cancel() for f in self._futures]
                self.logger.debug(f"Thread {ithread} raised {e!r}")
                raise
            else:
                self.logger.debug(f"Thread {ithread} exited successfully")

        # Log a summary for performance monitoring
        summary = self._queue.stats.copy()
        iotime = summary.pop("iotime")
        now = time.time()
        summary["time_secs"] = {
            "elapsed": now - self._start_time,
            "drain": now - close_time,
            "io": iotime,
        }
        self.logger.info(f"MemSafeQueue summary: {summary!r}")

    def _write_fields(self, req):
        """Asynchronously cache fields."""
        # Refuse puts if the object is in an error state
        if self._fatal_exception:
            raise Exception(f"Cacher has raised {self._fatal_exception!r}")
        # Start the copying thread if not done already
        with self._lock1:
            if not self._futures:
                self._start_copy_threads()
        self._queue.put(req)

    def _copier(self, *args, **kwargs):
        """Thread to actually copy the data. There will be several of these
        running at the same time.
        """
        try:
            self._copier2(*args, **kwargs)
        except Exception as e:
            # Signal to other threads that they should stop because this one
            # encountered an exception
            self._fatal_exception = self._fatal_exception or e
            self.logger.error("Exception in copy thread: " + repr(e))
            raise

    def _copier2(self, ithread):
        # Loop over items in the queue
        while True:
            # Get an item from the queue
            req = self._queue_get(ithread)
            if req is None:
                break

            # if time.time() - self._start_time > 3:
            #    raise Exception('foobar')

            # It may be that this item represents multiple fields, in which case
            # we want to cache them in parallel for speed, so put every item
            # apart from the last back in the queue and only cache the last
            # one. Note: this code block could be commented and everything would
            # still work, just slower for multi-field inputs.
            prev = None
            for fieldinfo, data in self._field_iter(req):
                if self._fatal_exception:
                    raise Exception("Stopping due to exception in another thread")
                if prev:
                    self._queue.put(prev)
                prev = {"req": fieldinfo, "data": data}
            req = prev
            # Indicate that there is no longer any chance of this thread putting
            # anything related to this item in self._queue.
            self._processing_item[ithread] = False

            self._write_fields_sync(req)

        n = self._queue.qsize()
        if n > 0:
            raise Exception(f"{n} unconsumed items in queue")

    def _queue_get(self, ithread):
        """Return an item from the queue or None if there are no more items."""
        # Loop to poll for a new item
        while time.time() - self._start_time < self.timeout:
            if self._fatal_exception:
                raise Exception("Stopping due to exception in another thread")

            closed = self._close_was_called
            with self._lock2:
                try:
                    req = self._queue.get(timeout=0.1)
                    self._processing_item[ithread] = True
                except queue.Empty:
                    # There will be no new items if self.close() has been called
                    # and no other thread is at a stage whereby it might put
                    # something else in the queue.
                    if closed and not any(self._processing_item):
                        return None
                else:
                    return req

    def _write_fields_sync(self, req):
        """Write the fields to the appropriate cache location."""
        # Loop over grib messages in the data
        for fieldinfo, data in self._field_iter(req):
            self._write_1field_sync(data, fieldinfo)


class CacherS3(AbstractAsyncCacher):
    """Class to look after cache storage to, and retrieval from, an S3
    bucket.
    """

    def __init__(self, *args, s3_bucket=None, create_bucket=False, **kwargs):
        super().__init__(*args, **kwargs)

        endpoint_url = os.environ["STORAGE_API_URL"]
        self._host = urlparse(endpoint_url).hostname
        self._bucket = s3_bucket or "cci2-cams-regional-fc"
        self._credentials = dict(
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ["STORAGE_ADMIN"],
            aws_secret_access_key=os.environ["STORAGE_PASSWORD"],
        )
        self.client = boto3.client("s3", **self._credentials)

        # If it's not guaranteed that the bucket already exists then the caller
        # should pass create_bucket=True
        if create_bucket:
            rsrc = boto3.resource("s3", **self._credentials)
            bkt = rsrc.Bucket(self._bucket)
            if not bkt.creation_date:
                bkt = self.client.create_bucket(Bucket=self._bucket)

    def _write_1field_sync(self, data, fieldinfo):
        """Write one field to the appropriate cache location."""
        local_object = io.BytesIO(data)
        remote_path = self._cache_file_path(fieldinfo)

        self.logger.info(
            f"Caching {fieldinfo} to {self._host}:{self._bucket}:{remote_path}"
        )

        attempt = 0
        t0 = time.time()
        while True:
            attempt += 1
            try:
                if not self.no_put:
                    self.client.put_object(
                        Bucket=self._bucket,
                        Key=remote_path,
                        Body=local_object.getvalue(),
                    )
                status = "uploaded"
                break
            except Exception as exc:
                self.logger.error(
                    "Failed to upload to S3 bucket (attempt " f"#{attempt}): {exc!r}"
                )
                status = f"process ended in error: {exc!r}"
                if attempt >= 5:
                    break
        t1 = time.time()

        return {
            "status": status,
            "upload_time": t0 - t1,
            "upload_size": local_object.tell(),
        }

    def cache_file_url(self, fieldinfo):
        """Return the URL of the specified field in the cache."""
        return f"https://{self._host}/{self._bucket}/" + self._cache_file_path(
            fieldinfo
        )

    def delete(self, fieldinfo):
        """Only used for testing at the time of writing."""
        remote_path = self._cache_file_path(fieldinfo)
        self.client.delete_object(Bucket=self._bucket, Key=remote_path)


class CacherDisk(AbstractAsyncCacher):
    def __init__(self, *args, field2path=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.field2path = field2path

    def _write_1field_sync(self, data, fieldinfo):
        path = self.field2path(fieldinfo)
        self.logger.info(f"Writing {fieldinfo} to {path}")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with AtomicWrite(path, "wb") as f:
            f.write(data)


class CacherS3AndDisk(CacherS3):
    """Sub-class of CacherS3 to cache not only to an S3 bucket but to a local
    file as well. Failures to write to the S3 bucket are logged but considered
    non-fatal so as not to disrupt the caching to file, which is considered
    higher priority.
    """

    def __init__(self, *args, field2path=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.field2path = field2path
        self.s3_errors = []

    def _write_1fiels_sync(self, data, fieldinfo):
        # Write to a local path?
        if self.field2path:
            path = self.field2path(fieldinfo)
            self.logger.info(f"Writing {fieldinfo} to {path}")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with AtomicWrite(path, "wb") as f:
                f.write(data)

        # Write to the S3 bucket
        try:
            super()._write_fields_sync(data, fieldinfo)
        except Exception as e:
            self._log_s3_error("S3 write", e)

    def _log_s3_error(self, prelude, exc):
        txt = f"{prelude} failed with:\n" + "".join(traceback.format_exception(exc))
        self.s3_errors.append(txt)
        self.logger.error(txt)
