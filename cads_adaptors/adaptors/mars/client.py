import http
import json
import random
import time
import requests
import sys

import urllib3
import logging

LOG = logging.getLogger(__name__)


def bytes(n):

    if n < 0:
        sign = "-"
        n -= 0
    else:
        sign = ""

    u = ["", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB"]
    i = 0
    while n >= 1024:
        n /= 1024.0
        i += 1
    return "%s%g%s" % (sign, int(n * 10 + 0.5) / 10.0, u[i])


class Result:
    def __init__(
        self,
        /,
        error=None,
        message=None,
        retry_same_host=False,
        retry_next_host=False,
    ):
        self.error = error
        self.message = message
        self.retry_same_host = retry_same_host
        self.retry_next_host = retry_next_host

    def __repr__(self):
        message = "None" if self.message is None else self.message[:10]
        return (
            f"{self.__class__.__name__}(error={self.error!r}, retry_same_host={self.retry_same_host},"
            f" retry_next_host={self.retry_next_host}, message={message}... )"
        )


class RemoteMarsClientSession:

    def __init__(self, url, request, environ, target, timeout=60):
        self.url = url
        self.request = request
        self.environ = environ
        self.target = target
        self.uid = None
        self.endr_recieved = False
        self.timeout = timeout

    def _transfer(self, r):
        start = time.time()
        total = 0
        with open(self.target, "wb") as f:
            self.endr_recieved = False
            count = 0
            for chunk in r.raw.read_chunked():
                count += 1
                total += len(chunk)
                if len(chunk) == 4:
                    if chunk == b"RWND":
                        f.seek(0)
                        f.truncate(0)
                        continue

                    if chunk == b"EROR":
                        raise ValueError("Error received")

                    if chunk == b"ENDR":
                        self.endr_recieved = True
                        continue

                    raise ValueError(f"Unknown message {chunk}")

                f.write(chunk)

            if not self.endr_recieved:
                raise ValueError("ENDR not received")

        elapsed = time.time() - start
        LOG.info(f"Transfered {bytes(total)} in {elapsed:.1f}s, {bytes(total/elapsed)}")

    def execute(self):

        error = None

        try:
            r = requests.post(
                self.url,
                json=dict(
                    request=self.request,
                    environ=self.environ,
                ),
                stream=True,
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout as e:
            LOG.error(f"Timeout {e}")
            return Result(error=e, retry_next_host=True)
        except requests.exceptions.ConnectionError as e:
            LOG.error(f"Connection error {e}")
            return Result(error=e, retry_next_host=True)

        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOG.error(f"HTTP error {e}")
            error = e

        uid = None
        code = r.status_code
        if code not in (http.HTTPStatus.BAD_REQUEST, http.HTTPStatus.OK):

            retry_same_host = code in (
                http.HTTPStatus.BAD_GATEWAY,
                http.HTTPStatus.GATEWAY_TIMEOUT,
                http.HTTPStatus.INTERNAL_SERVER_ERROR,
                http.HTTPStatus.REQUEST_TIMEOUT,
                http.HTTPStatus.SERVICE_UNAVAILABLE,
            )

            retry_next_host = code in (http.HTTPStatus.TOO_MANY_REQUESTS,)

            if "X-MARS-SIGNAL" in r.headers:
                signal = int(r.headers["X-MARS-SIGNAL"])
                LOG.error(f"MARS client kill by signal {signal}")

            if "X-MARS-RETRY-SAME-HOST" in r.headers:
                retry_same_host = int(r.headers["X-MARS-RETRY-SAME-HOST"])

            if "X-MARS-RETRY-NEXT-HOST" in r.headers:
                retry_next_host = int(r.headers["X-MARS-RETRY-NEXT-HOST"])

            return Result(
                error=error,
                message=r.text or str(error),
                retry_same_host=retry_same_host,
                retry_next_host=retry_next_host or retry_same_host,
            )

        uid = r.headers["X-MARS-UID"]

        if code == http.HTTPStatus.BAD_REQUEST:
            if "X-MARS-EXIT-CODE" in r.headers:
                exitcode = int(r.headers["X-MARS-EXIT-CODE"])
                LOG.error(f"MARS client exited with code {exitcode}")

        if code == http.HTTPStatus.OK:
            try:
                self._transfer(r)
            except urllib3.exceptions.ProtocolError as e:
                LOG.exception("Error transferring file (1)")
                return Result(error=e, retry_same_host=True, retry_next_host=True)
            except Exception as e:
                LOG.exception("Error transferring file (2)")
                return Result(error=e)

        logfile = None

        try:
            r = requests.get(self.url + "/" + uid)
            r.raise_for_status()
            logfile = r.text
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            LOG.exception("Error getting log file")

        try:
            r = requests.delete(self.url + "/" + uid)
            r.raise_for_status()
            self.uid = None
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            LOG.exception("Error deleting log file")

        return Result(error=error, message=logfile or str(error))

    def __del__(self):
        try:
            if self.uid is not None:
                requests.delete(self.url + "/" + self.uid)
        except Exception:
            pass


class RemoteMarsClient:

    def __init__(self, url, retries=3, delay=10, timeout=60):
        self.url = url
        self.retries = retries
        self.delay = delay
        self.timeout = timeout

    def execute(self, request, environ, target):
        session = RemoteMarsClientSession(
            self.url, request, environ, target, self.timeout
        )

        for i in range(self.retries):
            reply = session.execute()
            if not reply.error:
                return reply

            if not reply.retry_same_host:
                return reply

            LOG.error(f"Error {reply}")
            LOG.error(f"Retry on the same host {self.url}")

            time.sleep(self.delay)

        return reply


class RemoteMarsClientCluster:
    def __init__(self, urls, retries=3, delay=10, timeout=60):
        self.urls = urls
        self.retries = retries
        self.delay = delay
        self.timeout = timeout

    def execute(self, request, environ, target):
        random.shuffle(self.urls)
        for url in self.urls:
            client = RemoteMarsClient(url, self.retries, self.delay, self.timeout)
            reply = client.execute(request, environ, target)
            if not reply.error:
                return reply

            if not reply.retry_next_host:
                return reply

            LOG.error(f"Error {reply}")
            LOG.error(f"Retry on the next host {url}")

        return reply


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(process)d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cluster = RemoteMarsClientCluster(
        ["http://mars-worker-2000.shared.compute.cci2.ecmwf.int:9000",
         "http://mars-worker-2001.shared.compute.cci2.ecmwf.int:9000",
         "http://mars-worker-1000.shared.compute.cci1.ecmwf.int:9000",
         "http://mars-worker-1001.shared.compute.cci1.ecmwf.int:9000"],
        retries=3,
        delay=10,
        timeout=None,
    )

    request = json.load(open(sys.argv[1]))
    environ = dict(uid="test")

    print(cluster.execute(request, environ, "target.grib"))
