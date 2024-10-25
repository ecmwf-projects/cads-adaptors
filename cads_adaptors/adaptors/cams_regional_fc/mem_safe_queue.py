import logging
import os
import queue
import threading
import time
from tempfile import NamedTemporaryFile


class MemSafeQueue(queue.Queue):
    """Subclass of Queue that holds queued items in memory until the queue size
    hits a limit and then starts temporarily storing them on file instead. It
    means the queue memory usage will not grow out of control.
    """

    def __init__(
        self, nbytes_max, *args, tmpdir=None, logger=None, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.nbytes_max = nbytes_max
        self.nbytes = 0
        self.tmpdir = tmpdir
        self.logger = logging.getLogger(__name__) if logger is None else logger
        self._lock = threading.Lock()

        self.stats = {}
        for k1 in ["queue", "mem", "file"]:
            self.stats[k1] = {}
            for k2 in ["current", "total", "max"]:
                self.stats[k1][k2] = 0
        self.stats["iotime"] = 0.0

    def put(self, item, **kwargs):
        """Put an item in the queue."""
        data, fieldinfo = item
        self.stats["queue"]["total"] += 1
        self.stats["queue"]["max"] = max(self.stats["queue"]["max"], self.qsize())

        self.logger.debug(
            f'MemSafeQueue: Queue nbytes={self.nbytes}, '
            f'in-mem size={self.stats["mem"]["current"]}, '
            f'total size={self.qsize()}'
        )

        # Keep the item in memory or write to file and replace with the path?
        self._lock.acquire()
        if self.nbytes + len(data) <= self.nbytes_max:
            self.nbytes += len(data)
            self.stats["mem"]["total"] += 1
            self.stats["mem"]["current"] += 1
            self.stats["mem"]["max"] = max(
                self.stats["mem"]["current"], self.stats["mem"]["max"]
            )
            self._lock.release()
        else:
            self.stats["file"]["total"] += 1
            self.stats["file"]["current"] += 1
            self.stats["file"]["max"] = max(
                self.stats["file"]["current"], self.stats["file"]["max"]
            )
            self._lock.release()
            self.logger.debug(f"MemSafeQueue: storing on disk: {fieldinfo!r}")
            t = time.time()
            os.makedirs(self.tmpdir, exist_ok=True)
            with NamedTemporaryFile(dir=self.tmpdir, delete=False) as tmp:
                tmp.write(data)
            self.stats["iotime"] += time.time() - t
            item = (tmp.name, fieldinfo)

        super().put(item, **kwargs)

    def put_nowait(self, item, **kwargs):
        self.put(item, block=False)

    def get(self, **kwargs):
        xx, fieldinfo = super().get(**kwargs)

        # Received data or a temporary file path?
        if isinstance(xx, bytes):
            data = xx
            self.nbytes -= len(data)
            self.stats["mem"]["current"] -= 1
            self.logger.debug(
                f'MemSafeQueue: Queue nbytes={self.nbytes}, '
                f'in-mem size={self.stats["mem"]["current"]}, '
                f'total size={self.qsize()}'
            )
        else:
            self.stats["file"]["current"] -= 1
            t = time.time()
            with open(xx, "rb") as tmp:
                data = tmp.read()
            os.remove(xx)
            self.stats["iotime"] += time.time() - t

        return (data, fieldinfo)

    def get_nowait(self):
        return self.get(block=False)
