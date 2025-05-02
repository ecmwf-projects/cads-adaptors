import logging
import os
import pickle
import queue
import threading
import time
from collections import deque
from itertools import chain
from sys import getsizeof
from tempfile import NamedTemporaryFile


class MemSafeQueue(queue.Queue):
    """Subclass of Queue that holds queued items in memory until the queue size
    hits a limit and then starts temporarily storing them on file instead. It
    means the queue memory usage will not grow out of control.
    """

    def __init__(self, nbytes_max, *args, tmpdir=None, logger=None, **kwargs):
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
        with self._lock:
            self.stats["queue"]["total"] += 1
            self.stats["queue"]["max"] = max(self.stats["queue"]["max"], self.qsize())

        self.logger.debug(
            f'MemSafeQueue: Queue nbytes={self.nbytes}, '
            f'in-mem size={self.stats["mem"]["current"]}, '
            f'total size={self.qsize()}'
        )

        # Keep the item in memory or write to file and replace with the path?
        size = total_size(item)
        if self.nbytes + size <= self.nbytes_max:
            with self._lock:
                self.nbytes += size
            kstats = "mem"
        else:
            t = time.time()
            item = _Pickle(item, self.tmpdir)
            self.logger.debug(f"MemSafeQueue: stored {size}B item on disk")
            with self._lock:
                self.stats["iotime"] += time.time() - t
            kstats = "file"

        # Update summary stats
        with self._lock:
            self.stats[kstats]["total"] += 1
            self.stats[kstats]["current"] += 1
            self.stats[kstats]["max"] = max(
                self.stats[kstats]["current"], self.stats[kstats]["max"]
            )

        super().put((item, size), **kwargs)

    def put_nowait(self, item, **kwargs):
        self.put(item, block=False)

    def get(self, **kwargs):
        item, size = super().get(**kwargs)

        # Received original item or a path to a temp pickle file?
        if isinstance(item, _Pickle):
            t = time.time()
            item = item.unpickle()
            with self._lock:
                self.stats["file"]["current"] -= 1
                self.stats["iotime"] += time.time() - t
        else:
            with self._lock:
                self.nbytes -= size
                self.stats["mem"]["current"] -= 1
            self.logger.debug(
                f'MemSafeQueue: Queue nbytes={self.nbytes}, '
                f'in-mem size={self.stats["mem"]["current"]}, '
                f'total size={self.qsize()}'
            )            

        return item

    def get_nowait(self):
        return self.get(block=False)


class _Pickle:
    """Class to pickle & unpickle an object to/from a temporary file"""

    def __init__(self, item, tmpdir):
        if tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
        with NamedTemporaryFile(dir=tmpdir, delete=False) as f:
            self.path = f.name
            pickle.dump(item, f)

    def unpickle(self):
        with open(self.path, "rb") as f:
            item = pickle.load(f)
        os.remove(self.path)
        return item


# This function is a copy-pasted recipe from the internet
def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o))

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)
