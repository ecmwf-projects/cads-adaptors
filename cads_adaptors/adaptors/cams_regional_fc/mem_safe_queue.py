import os
import queue
import logging
import threading
from tempfile import NamedTemporaryFile


class MemSafeQueue(queue.Queue):
    """Subclass of Queue that holds queued items in memory until the queue size
       hits a limit and then starts temporarily storing them on file instead. It
       means the queue memory usage will not grow out of control.
    """

    def __init__(self, nbytes_max, tmpdir, *args,
                 logger=logging.getLogger(__name__), **kwargs):
        super().__init__(*args, **kwargs)
        self.nbytes_max = nbytes_max
        self.nbytes = 0
        self.counts = {}
        for k1 in ['queue', 'mem', 'file']:
            self.counts[k1] = {}
            for k2 in ['current', 'total', 'max']:
                self.counts[k1][k2] = 0
        self.tmpdir = tmpdir
        self.logger = logger
        self._lock = threading.Lock()

    def put(self, item, **kwargs):
        """Put an item in the queue"""

        data, fieldinfo = item
        self.counts['queue']['total'] += 1
        self.counts['queue']['max'] = max(self.counts['queue']['max'],
                                          self.qsize())

        self.logger.debug(f'MemSafeQueue: Queue nbytes={self.nbytes}, '
                          f'in-mem size={self.counts["mem"]["current"]}, '
                          f'total size={self.qsize()}')

        # Keep the item in memory or write to file and replace with the path?
        self._lock.acquire()
        if self.nbytes + len(data) <= self.nbytes_max:
            self.nbytes += len(data)
            self.counts['mem']['total'] += 1
            self.counts['mem']['current'] += 1
            self.counts['mem']['max'] = max(self.counts['mem']['current'],
                                            self.counts['mem']['max'])
            self._lock.release()
        else:
            self.counts['file']['total'] += 1
            self.counts['file']['current'] += 1
            self.counts['file']['max'] = max(self.counts['file']['current'],
                                             self.counts['file']['max'])
            self._lock.release()
            self.logger.debug(f'MemSafeQueue: storing on disk: {fieldinfo!r}')
            with NamedTemporaryFile(dir=self.tmpdir, delete=False) as tmp:
                tmp.write(data)
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
            self.counts['mem']['current'] -= 1
            self.logger.debug(f'MemSafeQueue: Queue nbytes={self.nbytes}, '
                              f'in-mem size={self.counts["mem"]["current"]}, '
                              f'total size={self.qsize()}')
        else:
            self.counts['file']['current'] -= 1
            with open(xx, 'rb') as tmp:
                data = tmp.read()
            os.remove(xx)

        return (data, fieldinfo)

    def get_nowait(self):
        return self.get(block=False)


