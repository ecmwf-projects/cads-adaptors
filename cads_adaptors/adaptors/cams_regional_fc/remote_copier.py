import concurrent.futures
import io
import logging
import queue
import re
import socket
import subprocess
import tarfile
import threading
import time
from datetime import datetime
from os import environ, remove
from random import randint


class RemoteCopier:
    """Class to allow fast asynchronous copying of lots of small files to remote
    hosts from threaded applications. Transfers the files sequentially but
    re-uses the same ssh connection for each. Useful when, if an scp or rsync
    were used for each file, the total time spent establishing connections
    would greater than any saving made by transferring in parallel.
    """

    def __init__(self, logger=logging.getLogger(__name__)):
        self.executor = concurrent.futures.ThreadPoolExecutor(1)
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self._local_host = socket.gethostname().split(".")[0]
        self._dirs = set()
        self._logger = logger
        rand = randint(0, 10000000)
        self._unique_string = datetime.now().strftime("%s%f") + "." + str(rand)
        self._ssh_opts = [
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "ConnectTimeout=2",
            "-o",
            "ControlPath=" + "~/.ssh/master-%C." + str(rand),
            "-o",
            "ControlMaster=auto",
            "-o",
            "ControlPersist=60",
            "-o",
            "LogLevel=ERROR",
        ]
        self._tf = {}
        self._tf_count = {}

        # Temporary locations for tar file storage
        self._temp_dirs = {
            "local": "/cache/tmp",
            "compute-.*": "/cache/tmp",
            "datastore": "/scratch/tmp",
        }
        # For when testing/debugging on local desktop
        if environ.get("CDS_UNIT_TESTING"):
            self._temp_dirs["local"] = "/var/tmp/nal/DATA"
            self._temp_dirs["feldenak.ecmwf.int:8080"] = environ["SCRATCH"]

        # Start the thread that will copy the tar files
        self.future = self.executor.submit(self._copier)
        self.executor.shutdown(wait=False)

    def done(self):
        """Must be called once all files copied."""
        self.queue.put(None)
        exc = self.future.exception()
        if exc is not None:
            raise exc from exc

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.done()

    def copy(self, data, user, host, remote_path):
        """Asynchronously copy the bytes data to the specified file on specified host."""
        with self.lock:
            # Make an object to hold tar member metadata
            ti = tarfile.TarInfo(remote_path)
            ti.mtime = time.time()
            ti.size = len(data)

            # Write the data to the tar file
            tf = self._get_tar_file(user, host)
            with io.BytesIO(data) as f:
                tf["fileobj"].addfile(ti, fileobj=f)
            tf["size"] += len(data)
            tf["nfiles"] += 1

            # Copy to host and untar if above a certain size
            if tf["size"] > 100000000:
                uhost = user + "@" + host
                self.queue.put(uhost)

    def _get_tar_file(self, user, host):
        """Return a dict representing the tar file to write to."""
        uhost = user + "@" + host

        # Need to open a new tar file for this destination user and host?
        if uhost not in self._tf:
            # Get the remote temp directory for this host
            for regex, tmpdir in self._temp_dirs.items():
                if re.match(regex, host):
                    remote_tmpdir = tmpdir
                    break
            else:
                raise Exception("Do not know tmpdir for " + host)

            # A new tar file for this uhost may be created while the previous
            # one is still being copied. Use a count to prevent filename
            # clashes
            self._tf_count[uhost] = self._tf_count.get(uhost, 0) + 1
            name = "regional_fc.{}.{}.{}".format(
                uhost, self._unique_string, self._tf_count[uhost]
            )
            tf = {
                "path": self._temp_dirs["local"] + f"/{name}.l.tar",
                "remote_path": remote_tmpdir + f"/{name}.r.tar",
                "size": 0,
                "nfiles": 0,
            }
            tf["fileobj"] = tarfile.open(tf["path"], "w")
            self._tf[uhost] = tf
        else:
            tf = self._tf[uhost]

        return tf

    def _copier(self):
        """Thread to copy tar files to remote hosts and untar."""
        DO_COPY = False
        # Copy any tar files that exceed the max size
        while True:
            uhost = self.queue.get()
            if uhost is None:
                break
            if DO_COPY:
                self._copy_tar(uhost)
        # self._logger.debug('REMCOP Expecting no more copy calls. Remaining '
        #                   'files are ' +
        #                   ', '.join(v['path'] for v in self._tf.values()))

        # Copy any remaining tar files
        for uhost in list(self._tf.keys()):
            if DO_COPY:
                self._copy_tar(uhost)

    def _copy_tar(self, uhost):
        """Copy tar file to remote host and untar."""
        with self.lock:
            tf = self._tf.pop(uhost, None)
        if tf is None:
            # The file has already been copied
            return
        tf["fileobj"].close()
        self._logger.info(
            f'Copying tar file containing {tf["nfiles"]}' f' members to {uhost}'
        )
        self._exec(
            ["scp"] + self._ssh_opts + [tf["path"], f'{uhost}:{tf["remote_path"]}']
        )
        self._ssh(uhost, ["tar", "xPf", tf["remote_path"]])
        self._ssh(uhost, ["rm", tf["remote_path"]])
        remove(tf["path"])

    def _ssh(self, host, cmd, **kwargs):
        """Execute an ssh command in a way that re-uses an existing connection,
        if available.
        """
        self._exec(["ssh"] + self._ssh_opts + [host] + cmd, **kwargs)

    def _exec(self, cmd, **kwargs):
        # self._logger.info('Running command: ' + ' '.join(cmd))
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            **kwargs,
        )
        if proc.stdout:
            self._logger.info("stdout from " + repr(cmd) + ": " + str(proc.stdout))
        if proc.stderr:
            self._logger.warning("stderr from " + repr(cmd) + ": " + str(proc.stderr))
        if proc.returncode != 0:
            self._logger.error("Command failed: " + " ".join(cmd))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    t0 = time.time()
    with RemoteCopier() as r:
        for i in range(100):
            file = "foo." + str(i)
            print("Writing " + file)
            with open(file, "w") as f:
                f.write("This is file " + file + "\n")
                f.write("0" * 800000)
            with open(file, "rb") as f:
                r.copy(f.read(), "cds", "compute-0001", "/cache/downloads/" + file)
        print("Waiting for copying to finish")
    telapsed = time.time() - t0
    print("Copying took " + str(telapsed) + "s")
