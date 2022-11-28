import os
from typing import List
import urllib
import zipfile

import multiurl
import jinja2

from . import hcube_tools

import logging

logger = logging.Logger(__name__)


def requests_to_urls(requests, patterns):
    """Given a list of requests and a list of URL patterns with Jinja2
       formatting, yield the associated URLs to download."""

    templates = [jinja2.Template(p) for p in patterns]

    for req in hcube_tools.unfactorise(requests):
        for url in [t.render(req).strip() for t in templates]:
            if url:
                yield {'url': url, 'req': req}


def download_from_urls(urls: List[str]) -> List[str]:

    paths = []
    for url in urls:
        path = os.path.basename(urllib.parse.urlparse(url).path)
        multiurl.download(url, path)
        paths.append(path)

    path = str(hash(tuple(paths)))

    with zipfile.ZipFile(path, mode="w") as archive:
        for p in paths:
            archive.write(p)

    for p in paths:
        os.remove(p)

    return path

