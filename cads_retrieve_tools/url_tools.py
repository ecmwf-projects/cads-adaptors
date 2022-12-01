import os
import requests
import tarfile
from typing import List, Optional
import urllib
import zipfile

import multiurl
import jinja2

from . import hcube_tools

import logging

logger = logging.Logger(__name__)


# copied from cdscommon/url2
def requests_to_urls(requests, patterns):
    """Given a list of requests and a list of URL patterns with Jinja2
       formatting, yield the associated URLs to download."""

    templates = [jinja2.Template(p) for p in patterns]

    for req in hcube_tools.unfactorise(requests):
        for url in [t.render(req).strip() for t in templates]:
            if url:
                yield {'url': url, 'req': req}


# TODO use targzstream
def download_zip_from_urls(
        urls: List[str],
        base_target: str,
) -> str:

    target = f"{base_target}.zip"
    paths = []
    for url in urls:
        path = os.path.basename(urllib.parse.urlparse(url).path)
        multiurl.download(url, path)
        paths.append(path)

    with zipfile.ZipFile(target, mode="w") as archive:
        for p in paths:
            archive.write(p)

    for p in paths:
        os.remove(p)

    return target


# TODO zipstream for archive creation
def download_tgz_from_urls(
        urls: List[str],
        base_target: str,
) -> str:

    target = f"{base_target}.tar.gz"
    paths = []
    for url in urls:
        path = os.path.basename(urllib.parse.urlparse(url).path)
        multiurl.download(url, path)
        paths.append(path)

    with tarfile.open(target, "w:gz") as archive:
        for p in paths:
            archive.add(p)

    for p in paths:
        os.remove(p)


def download_from_urls(
        urls: List[str],
        data_format: str = "zip",
) -> str:

    base_target = str(hash(tuple(urls)))

    if data_format == "tgz":
        target = download_tgz_from_urls(urls=urls, base_target=base_target)
    elif data_format == "zip":
        target = download_zip_from_urls(urls=urls, base_target=base_target)
    else:
        raise ValueError(f"{data_format=} is not supported")

    return target


def download_zip_from_urls_in_memory(urls: List[str], target: Optional[str] = None) -> str:
    if target is None:
        target = str(hash(tuple(urls)))
    with zipfile.ZipFile(target, 'w') as f:
        for url in urls:
            name = os.path.basename(urllib.parse.urlparse(url).path)
            response = multiurl.robust(requests.get)(url)
            data = response.content
            f.writestr(name, data)

    return target
