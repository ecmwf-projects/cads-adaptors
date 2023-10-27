import logging
import os
import tarfile
import urllib
import zipfile
from typing import Any, Dict, Generator, List, Optional

import jinja2
import multiurl
import requests
import wget
import yaml

from . import hcube_tools

logger = logging.Logger(__name__)

DOWNLOADERS = {
    "http": multiurl.download,
    "https": multiurl.download,
    "ftp": wget.download,
}


# copied from cdscommon/url2
def requests_to_urls(
    requests: Dict[str, Any], patterns: List[str]
) -> Generator[Dict[str, Any], None, None]:
    """Given a list of requests and a list of URL patterns with Jinja2
    formatting, yield the associated URLs to download.
    """
    templates = [jinja2.Template(p) for p in patterns]

    for req in hcube_tools.unfactorise(requests):  # type: ignore
        for url in [t.render(req).strip() for t in templates]:
            if url:
                yield {"url": url, "req": req}


def try_download(urls: List[str]) -> List[str]:
    paths = []
    excs = []
    for url in urls:
        server_type = url.split(":")[0]
        downloader = DOWNLOADERS.get(server_type, DOWNLOADERS["https"])
        path = urllib.parse.urlparse(url).path.lstrip("/")
        dir = os.path.dirname(path)
        os.makedirs(dir, exist_ok=True)
        try:
            downloader(url, path)
        except Exception as exc_multiurl:
            excs.append({url: exc_multiurl})
            logger.warning(f"Failed download for URL: {url}\nTraceback: {exc_multiurl}")
        else:
            paths.append(path)

    if len(paths) == 0:
        raise RuntimeError(
            f"Request empty. At least one of the following:\n{yaml.safe_dump(urls, indent=2)} "
            "must be a valid url from which to download the data. "
        )
    return paths


# TODO use targzstream
def download_zip_from_urls(
    urls: List[str],
    base_target: str,
) -> str:
    target = f"{base_target}.zip"
    paths = try_download(urls)
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
    paths = try_download(urls)
    with tarfile.open(target, "w:gz") as archive:
        for p in paths:
            archive.add(p)

    for p in paths:
        os.remove(p)

    return target


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


def download_zip_from_urls_in_memory(
    urls: List[str], target: Optional[str] = None
) -> str:
    if target is None:
        target = str(hash(tuple(urls)))
    with zipfile.ZipFile(target, "w") as f:
        for url in urls:
            name = os.path.basename(urllib.parse.urlparse(url).path)
            response = multiurl.robust(requests.get)(url)
            data = response.content
            f.writestr(name, data)

    return target
