import functools
import os
import tarfile
import urllib
import zipfile
from typing import Any, Dict, Generator, List, Optional

import jinja2
import multiurl
import requests
import yaml
from tqdm import tqdm

from cads_adaptors.adaptors import Context
from cads_adaptors.tools import hcube_tools


# copied from cdscommon/url2
def requests_to_urls(
    requests: dict[str, Any] | list[dict[str, Any]], patterns: List[str]
) -> Generator[Dict[str, Any], None, None]:
    """Given a list of requests and a list of URL patterns with Jinja2
    formatting, yield the associated URLs to download.
    """
    jinja_env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    templates = [jinja_env.from_string(p) for p in patterns]

    for req in hcube_tools.unfactorise(requests):  # type: ignore
        for template in templates:
            try:
                url = template.render(req).strip()
            except jinja2.TemplateError:
                pass
            else:
                if url:
                    yield {"url": url, "req": req}


def try_download(urls: List[str], context: Context, **kwargs) -> List[str]:
    # Ensure that URLs are unique to prevent downloading the same file multiple times
    urls = sorted(set(urls))

    paths = []
    context.write_type = "stdout"
    # set some default kwargs for establishing a connection
    kwargs = {"timeout": 1, "maximum_retries": 1, "retry_after": 1, **kwargs}
    for url in urls:
        path = urllib.parse.urlparse(url).path.lstrip("/")
        dir = os.path.dirname(path)
        if dir:
            os.makedirs(dir, exist_ok=True)
        try:
            context.add_stdout(f"Downloading {url} to {path}")
            multiurl.download(
                url,
                path,
                progress_bar=functools.partial(tqdm, file=context, mininterval=5),
                **kwargs,
            )
        except Exception as e:
            context.add_stdout(f"Failed download for URL: {url}\nException: {e}")
        else:
            paths.append(path)

    if len(paths) == 0:
        context.add_user_visible_error(
            "Your request has not found any data, please check your selection.\n"
            "If you believe this to be a data store error, please contact user support."
        )
        raise RuntimeError(
            f"Request empty. No data found from the following URLs:"
            f"\n{yaml.safe_dump(urls, indent=2)} "
        )
    # TODO: raise a warning if len(paths)<len(urls). Need to check who sees this warning
    return paths


# TODO use targzstream
def download_zip_from_urls(
    urls: List[str],
    base_target: str,
    context: Context,
) -> str:
    target = f"{base_target}.zip"
    paths = try_download(urls, context=context)
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
    context: Context,
) -> str:
    target = f"{base_target}.tar.gz"
    paths = try_download(urls, context=context)
    with tarfile.open(target, "w:gz") as archive:
        for p in paths:
            archive.add(p)

    for p in paths:
        os.remove(p)

    return target


def download_from_urls(
    urls: List[str],
    context: Context,
    data_format: str = "zip",
) -> str:
    base_target = str(hash(tuple(urls)))

    if data_format == "tgz":
        target = download_tgz_from_urls(
            urls=urls, base_target=base_target, context=context
        )
    elif data_format == "zip":
        target = download_zip_from_urls(
            urls=urls, base_target=base_target, context=context
        )
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
