import functools
import os
import tarfile
import time
import urllib
import zipfile
from typing import Any, Dict, Generator, List, Optional

import jinja2
import multiurl
import requests
import yaml
from tqdm import tqdm

from cads_adaptors.adaptors import Context
from cads_adaptors.exceptions import InvalidRequest, UrlNoDataError
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


def try_download(
    urls: List[str], context: Context, server_suggested_filename=False, **kwargs
) -> List[str]:
    # Ensure that URLs are unique to prevent downloading the same file multiple times
    urls = sorted(set(urls))

    paths = []
    context.write_type = "stdout"
    # set some default kwargs for establishing a connection
    # the default timeout value (3) has been determined empirically (it also included a safety margin)
    kwargs = {"timeout": 3, "maximum_retries": 1, "retry_after": 1, **kwargs}
    for url in urls:
        path = urllib.parse.urlparse(url).path.lstrip("/")
        if server_suggested_filename:
            path = os.path.join(os.path.dirname(path), multiurl.Downloader(url).title())

        dir = os.path.dirname(path)
        if dir:
            os.makedirs(dir, exist_ok=True)
        try:
            context.debug(f"Downloading {url} to {path}")
            MAX_RETRIES = kwargs.get("max_retries", 10)
            IS_RESUME_TRANSFERS_ON = kwargs.get("resume_transfers", False)
            SLEEP_BETWEEN_RETRIES = kwargs.get("sleep_between_retries", 1)
            SLEEP_BETWEEN_RETRIES_INCREASE_RATE = kwargs.get(
                "sleep_between_retries_increase_rate", 1.3
            )
            MAX_SLEEP_BETWEEN_RETRIES = kwargs.get("max_sleep_between_retries", 120)
            for i_retry in range(MAX_RETRIES):
                try:
                    multiurl.download(
                        url,
                        path,
                        progress_bar=functools.partial(
                            tqdm, file=context, mininterval=5
                        ),
                        **kwargs,
                    )
                    break
                except Exception as e:
                    downloaded_bytes = os.path.getsize(path)
                    context.add_stdout(
                        f"Attempt {i_retry+1} to download {url} failed "
                        f"(only {downloaded_bytes}B downloaded so far, "
                        f"with resume_transfers={IS_RESUME_TRANSFERS_ON}): {e!r}"
                    )
                    time.sleep(SLEEP_BETWEEN_RETRIES)
                    SLEEP_BETWEEN_RETRIES = min(
                        SLEEP_BETWEEN_RETRIES * SLEEP_BETWEEN_RETRIES_INCREASE_RATE,
                        MAX_SLEEP_BETWEEN_RETRIES,
                    )
                    if i_retry + 1 == MAX_RETRIES:
                        raise
        except requests.exceptions.ConnectionError as e:
            # The way "multiurl" uses "requests" at the moment,
            # the read timeouts raise requests.exceptions.ConnectionError.
            if kwargs.get("fail_on_timeout_for_any_part", True):
                context.add_user_visible_error(
                    "Your request has not found some of the data expected to be present.\n"
                    "This may be due to temporary connectivity issues with the source data.\n"
                    "If this problem persists, please contact user support."
                )
                raise UrlNoDataError(
                    f"Incomplete request result. No data found from the following URL:"
                    f"\n{yaml.safe_dump(url, indent=2)} "
                )
            else:
                context.debug(f"Failed download for URL: {url}\nException: {e}")
        except Exception as e:
            context.debug(f"Failed download for URL: {url}\nException: {e}")
        else:
            paths.append(path)

    if len(paths) == 0:
        context.add_user_visible_error(
            "Your request has not found any data, please check your selection.\n"
            "This may be due to temporary connectivity issues with the source data.\n"
            "If this problem persists, please contact user support."
        )
        raise UrlNoDataError(
            f"Request empty. No data found from the following URLs:"
            f"\n{yaml.safe_dump(urls, indent=2)} "
        )
    # TODO: raise a warning if len(paths)<len(urls). Need to check who sees this warning
    return paths


# TODO: remove unused function
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


# TODO: remove unused function
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


# TODO: remove unused function
def download_from_urls(
    urls: List[str],
    context: Context,
    download_format: str = "zip",
) -> str:
    base_target = str(hash(tuple(urls)))

    if download_format == "tgz":
        target = download_tgz_from_urls(
            urls=urls, base_target=base_target, context=context
        )
    elif download_format == "zip":
        target = download_zip_from_urls(
            urls=urls, base_target=base_target, context=context
        )
    else:
        raise InvalidRequest(f"Download format '{download_format}' is not supported")

    return target


# TODO: remove unused function
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
