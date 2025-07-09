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
from cads_adaptors.exceptions import (
    InvalidRequest,
    UrlConnectionError,
    UrlNoDataError,
    UrlUnknownError,
)
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
    urls: List[str], context: Context, server_suggested_filename: bool = False, **kwargs
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
            max_retries = kwargs.get("max_retries", 10)
            is_resume_transfers_on = kwargs.get("resume_transfers", False)
            sleep_between_retries = kwargs.get("sleep_between_retries", 1)
            sleep_between_retries_increase_rate = kwargs.get(
                "sleep_between_retries_increase_rate", 1.3
            )
            max_sleep_between_retries = kwargs.get("max_sleep_between_retries", 120)
            for i_retry in range(max_retries):
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
                except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ReadTimeout,
                ) as e:
                    downloaded_bytes = os.path.getsize(path)
                    context.add_stdout(
                        f"Attempt {i_retry+1} to download {url} failed "
                        f"(only {downloaded_bytes}B downloaded so far, "
                        f"with resume_transfers={is_resume_transfers_on}): {e!r}"
                    )
                    time.sleep(sleep_between_retries)
                    sleep_between_retries = min(
                        sleep_between_retries * sleep_between_retries_increase_rate,
                        max_sleep_between_retries,
                    )
                    if i_retry + 1 == max_retries:
                        raise UrlConnectionError(e)
        except requests.exceptions.HTTPError as e:
            # We don't care about HTTP 4XX errors, the current adaptor setup relies on ignoring them
            # The reason for this is that our jinja templating is an incomplete extension of jinja,
            # which uses patterns created from multiple regex definitions.
            # It is possible that a single valid combination of key:values pairs can create multiple URLs,
            # but only one should exist.
            # To remove this, we would need to change how we use jinja in gecko and here,
            # or have a check against the URLs in the manifest file.
            status = e.response.status_code if e.response else None
            if not (status and status >= 400 and status < 500):
                # If the error is not a 4XX, we raise it as an exception
                context.error(f"Failed download for URL: {url}\nException: {e}")
                context.add_user_visible_error(
                    "Your request has failed unexpectedly, this may be a temporary "
                    "issue with the data source, please try your request again. "
                    "If the issue persists, please contact user support."
                )
                raise UrlUnknownError(e)
            context.debug(f"HTTP error {status} for URL {url}, skipping download.")
        except UrlConnectionError:
            # The way "multiurl" uses "requests" at the moment,
            # the read timeouts raise requests.exceptions.ConnectionError.
            context.add_user_visible_error(
                "Your request has not found some of the data expected to be present.\n"
                "This may be due to temporary connectivity issues with the source data.\n"
                "If this problem persists, please contact user support."
            )
            raise UrlConnectionError(
                f"Incomplete request result. No data found from the following URL:"
                f"\n{yaml.safe_dump(url, indent=2)} "
            )
        except Exception as e:
            context.error(f"Failed download for URL: {url}\nException: {e}")
            # System flag to raise unknown exceptions, this is a change in 
            # behaviour hence to be monitored when changed.
            # Setting to True is closer to how the legacy CDS operated.
            if os.getenv("RAISE_UKNOWN_URL_EXCEPTIONS", False):
                context.add_user_visible_error(
                    "Your request has failed unexpectedly, this may be a temporary "
                    "issue with the data source, please try your request again. "
                    "If the issue persists, please contact user support."
                )
                raise e  #UrlUnknownError(e)
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
