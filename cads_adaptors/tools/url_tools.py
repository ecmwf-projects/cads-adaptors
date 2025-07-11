import functools
import os
import tarfile
import urllib
import zipfile
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import jinja2
import multiurl
import requests
import yaml
from tqdm import tqdm

from cads_adaptors.adaptors import Context
from cads_adaptors.exceptions import InvalidRequest, UrlNoDataError
from cads_adaptors.tools import hcube_tools


class RobustDownloader:
    def __init__(
        self,
        target: str,
        maximum_tries: int,
        retry_after: float,
        **download_kwargs,
    ):
        self.target = target
        self.maximum_tries = maximum_tries
        self.retry_after = retry_after
        self.download_kwargs = download_kwargs

    def remove_target(self):
        path = Path(self.target)
        path.unlink(missing_ok=True)
        path.parent.mkdir(exist_ok=True, parents=True)

    def _download(self, url):
        multiurl.download(
            url=url,
            target=self.target,
            maximum_retries=1,  # Handled by outer robust wrapper
            stream=True,
            resume_transfers=True,
            **self.download_kwargs,
        )
        return requests.Response()

    def download(self, url: str) -> None:
        self.remove_target()
        robust_download = multiurl.robust(
            self._download,
            maximum_tries=self.maximum_tries,
            retry_after=self.retry_after,
        )
        return robust_download(url=url)


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
    urls: List[str],
    context: Context,
    server_suggested_filename=False,
    maximum_tries: int = 10,  # TODO: Check with ECMWF
    retry_after: float = 60,  # TODO: Check with ECMWF
    # the default timeout value (3) has been determined empirically (it also included a safety margin)
    timeout: float = 3,
    **kwargs,
) -> List[str]:
    kwargs.setdefault(
        "progress_bar", functools.partial(tqdm, file=context, mininterval=5)
    )

    # Ensure that URLs are unique to prevent downloading the same file multiple times
    urls = sorted(set(urls))

    paths = []
    context.write_type = "stdout"
    # set some default kwargs for establishing a connection
    for url in urls:
        target = urllib.parse.urlparse(url).path.lstrip("/")
        if server_suggested_filename:
            target = os.path.join(
                os.path.dirname(target), multiurl.Downloader(url).title()
            )
        downloader = RobustDownloader(
            target,
            maximum_tries=maximum_tries,
            retry_after=retry_after,
            timeout=timeout,
            **kwargs,
        )
        try:
            context.debug(f"Downloading {url} to {target}")
            downloader.download(url)
        except (requests.ConnectionError, requests.ReadTimeout):
            # The way "multiurl" uses "requests" at the moment,
            # the read timeouts raise requests.exceptions.ConnectionError.
            context.add_user_visible_error(
                "Your request has not found some of the data expected to be present.\n"
                "This may be due to temporary connectivity issues with the source data.\n"
                "If this problem persists, please contact user support."
            )
            raise UrlNoDataError(
                f"Incomplete request result. No data found from the following URL:"
                f"\n{yaml.safe_dump(url, indent=2)} "
            )
        except Exception as e:
            context.debug(f"Failed download for URL: {url}\nException: {e}")
        else:
            paths.append(target)

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
