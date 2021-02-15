import os

import fsspec
from prefect import task
import requests

@task
def download(source_url, cache_location, auth=(), use_source_filename=False):
    """
    Download a remote file to a cache.

    Parameters
    ----------
    source_url : str
        Path or url to the source file.
    cache_location : str
        Path or url to the target location for the source file.
    auth : tuple of str
        Username and password to uses with `reqests.get`
    use_source_filename : bool
        Defaults to `False`. If true, download files to their original filename.

    Returns
    -------
    target_url : str
        Path or url in the form of `{cache_location}/hash({source_url})`.
    """
    if use_source_filename == True:
        cache_filename = os.path.basename(source_url)
    else:
        cache_filename = str(hash(source_url))

    # destination of file download
    target_url = os.path.join(cache_location, cache_filename)

    # there is probably a better way to do caching!
    try:
        fsspec.open(target_url).open()
        return target_url
    except FileNotFoundError:
        pass

    # REVIEW: What was setting this before?
    kwargs = {}
    # alternate download method if we need to use auth
    # REVIEW: is there a way to use fsspec with http basic authentication?
    if len(auth) == 2:
        r = requests.get(source_url, auth=auth)
        with fsspec.open(target_url, mode="wb") as target:
            target.write(r.content)
    else:
        with fsspec.open(source_url, mode="rb", **kwargs) as source:
            with fsspec.open(target_url, mode="wb") as target:
                target.write(source.read())
    return target_url
