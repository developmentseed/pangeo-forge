from typing import List

import fsspec
import xarray as xr
from prefect import task


@task
def combine_and_write(
    sources: List[str], target: str, append_dim: str, concat_dim: str, group: str
) -> List[str]:
    """
    Write a batch of intermediate files to a combined zarr store.

    Parameters
    ----------
    sources : List[str]
        A list of URLs pointing to the intermediate files.
    target : str
        The URL for the target combined store.
    append_dim : str
        Name of the dimension of which datasets should be appended during write.
    concat_dim : str
        The dimension to concatenate along.

    Returns
    -------
    target : str
        The URL of the written combined Zarr store (same as target).

    Examples
    --------
    >>> import pangeo_forge.tasks.xarray
    >>> import fsspec
    >>> import xarray as xr
    >>> from prefect import Flow

    >>> # Load sample data into `sources`.
    >>> ds = xr.tutorial.open_dataset('rasm').load()
    >>> fs = fsspec.get_filesystem_class("memory")()
    >>> dsets = ds.isel(time=slice(18)), ds.isel(time=slice(18, None))
    >>> for i, dset in enumerate(dsets):
    ...     as_bytes = dset.to_netcdf()
    ...     with fs.open(f"cache/{i}.nc", "wb") as f:
    ...         f.write(as_bytes)

    >>> sources = [f"memory://{dset}" for dset in fs.ls("cache")]
    >>> with Flow("my-flow") as flow:
    ...    result = pangeo_forge.tasks.xarray.combine_and_write(
    ...        sources, "memory://target.zarr", concat_dim="time"
    ...    )
    >>> result
    <Task: combine_and_write>

    We can run that outside of a flow context with ``.run()``
    >>> pangeo_forge.tasks.xarray.combine_and_write.run(
    ...     sources, "memory://target.zarr", concat_dim="time"
    ... )
    'memory://target.zarr'
    """
    # double_open_files = [fsspec.open(url).open() for url in sources]
    # TODO: Figure out why using `double_open_files` in `open_mfdataset` throws `ValueError: I/O operation on closed file.`
    kwargs = {
        'combine': "nested",
        'concat_dim': concat_dim
    }
    if group:
        kwargs['group'] = group

    ds = xr.open_mfdataset(sources, **kwargs)
    # by definition, this should be a contiguous chunk
    ds = ds.chunk({append_dim: len(sources)})
    mapper = fsspec.get_mapper(target)

    if not len(mapper):
        # The first write, .
        kwargs = dict(mode="w")
    else:
        kwargs = dict(mode="a", append_dim=append_dim)
    ds.to_zarr(mapper, **kwargs)
    return target
