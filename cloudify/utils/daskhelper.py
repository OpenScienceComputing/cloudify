async def get_dask_client():
    from dask.distributed import Client

    return await Client(
        asynchronous=True,
        processes=True,
        n_workers=2,
        threads_per_worker=4,
        memory_limit="4GB",
        set_as_default=False,
    )


async def get_dask_cluster():
    from dask.distributed import LocalCluster

    return LocalCluster(
        processes=True,
        n_workers=2,
        threads_per_worker=8,
        memory_limit="16GB",
    )
