from cloudify.utils.daskhelper import *
from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.forwarder import *
from cloudify.utils.datasethelper import *
import xpublish as xp
import fastapi

if __name__ == "__main__":  # This avoids infinite subprocess creation
    collection = xp.Rest(
            {},
        cache_kws=dict(available_bytes=0),
        app_kws=dict(
            redirect_slashes=False,
            dependencies=[fastapi.Depends(set_custom_header)]
            # middleware=middleware
        ),
    )
    collection.register_plugin(ForwardPlugin())
    collection.register_plugin(DynamicKerchunk())

    collection.serve(host="0.0.0.0", port=9000)
