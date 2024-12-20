import os
from tempfile import mkstemp

from cds_common.cams.regional_fc_api import regional_fc_api

from . import STACK_DOWNLOAD_DIR, STACK_TEMP_DIR
from .meteo_france_retrieve import meteo_france_retrieve


def subrequest_main(backend, request, child_config, context):
    """Get data from the specified Meteo France backend."""
    parent_config = request["parent_config"]
    message = (
        f"The parent request is {parent_config['request_uid']}, "
        f"launched by user {parent_config['user_uid']}."
    )
    context.add_stdout(message)

    # Are we using the "integration" (test) or main server?
    cfg = parent_config.get("regional_fc", {})
    integration_server = cfg.get("integration_server", False)
    if integration_server:
        context.info("Using integration server")

    # Get an object which will give us information/functionality associated
    # with the Meteo France regional forecast API
    regapi = regional_fc_api(integration_server=integration_server, logger=context)

    # Construct a target file name
    os.makedirs(STACK_DOWNLOAD_DIR, exist_ok=True)
    fd, target = mkstemp(
        prefix=child_config["request_uid"] + "_", suffix=".grib", dir=STACK_DOWNLOAD_DIR
    )
    os.close(fd)

    cacher_kwargs = cfg.get("cacher_kwargs", {})
    if cfg.get("no_cache_key"):
        cacher_kwargs["no_cache_key"] = cfg["no_cache_key"]

    # Get the data
    try:
        meteo_france_retrieve(
            request["requests"],
            regapi,
            cfg["definitions"],
            integration_server,
            target=target,
            logger=context,
            tmpdir=STACK_TEMP_DIR,
            max_rate=cfg.get("meteofrance_max_rate"),
            max_simultaneous=cfg.get("meteofrance_max_simultaneous"),
            cacher_kwargs=cacher_kwargs,
        )
    except Exception as e:
        message = f"Failed to obtain data from remote server: {e!r}"
        context.add_stderr(message)
        raise RuntimeError(message) from None

    return open(target, "rb")
